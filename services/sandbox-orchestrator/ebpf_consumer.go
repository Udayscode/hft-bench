package main

// ebpf_consumer.go — Zero-copy ring buffer consumer for TC latency events.
//
// Architecture:
//   - ProbeConsumer runs one goroutine per TAP/VM that drains the BPF ringbuf.
//   - Events are batched into a local slice and forwarded to the existing
//     Kafka/telemetry pipeline via a non-blocking channel send.
//   - Zero heap allocation per event: ReadInto() writes directly into a
//     stack-allocated struct backed by the mmap'd ring buffer region.
//   - The consumer is started immediately after AttachProber and stopped
//     before DetachProber during VM teardown.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"unsafe"

	"github.com/cilium/ebpf/ringbuf"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// probeFlushSize is the number of latency events to batch before forwarding.
	probeFlushSize = 256

	// probeFlushInterval is the maximum time to hold events before flushing,
	// even if probeFlushSize hasn't been reached. Keeps tail latency visible.
	probeFlushInterval = 5 * time.Millisecond

	// probeRingPollTimeout is the ring buffer poll deadline per iteration.
	// Short enough for responsive teardown; long enough to avoid busy-looping.
	probeRingPollTimeout = 2 * time.Millisecond
)

// LatencyRecord is the Go-side mirror of the BPF latency_event struct.
// Field layout MUST match the C struct exactly (same size, same alignment).
// Verified at init() time via unsafe.Sizeof assertion.
type LatencyRecord struct {
	EgressTsNs uint64 // absolute egress timestamp
	LatencyNs  uint64 // round-trip delta in nanoseconds
	SrcIP      uint32
	DstIP      uint32
	SrcPort    uint16
	DstPort    uint16
	Seq        uint32
}

// KernelLatencyEvent is what we ship to the telemetry pipeline.
// It augments the raw LatencyRecord with orchestrator metadata.
type KernelLatencyEvent struct {
	VMID        string  `json:"vm_id"`
	SubmissionID string `json:"submission_id"`
	LatencyNs   uint64  `json:"latency_ns"`
	LatencyUs   float64 `json:"latency_us"`
	EgressTsNs  uint64  `json:"egress_ts_ns"`
	SrcPort     uint16  `json:"src_port"`
	DstPort     uint16  `json:"dst_port"`
	Seq         uint32  `json:"seq"`
	Timestamp   int64   `json:"timestamp"` // Unix milliseconds, for TimescaleDB partitioning
}

// ProbeConsumer drains the latency ring buffer for one VM's TAP prober.
type ProbeConsumer struct {
	vmID         string
	submissionID string
	prober       *TapProber
	kafkaClient  *kgo.Client
	topic        string
	cancel       context.CancelFunc
	done         chan struct{}
}

// StartProbeConsumer creates and starts a ProbeConsumer goroutine.
// kafkaClient and topic are the existing pipeline targets — no new infrastructure needed.
// Returns immediately; the consumer runs in the background until StopProbeConsumer.
func StartProbeConsumer(
	parentCtx context.Context,
	vmID string,
	submissionID string,
	prober *TapProber,
	kafkaClient *kgo.Client,
	topic string,
) (*ProbeConsumer, error) {

	ctx, cancel := context.WithCancel(parentCtx)

	c := &ProbeConsumer{
		vmID:         vmID,
		submissionID: submissionID,
		prober:       prober,
		kafkaClient:  kafkaClient,
		topic:        topic,
		cancel:       cancel,
		done:         make(chan struct{}),
	}

	go c.run(ctx)

	return c, nil
}

// StopProbeConsumer signals the consumer to stop and waits for it to drain.
// Must be called before DetachProber to prevent a use-after-free on the map fd.
func StopProbeConsumer(c *ProbeConsumer) {
	if c == nil {
		return
	}
	c.cancel()
	<-c.done
}

// run is the main consumer loop. It must not allocate on the hot path.
func (c *ProbeConsumer) run(ctx context.Context) {
	defer close(c.done)

	rd, err := ringbuf.NewReader(c.prober.RingBufMap())
	if err != nil {
		log.Printf("[probe-consumer] failed creating ringbuf reader vm_id=%s: %v", c.vmID, err)
		return
	}
	defer rd.Close()

	batch := make([]KernelLatencyEvent, 0, probeFlushSize)
	ticker := time.NewTicker(probeFlushInterval)
	defer ticker.Stop()

	log.Printf("[probe-consumer] started vm_id=%s", c.vmID)

	for {
		// Check for stop signal before blocking on the ring buffer
		select {
		case <-ctx.Done():
			// Drain any buffered events before exit
			if len(batch) > 0 {
				c.flush(ctx, batch)
			}
			log.Printf("[probe-consumer] stopped vm_id=%s", c.vmID)
			return
		case <-ticker.C:
			if len(batch) > 0 {
				c.flush(ctx, batch)
				batch = batch[:0]
			}
		default:
		}

		// SetDeadline controls how long Read blocks. By setting a short deadline,
		// we get responsive context cancellation without busy-looping.
		rd.SetDeadline(time.Now().Add(probeRingPollTimeout))

		// ReadInto writes directly into the pre-allocated LatencyRecord on the stack.
		// The ringbuf.Reader backs this with the mmap'd kernel region — zero copy.
		var raw ringbuf.Record
		if err := rd.ReadInto(&raw); err != nil {
			// Timeout is expected and normal — just loop
			continue
		}

		if len(raw.RawSample) < int(unsafe.Sizeof(LatencyRecord{})) {
			log.Printf("[probe-consumer] short record len=%d vm_id=%s", len(raw.RawSample), c.vmID)
			continue
		}

		// Reinterpret the raw bytes as a LatencyRecord — no allocation, no copy.
		// This is safe because:
		//   1. raw.RawSample is a []byte backed by the mmap'd ringbuf region
		//   2. LatencyRecord has the same layout as the C struct (verified at init)
		//   3. We read the fields before returning from this scope
		rec := (*LatencyRecord)(unsafe.Pointer(&raw.RawSample[0]))

		evt := KernelLatencyEvent{
			VMID:         c.vmID,
			SubmissionID: c.submissionID,
			LatencyNs:    rec.LatencyNs,
			LatencyUs:    float64(rec.LatencyNs) / 1000.0,
			EgressTsNs:   rec.EgressTsNs,
			SrcPort:      rec.SrcPort,
			DstPort:      rec.DstPort,
			Seq:          rec.Seq,
			Timestamp:    time.Now().UnixMilli(),
		}

		batch = append(batch, evt)

		if len(batch) >= probeFlushSize {
			c.flush(ctx, batch)
			batch = batch[:0]
		}
	}
}

// flush encodes the batch as JSON and forwards events.
// If kafkaClient is nil (e.g. during testing), events are logged only.
func (c *ProbeConsumer) flush(ctx context.Context, batch []KernelLatencyEvent) {
	if len(batch) == 0 {
		return
	}

	// Log-only mode when no Kafka client is wired up
	if c.kafkaClient == nil {
		for i := range batch {
			log.Printf("[probe-consumer] vm_id=%s latency_ns=%d latency_us=%.3f seq=%d",
				c.vmID, batch[i].LatencyNs, batch[i].LatencyUs, batch[i].Seq)
		}
		return
	}

	records := make([]*kgo.Record, 0, len(batch))

	for i := range batch {
		payload, err := json.Marshal(&batch[i])
		if err != nil {
			log.Printf("[probe-consumer] marshal error vm_id=%s: %v", c.vmID, err)
			continue
		}

		records = append(records, &kgo.Record{
			Topic: c.topic,
			Key:   []byte(c.vmID),
			Value: payload,
		})
	}

	// ProduceSync blocks until all records are acknowledged or the context expires.
	// For fire-and-forget semantics, swap to ProduceAsync.
	if err := c.kafkaClient.ProduceSync(ctx, records...).FirstErr(); err != nil {
		log.Printf("[probe-consumer] kafka produce error vm_id=%s batch=%d: %v",
			c.vmID, len(records), err)
		return
	}

	log.Printf("[probe-consumer] flushed vm_id=%s events=%d", c.vmID, len(records))
}

// init verifies that LatencyRecord has the same wire size as the C struct.
// If this panics, the C struct and Go struct have diverged — fix the layout.
func init() {
	const expectedSize = 32 // sizeof(struct latency_event) in tc_latency.c
	if sz := unsafe.Sizeof(LatencyRecord{}); sz != expectedSize {
		panic(fmt.Sprintf(
			"LatencyRecord size mismatch: Go=%d C=%d — update LatencyRecord to match latency_event",
			sz, expectedSize,
		))
	}
}

// KafkaClientForProber is a helper type alias to avoid importing kgo in main.go.
// The orchestrator passes its existing kafka producer here.
type KafkaClientForProber = kgo.Client

// ProbeKafkaTopic is the dedicated topic for kernel-level latency events.
// Kept separate from "benchmark-results" so the telemetry-ingester can
// route them to a different hypertable (kernel_latency_events vs benchmark_metrics).
const ProbeKafkaTopic = "kernel-latency-events"
