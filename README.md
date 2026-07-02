<p align="center">
  <h1 align="center">⚡ HFT Bench</h1>
  <p align="center">
    <b>Bare-metal matching engine benchmarking with hardware-isolated Firecracker microVMs, kernel-bypass load injection, and eBPF-instrumented latency measurement.</b>
  </p>
  <p align="center">
    <a href="#why-this-exists">Why</a> · <a href="#system-architecture">Architecture</a> · <a href="#quick-start">Quick Start</a> · <a href="#contestant-api-contract">API Contract</a> · <a href="#project-structure">Structure</a> · <a href="./ARCHITECTURE.md">Deep Dive →</a>
  </p>
</p>

---

## Why This Exists

Standard benchmarking tools (Docker, K8s, gVisor) introduce **2–10ms of jitter** from shared-kernel context switches, network bridge translations, and CPU cache pollution. When the engines you're measuring operate at **single-digit microsecond** latencies, the measurement infrastructure becomes the dominant noise source — the tool corrupts the result.

HFT Bench solves the **Observer Effect** of latency measurement by eliminating every shared abstraction between the load generator and the system under test:

| Problem | How We Solve It |
|---|---|
| Shared kernel (Docker `runc`) allows guest-to-host escape | **Firecracker microVMs** — hardware-enforced KVM boundary, separate guest kernel |
| CPU cache pollution from co-located processes | **`taskset` + `isolcpus`** — contestant vCPUs pinned to cores ripped from the Linux scheduler |
| Network stack overhead (TCP checksums, bridge NAT) | **AF_VSOCK** — zero-copy host↔guest transport via virtio, bypassing the entire IP stack |
| Measurement timestamps perturbed by OS jitter | **eBPF TC probes** — kernel-space `ktime_get_ns()` timestamps on TAP ingress/egress, zero userspace involvement |
| Noisy neighbors degrading parallel benchmarks | **Per-VM TAP + `tc` TBF** — each submission gets its own network namespace rate-limited to 100 Mbps |
| Database writes blocking the hot path | **Redpanda → TimescaleDB** — LZ4-compressed telemetry streamed async via Kafka, never touching the execution threads |

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        BARE-METAL LINUX HOST                           │
│  isolcpus=2-9 · IRQ affinity masked · C-states disabled · Hugepages   │
│                                                                        │
│  ┌──────────────┐    gRPC     ┌───────────────────────────────────┐    │
│  │ Submission    │───────────▶│     Sandbox Orchestrator (Go)     │    │
│  │ API (Go)     │            │  • Provisions Firecracker microVM  │    │
│  │ :8080        │            │  • Creates TAP + tc TBF qdisc     │    │
│  └──────┬───────┘            │  • Attaches eBPF TC latency probes│    │
│         │                    │  • Pins vCPUs via taskset          │    │
│         │                    └──────────┬────────────────────────┘    │
│         │                               │                             │
│         ▼                               ▼                             │
│  ┌──────────────┐    AF_VSOCK   ┌─────────────────┐                   │
│  │  Bot Fleet   │──────────────▶│  Firecracker VM  │                   │
│  │  (Rust/Tokio)│  zero-copy    │  ┌─────────────┐ │                   │
│  │  cores 4-7   │◀──────────────│  │ contestant  │ │                   │
│  └──────┬───────┘               │  │ strategy_bin│ │                   │
│         │                       │  └─────────────┘ │                   │
│         │ mirror every order    └─────────────────┘                   │
│         ▼                                                             │
│  ┌──────────────┐                                                     │
│  │Shadow Engine │  Deterministic BTreeMap LOB                         │
│  │  (Rust)      │  Catches fake/dropped trades                        │
│  │  cores 8-9   │                                                     │
│  └──────┬───────┘                                                     │
│         │ LZ4                                                         │
│         ▼                                                             │
│  ┌──────────────┐         ┌───────────────┐       ┌───────────────┐  │
│  │  Redpanda    │────────▶│  Telemetry    │──────▶│  TimescaleDB  │  │
│  │  (Kafka)     │  batch  │  Ingester (Go)│  WAL  │  hypertable   │  │
│  │  cores 10-11 │         └───────────────┘       └───────┬───────┘  │
│  └──────────────┘                                         │          │
│                                                           ▼          │
│                         ┌──────────────┐         ┌───────────────┐   │
│                         │ Leaderboard  │────────▶│  Next.js UI   │   │
│                         │ API (Go)     │  JSON   │  :3000        │   │
│                         │ :3001        │         └───────────────┘   │
│                         └──────────────┘                             │
└─────────────────────────────────────────────────────────────────────────┘
```

> **7 services** · **3 languages** (Go, Rust, TypeScript) · **0 shared kernels** on the hot path

For the full 20-section technical deep-dive (ADRs, failure mode registry, scoring math, security model), see **[ARCHITECTURE.md](./ARCHITECTURE.md)**.

---

## Project Structure

```
hft-bench/
├── services/
│   ├── sandbox-orchestrator/    # Go — Firecracker lifecycle, TAP networking, eBPF TC probes
│   │   ├── ebpf/tc/             #   Compiled eBPF bytecode (bpf2go generated)
│   │   ├── ebpf_prober.go       #   TC clsact attach/detach lifecycle
│   │   ├── ebpf_consumer.go     #   Zero-copy ringbuf → Kafka pipeline
│   │   ├── firecracker.go       #   VM boot, vCPU pinning, teardown
│   │   ├── network.go           #   TAP creation, tc TBF rate limiting
│   │   └── disk.go              #   rootfs.ext4 generation, init injection
│   ├── bot-fleet/               # Rust — Tokio VSOCK load generator (60/25/15 order mix)
│   ├── shadow-engine/           # Rust — Deterministic BTreeMap LOB for trade verification
│   ├── submission-api/          # Go — REST API, job queue, logarithmic scoring
│   ├── telemetry-ingester/      # Go — Redpanda consumer → TimescaleDB batch writer
│   ├── leaderboard/             # Go — Score aggregation API
│   └── leaderboard-ui/          # Next.js — Real-time podium, stats strip, submission drilldown
├── scripts/
│   ├── tune_system.sh           # isolcpus, IRQ masking, C-state disable, hugepage alloc
│   ├── bootstrap-sandbox.sh     # Downloads Firecracker + kernel, builds Alpine rootfs
│   ├── cpu_layout.env           # Core topology map (VM cores, bot-fleet cores, DB cores)
│   ├── blast_load.sh            # Concurrent 10-submission stress test
│   ├── isolate_vm.sh            # Per-VM cgroup isolation helper
│   └── trace_vsock.bt           # bpftrace script for VSOCK kernel tracing
├── infrastructure/
│   └── terraform/               # AWS .metal instance provisioning (IaC)
├── sandbox/
│   └── strategy_bin.c           # Reference matching engine (C) — contestants replace this
├── proto/                       # Protobuf definitions (orchestrator + benchmark gRPC)
├── docker-compose.yml           # Full stack: Redpanda, TimescaleDB, all 7 services
├── run-orchestrator.sh          # Root entrypoint: bridge setup, kernel tuning, go build + run
└── ARCHITECTURE.md              # 20-section technical reference (129 KB)
```

---

## Quick Start

### Prerequisites

> **⚠️ This platform requires bare-metal Linux with KVM.** It will not run on macOS, Windows, or nested virtualization (cloud VMs). The entire point is eliminating virtualization noise from measurements.

- Linux kernel 5.10+ with `/dev/kvm` and `cgroup_v2`
- `taskset`, `tc` (iproute2), `ip`, `brctl`
- Docker + Docker Compose
- Go 1.21+ and Rust toolchain
- Root access (for TAP interfaces and CPU pinning)

### 1. Bootstrap Sandbox Dependencies

Firecracker binaries, the Linux kernel, and the Alpine rootfs are **not committed to git** (they're 500MB+). This script fetches everything:

```bash
./scripts/bootstrap-sandbox.sh
```

This downloads Firecracker v1.7.0, the minimal AWS vmlinux kernel, and builds a custom Alpine ext4 filesystem with our injected `/sbin/init` that dual-mounts the contestant's strategy binary.

### 2. Start the Data Plane + Control Plane

```bash
docker compose up -d
```

This brings up all 8 containers: Redpanda, TimescaleDB, bot-fleet, shadow-engine, submission-api, telemetry-ingester, leaderboard API, and the Next.js frontend. CPU cores are pre-pinned via `cpuset` in the compose file.

### 3. Boot the Bare-Metal Orchestrator

The Firecracker orchestrator **must** run as root outside Docker. It manipulates kernel networking (TAP/bridge), applies CPU pinning, and attaches eBPF TC probes to each VM's virtual interface.

```bash
sudo ./run-orchestrator.sh
```

This script:
1. Sources `scripts/cpu_layout.env` for the core topology
2. Executes `scripts/tune_system.sh` to mask IRQs, disable C-states, and allocate hugepages
3. Sets up the `br0` bridge with IP forwarding
4. Compiles the Go orchestrator with CGO (required for eBPF/cilium)
5. Compiles the reference `strategy_bin.c`
6. Runs the orchestrator pinned to the designated cores via `taskset`

### 4. Submit a Matching Engine

```bash
# Submit the reference engine
curl -X POST \
  -F "submission_id=my-engine" \
  -F "binary=@sandbox/strategy_bin" \
  http://localhost:8080/submit

# Or blast 10 concurrent submissions for stress testing
bash ./scripts/blast_load.sh
```

### 5. View the Live Leaderboard

Open **http://localhost:3000** — the Next.js dashboard shows the real-time podium, per-submission latency distributions, and historical run comparisons.

---

## Contestant API Contract

Your binary is injected at `/strategy_bin` inside a 64 MiB ext4 Firecracker rootfs. It **must**:

- Be a **statically-linked Linux x86_64 ELF** (no glibc dependencies — the rootfs is Alpine/musl)
- Listen on TCP port **8080** with `TCP_NODELAY` enabled
- Process newline-delimited JSON, one order per line
- Respond with the resulting trades (or an empty array if the order rests)

**Input (one per line):**
```json
{"type":"limit","price":100,"qty":10,"side":"buy","order_id":1}
{"type":"market","qty":5,"side":"sell","order_id":2}
{"type":"cancel","order_id":1}
```

**Expected output (one per input):**
```json
{"trades":[]}
{"trades":[{"maker_order_id":1,"taker_order_id":2,"price":100,"quantity":5}]}
{"trades":[]}
```

> **Anti-cheat:** Every response is verified against the Shadow Engine's deterministic `BTreeMap`. Fake or dropped trades are flagged as correctness violations and heavily penalized in the composite score.

---

## Scoring

The composite score uses a **logarithmic curve** (not linear clamping) to preserve discrimination at microsecond granularity:

```
score = w_latency · log_scale(p99) + w_throughput · log_scale(tps) + w_correctness · success_rate
```

Where `log_scale(x) = 100 · (1 - ln(x / baseline) / ln(worst / baseline))`, ensuring that the difference between 400µs and 500µs is scored proportionally larger than between 4000µs and 4100µs — matching real-world HFT economics.

---

## Measurement Stack

HFT Bench operates two complementary measurement layers:

| Layer | Mechanism | Resolution | Location |
|---|---|---|---|
| **Userspace** | Rust `Instant::now()` (`CLOCK_MONOTONIC`) | ±10–50µs | `bot-fleet/src/main.rs` |
| **Kernel** | eBPF TC probes (`ktime_get_ns()`) on TAP ingress/egress | ±1–5µs | `sandbox-orchestrator/ebpf_prober.go` |

The eBPF probes attach to the `clsact` qdisc (coexisting with the TBF rate limiter) and write latency events into a per-TAP **ring buffer**. A zero-copy Go consumer (`ebpf_consumer.go`) drains these via `mmap`'d reads and forwards batches to Redpanda on a dedicated `kernel-latency-events` topic.

---

## Tech Stack

| Component | Technology | Why |
|---|---|---|
| VM Isolation | Firecracker + KVM | Hardware boundary, 125ms boot, 5MB memory footprint |
| Load Generator | Rust + Tokio | Zero-cost async, no GC pauses on the hot path |
| Shadow Engine | Rust + BTreeMap | Deterministic O(log n) price-time priority matching |
| Transport | AF_VSOCK | Kernel-bypass host↔guest, no TCP/IP overhead |
| Kernel Probes | eBPF/TC (cilium/ebpf) | Nanosecond timestamps without userspace context switches |
| Streaming | Redpanda (Kafka) | C++ Kafka, 10x lower tail latency than JVM-based Kafka |
| TSDB | TimescaleDB | Hypertable time-partitioning for billions of metrics rows |
| Control Plane | Go + gRPC | Simple, typed RPCs between orchestrator/API/bot-fleet |
| Frontend | Next.js + Tailwind | SSR, standalone Docker build, real-time polling |
| IaC | Terraform | AWS `.metal` instance provisioning for horizontal scaling |

---

## Known Limitations

- **Software timestamping floor:** `Instant::now()` provides ±10–50µs resolution. Rankings within this band are not statistically meaningful. The eBPF TC probes reduce this to ±1–5µs but do not yet feed into the scoring pipeline.
- **Single-host only:** The current architecture runs all VMs on one physical machine. Terraform IaC is laid for multi-host, but the orchestrator doesn't yet support distributed job scheduling.
- **No job reaper:** If the orchestrator crashes mid-benchmark, zombie VMs and orphaned TAP interfaces must be cleaned manually.
- **Statistical tie detection:** The scoring algorithm doesn't handle ties caused by metric quantization within the measurement floor.

See Section 20 of [ARCHITECTURE.md](./ARCHITECTURE.md) for the full production hardening roadmap.

---

## License

MIT
