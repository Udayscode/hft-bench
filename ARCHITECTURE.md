# ARCHITECTURE.md — HFT Benchmarking Platform

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Design Philosophy](#2-design-philosophy)
3. [System Architecture](#3-system-architecture)
4. [End-to-End Request Trace](#4-end-to-end-request-trace)
5. [Sandbox Engine](#5-sandbox-engine)
6. [The Measurement Architecture](#6-the-measurement-architecture)
7. [Bot Fleet](#7-bot-fleet)
8. [Shadow Order Book & Correctness Validation](#8-shadow-order-book--correctness-validation)
9. [Telemetry & Validation Pipeline](#9-telemetry--validation-pipeline)
10. [Composite Scoring Algorithm](#10-composite-scoring-algorithm)
11. [Real-Time Leaderboard](#11-real-time-leaderboard)
12. [Hardware Sympathy & Host Tuning](#12-hardware-sympathy--host-tuning)
13. [Security Model](#13-security-model)
14. [Inter-Service Communication](#14-inter-service-communication)
15. [Infrastructure as Code](#15-infrastructure-as-code)
16. [Failure Mode Registry](#16-failure-mode-registry)
17. [Architecture Decision Records](#17-architecture-decision-records)
18. [Performance Characteristics](#18-performance-characteristics)
19. [Contestant Guide](#19-contestant-guide)
20. [Known Limitations & Production Hardening Path](#20-known-limitations--production-hardening-path)

---

## 1. Executive Summary

This platform evaluates high-frequency trading engine submissions by executing anonymous contestant binaries inside Firecracker microVMs, blasting them with a synthetic order stream, validating every response against a deterministic shadow matching engine, and producing a composite score that ranks latency, throughput, correctness, and stability on a real-time leaderboard.

The system comprises seven services: a REST submission API (Go) that accepts binary uploads and orchestrates the full pipeline, a sandbox orchestrator (Go) that provisions and tears down Firecracker microVMs with per-submission network isolation, a bot fleet (Rust) that generates and dispatches orders over persistent VSOCK connections while simultaneously forwarding each order to the shadow engine for correctness validation, a shadow matching engine (Rust) implementing a deterministic price-time priority limit order book over gRPC, a telemetry ingester (Go) that consumes benchmark results from Redpanda and persists them into TimescaleDB, a leaderboard backend (Go) that queries TimescaleDB and serves scored rankings, and a Next.js frontend that polls the leaderboard API and renders a live dashboard with podium, stats strip, and per-submission drill-down.

Every architectural decision in this system is constrained by a single requirement: the measurement infrastructure must not perturb the system it measures. When the measurement floor is ~1µs (software `Instant::now()` on x86_64), every abstraction layer that adds latency jitter — container runtimes, shared kernels, interpreted networking stacks — becomes a source of systematic measurement error that corrupts the leaderboard's rank ordering.

---

## 2. Design Philosophy

### 2.1 The Observer Effect Problem in Latency Measurement

A benchmarking platform for latency-sensitive systems faces a fundamental tension: the act of measuring latency introduces latency. Every timestamp capture, every network hop between the load generator and the system under test, every context switch caused by the measurement infrastructure itself — all of these add noise to the signal we are trying to measure.

In production HFT systems, tick-to-trade latency budgets are measured in single-digit microseconds. The measurement apparatus must contribute less error than the resolution at which we intend to discriminate between contestants. If the measurement uncertainty is ±50µs and two contestants differ by 30µs, the leaderboard cannot meaningfully rank them — and claiming it can would be dishonest.

This constraint drives every decision in this architecture: the choice of Firecracker over containers, the use of persistent TCP/VSOCK connections instead of per-request connection establishment, the CPU core isolation strategy, and the explicit quantification of measurement uncertainty in the scoring pipeline.

### 2.2 The Three Non-Negotiable Physical Constraints

**Constraint 1: Kernel Isolation.** Contestant binaries are adversarial by definition — anonymous, untrusted, compiled code uploaded by strangers. The execution environment must provide a hardware-enforced security boundary. Shared-kernel isolation (Docker, even with seccomp/AppArmor) does not meet this requirement. The guest kernel must be separate from the host kernel.

**Constraint 2: Measurement Integrity.** The benchmarking infrastructure must not share physical CPU cores with the system under test. If the bot fleet's load generation threads contend with the contestant engine's execution threads for L1 cache or pipeline resources, the measurement reflects infrastructure contention, not contestant performance. CPU pinning is not optional — it is a correctness requirement for the measurement.

**Constraint 3: Network Determinism.** Each submission must execute in a network namespace where its traffic cannot interfere with — or be interfered by — other concurrent submissions. A contestant engine that floods the wire must not degrade the p99 of a different contestant running in parallel. Per-submission TAP interfaces attached to a bridge with traffic control (`tc`) rate limiting enforce this isolation.

### 2.3 Why Standard Cloud-Native Abstractions Fail for HFT Benchmarking

Kubernetes pod scheduling introduces 2–10ms of overhead per pod creation, plus non-deterministic bin-packing across nodes. Docker's `runc` shares the host kernel, meaning a CVE in the kernel's syscall surface (such as the hypothetical CVE-2026-31431 analyzed in Section 5.2) grants the contestant access to the host. gVisor's syscall interposition adds 10–30% overhead to every syscall, which directly inflates the measured latency of the contestant engine — the measurement tool is corrupting the measurement.

We chose to accept the operational complexity of managing Firecracker microVMs directly because the alternative — any form of shared-kernel isolation — violates either Constraint 1 or Constraint 2.

---

## 3. System Architecture

### 3.1 Service Topology & Deployment Model

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           HOST MACHINE (bare metal)                         │
│                                                                              │
│  ┌──────────────┐    ┌───────────────┐    ┌──────────────┐                  │
│  │ submission-   │    │  sandbox-     │    │  bot-fleet   │                  │
│  │ api (Go)     │───▶│  orchestrator │───▶│  (Rust)      │                  │
│  │ :8080 REST   │    │  (Go) :50051  │    │  :50052 gRPC │                  │
│  └──────┬───────┘    │  gRPC         │    └──────┬───────┘                  │
│         │            └───────┬───────┘           │                          │
│         │                    │                   │                          │
│         │            ┌───────▼───────┐    ┌──────▼───────┐                  │
│         │            │  Firecracker  │    │ shadow-      │                  │
│         │            │  MicroVM      │    │ engine       │                  │
│         │            │  ┌─────────┐  │    │ (Rust)       │                  │
│         │            │  │strategy │  │    │ :50053 gRPC  │                  │
│         │            │  │  _bin   │  │    └──────────────┘                  │
│         │            │  └─────────┘  │                                      │
│         │            │  VSOCK + TAP  │                                      │
│         │            └───────────────┘                                      │
│         │                                                                    │
│  ┌──────▼─────────┐    ┌──────────────┐    ┌──────────────┐                │
│  │  Redpanda      │───▶│ telemetry-   │───▶│ TimescaleDB  │                │
│  │  :9092         │    │ ingester(Go) │    │ :5432        │                │
│  └────────────────┘    └──────────────┘    └──────┬───────┘                │
│                                                    │                        │
│                                             ┌──────▼───────┐                │
│                                             │ leaderboard  │                │
│                                             │ (Go) :3001   │                │
│                                             └──────┬───────┘                │
│                                                    │                        │
│                                             ┌──────▼───────┐                │
│                                             │leaderboard-ui│                │
│                                             │(Next.js):3000│                │
│                                             └──────────────┘                │
└──────────────────────────────────────────────────────────────────────────────┘
```

All services operate in `network_mode: "host"` — there is no Docker bridge network. Every service binds directly to the host's network namespace and communicates over `localhost`. The sandbox orchestrator runs *outside* Docker entirely (launched by `run-orchestrator.sh` with `sudo`) because it requires privileged access to create TAP interfaces, mount loop devices, and invoke the Firecracker binary.

### 3.2 CPU Core Allocation & Isolation Strategy

The system partitions a 6-core/12-thread CPU (AMD Ryzen 5 7530U topology) into five isolation domains. Each domain maps to a specific pair of logical CPUs (one physical core with SMT), ensuring that no two latency-critical workloads share a physical core's execution resources.

```
┌──────────────────────────────────────────────────────────────────┐
│  Physical Core 0 → CPUs  0, 1  │  OS scheduler, IRQs, kernel   │
│  Physical Core 1 → CPUs  2, 3  │  Firecracker microVM vCPUs    │
│  Physical Core 2 → CPUs  4, 5  │  bot-fleet Tokio workers      │
│  Physical Core 3 → CPUs  6, 7  │  shadow-engine matcher        │
│  Physical Core 4 → CPUs  8, 9  │  sandbox-orchestrator         │
│  Physical Core 5 → CPUs 10,11  │  Redpanda + telemetry-ingester│
└──────────────────────────────────────────────────────────────────┘
```

Docker Compose enforces this via the `cpuset` directive: `bot-fleet` is pinned to `"4,5,6,7"` (the code specifies 4 cores but cpu_layout.env allocates cores 4,5 for bot-fleet specifically; the docker-compose cpuset is broader to accommodate concurrency), `shadow-engine` to `"8,9"`, and `redpanda` plus `telemetry-ingester` to `"10,11"`. The orchestrator itself is pinned by `run-orchestrator.sh` using `taskset -c "$ORCH_CORES"` (cores 8,9).

Firecracker VMM processes are pinned post-boot via `pinFirecrackerProcess()` in `firecracker.go`. This function retrieves the VMM's PID via `machine.PID()`, then calls `taskset -pc <cores> <pid>` to bind the VMM process. It additionally iterates through `/proc/<pid>/task/` to pin each vCPU thread individually — a belt-and-suspenders approach that handles older kernels where parent affinity changes don't automatically propagate to existing pthreads. The target cores are read from the `FC_VCPU_CORES` environment variable (default: `"2,3"`). A global `vmCoreIndex` atomic counter round-robins VMs across available cores when multiple cores are specified.

IRQ affinity is globally set to mask `0xF03` (cores 0,1,8,9,10,11), shielding cores 2–7 from all hardware interrupt delivery. The `tune_system.sh` script writes this mask to `/proc/irq/default_smp_affinity` and iterates over every existing IRQ's `smp_affinity` file.

### 3.3 Network Topology & Per-Submission Isolation

Each Firecracker microVM receives a dedicated TAP interface connected to a host bridge (`br0` at `172.16.0.1/24`). The orchestrator dynamically allocates IP addresses from the `172.16.0.0/24` subnet by scanning the VM registry for occupied addresses and returning the first free index in the range 2–254.

TAP interface names are deterministically generated from the submission ID using FNV-32a hashing: `tapNameForVM()` produces `"tap" + 8 hex chars = 11 chars`, always within the Linux `IFNAMSIZ-1 = 15` character limit regardless of submission ID length.

Traffic control is applied per-TAP at creation time in `SetupTapInterface()`:

- **Egress (host → VM):** `tc qdisc add dev <tap> root tbf rate 100mbit burst 32kbit latency 400ms` — a token bucket filter capping outbound bandwidth at 100 Mbit/s.
- **Ingress (VM → host):** A `tc` ingress qdisc with a `u32` police filter at `rate 100mbit burst 32kbit drop` — packets exceeding the rate are silently dropped at the kernel level.

This prevents a malicious contestant binary from saturating the host's network stack with traffic, which would degrade measurements for concurrent submissions.

`run-orchestrator.sh` creates the `br0` bridge, assigns `172.16.0.1/24`, enables IP forwarding (`sysctl net.ipv4.ip_forward=1`), and configures NAT masquerading via `iptables` for outbound guest connectivity.

### 3.4 Data Flow Overview

```
Contestant                                    Shadow
Binary Upload                                 Engine
     │                                           │
     ▼                                           │
┌──────────┐  gRPC SpawnVM   ┌──────────────┐   │
│submission│────────────────▶│  sandbox-    │   │
│   api    │                 │ orchestrator │   │
│  :8080   │  gRPC StartBenchmark           │   │
│          │───┐             │ Firecracker  │   │
└──────────┘   │             │ boot + TAP   │   │
               │             └──────┬───────┘   │
               ▼                    │ VSOCK      │
         ┌──────────┐              │            │
         │bot-fleet │◀─────────────┘            │
         │  :50052  │   Order JSON over         │
         │          │   persistent connection   │
         │          │──────────────────────────▶│
         │          │   gRPC MatchOrder /       │
         │          │   CancelOrder per order   │
         └────┬─────┘                           │
              │ Kafka produce                   │
              ▼ (benchmark-results topic)       │
         ┌──────────┐                           │
         │ Redpanda  │                           │
         │  :9092    │                           │
         └────┬──────┘                           │
              │ franz-go consumer                │
              ▼                                  │
         ┌──────────┐                            │
         │telemetry │                            │
         │ingester  │                            │
         └────┬─────┘                            │
              │ Batch INSERT                     │
              ▼                                  │
         ┌──────────┐                            │
         │TimescaleDB│                           │
         │  :5432    │                           │
         └────┬──────┘                           │
              │ SQL query                        │
              ▼                                  │
         ┌──────────┐    poll /api/scores        │
         │leaderboard│◀─────────────────┐       │
         │  :3001    │                  │       │
         └───────────┘           ┌──────┴──────┐│
                                 │leaderboard- ││
                                 │ui :3000     ││
                                 │ (Next.js)   ││
                                 └─────────────┘│
```

---

## 4. End-to-End Request Trace

### 4.1 Submission Ingestion

A contestant uploads their binary via `POST /submit` as a `multipart/form-data` payload containing a `submission_id` field and a `binary` file. The submission API enforces a 32 MiB upload limit (`MaxUploadSize = 32 << 20`). The binary is saved to a temporary file at `$TMPDIR/submission-<id>.bin`, and a row is inserted into the `benchmark_jobs` table in TimescaleDB (PostgreSQL) with status `PENDING`.

The submission API operates an asynchronous worker pool with 2 goroutines (`startWorkerPool(2)`). Each worker polls the `benchmark_jobs` table every 1 second using `SELECT ... WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED LIMIT 1` — a PostgreSQL advisory locking pattern that prevents two workers from claiming the same job without explicit distributed coordination.

### 4.2 Sandbox Provisioning Sequence

When a worker picks up a `PENDING` job, it calls `runBenchmarkSync()` which executes the following sequence:

1. **SpawnVM gRPC call** to the sandbox orchestrator at `localhost:50051`, requesting 1 vCPU and 256 MiB of memory.
2. The orchestrator **validates the request**, checks for duplicate VM IDs, and **allocates a free IP** from the `172.16.0.0/24` subnet.
3. A **TAP interface** is created (`ip tuntap add dev <tap> mode tap`), attached to `br0` (`ip link set <tap> master br0`), brought up, and rate-limited with `tc`.
4. A **submission disk image** is provisioned: a 64 MiB sparse ext4 image (`truncate -s 64M`) containing the contestant binary and a guest bootstrap shell script. The image is formatted with `mkfs.ext4 -F`, loop-mounted, files copied in, and unmounted.
5. The **Firecracker microVM is booted** with two drives (read-only `rootfs.ext4` as `/dev/vda`, read-write submission disk as `/dev/vdb`), a TAP network interface, and a VSOCK device. The kernel command line configures the IP address, root filesystem, and init process.
6. Post-boot, the orchestrator **pins the VMM process** to the `FC_VCPU_CORES` using `taskset` and **applies cgroup v2 limits** (1 CPU core via `cpu.max = "100000 100000"`, 256 MiB via `memory.max = "268435456"`).
7. The `SpawnResponse` returns the VM ID, allocated IP address, and VSOCK Unix socket path.

### 4.3 Benchmark Execution Pipeline

After a 100ms settle delay (`time.Sleep(100 * time.Millisecond)`), the submission API calls `StartBenchmark` gRPC on the bot fleet at `[::1]:50052`, passing the target IP, port 8080, 1000 total orders, concurrency 1, mode `"burst"`, and the VSOCK path.

The bot fleet:

1. Acquires a semaphore permit from a global limiter of 8 concurrent benchmarks (`GLOBAL_MAX_ACTIVE_BENCHMARKS = 8`).
2. Connects to the shadow engine at the URL specified by `SHADOW_ENGINE_URL` (default `http://localhost:50053`) and sends a `StartBenchmark` RPC to reset the shadow order book for this submission.
3. Spawns `concurrency` worker tasks. Each worker establishes a **persistent connection** — either a VSOCK Unix socket (via `CONNECT <port>\n` handshake) or a TCP connection with `TCP_NODELAY` — to the contestant engine.
4. The main task generates 1000 orders via `generate_single_order()` and dispatches them through an MPSC channel (`WORKER_CHANNEL_SIZE = 4096`).
5. Each worker sends the order as newline-delimited JSON over the persistent connection, captures `Instant::now()` before the write and computes elapsed microseconds after the read, then forwards the same order to the shadow engine via gRPC for correctness validation.
6. The worker compares the contestant's trade response against the shadow engine's response field-by-field (`compare_trades()`). A match is counted as success; a mismatch is logged and counted as failure.
7. After all orders complete, percentiles are computed by sorting the latency vector and indexing at p50/p90/p99/p99.9.
8. A `TelemetryPayload` is published to the `benchmark-results` Redpanda topic with LZ4 compression and `acks=1`.

### 4.4 Telemetry Capture & Propagation

The telemetry ingester consumes from the `benchmark-results` topic using the `franz-go` client library with consumer group `telemetry-ingester-group` and manual offset commits (`DisableAutoCommit()`). Records are parsed, buffered into batches of up to 500 (`MaxBatchSize = 500`), and flushed every 2 seconds (`FlushInterval = 2 * time.Second`) — whichever threshold is hit first.

Each flush executes a PostgreSQL transaction that prepares a single `INSERT INTO benchmark_metrics` statement and executes it for every record in the batch. On successful commit, the consumer commits the Kafka offsets for the persisted records. On failure, the offsets are not committed, ensuring at-least-once delivery semantics — duplicate records are possible but accepted as the lesser evil compared to data loss.

### 4.5 Score Computation & Leaderboard Update

After the benchmark gRPC call returns, the submission API polls `benchmark_metrics` in TimescaleDB for the latest record matching the submission ID, with a timeout of 5 seconds and polling interval of 250ms. If the poll succeeds, it extracts p50, p99, and success rate. If the poll times out (the telemetry pipeline hasn't flushed yet), it falls back to the latency percentiles returned directly in the `BenchmarkResponse` protobuf.

The composite score is computed by `calculateScore()` (detailed in Section 10) and written back to the `benchmark_jobs` table with status `COMPLETED`.

The leaderboard backend queries `benchmark_metrics` for the latest run per submission (using `MAX(id) GROUP BY submission_id`), re-computes the score, and sorts by score descending with p99 as tiebreaker. The Next.js frontend polls `GET /api/scores` every 1500ms and renders the ranked leaderboard.

### 4.6 Latency Budget Per Hop

```
┌────────────────────────────────────────┬─────────────────┬──────────────────┐
│ Hop                                    │ Transport       │ Approx Latency   │
├────────────────────────────────────────┼─────────────────┼──────────────────┤
│ POST /submit → save binary to disk    │ HTTP + fs write │ 5–50ms           │
│ INSERT benchmark_jobs (PostgreSQL)     │ TCP :5432       │ 0.5–2ms          │
│ Worker poll (1s sleep loop)            │ PostgreSQL poll │ 0–1000ms (avg    │
│                                        │                 │  500ms)          │
│ SpawnVM gRPC (orchestrator)            │ gRPC :50051     │ 0.5–1ms (call)   │
│ TAP + disk + Firecracker boot          │ system calls    │ 500–2000ms       │
│ CPU pin + cgroup setup                 │ taskset + fs    │ 5–20ms           │
│ Settle delay                           │ sleep           │ 100ms (fixed)    │
│ StartBenchmark gRPC (bot-fleet)        │ gRPC :50052     │ 0.5–1ms          │
│ Shadow engine StartBenchmark           │ gRPC :50053     │ 0.5–1ms          │
│ VSOCK connect + handshake              │ Unix socket     │ 1–50ms           │
│ Per-order round trip (measurement)     │ VSOCK/TCP       │ 50–5000µs (SUT)  │
│ Per-order shadow gRPC                  │ gRPC :50053     │ 50–500µs         │
│ Kafka produce (telemetry)              │ TCP :9092       │ 1–5ms            │
│ Telemetry ingester flush               │ Kafka → PG      │ 0–2000ms (batch) │
│ Submission API DB poll                 │ PostgreSQL      │ 0–5000ms         │
│ UPDATE benchmark_jobs                  │ PostgreSQL      │ 0.5–2ms          │
│ Leaderboard API query                  │ PostgreSQL      │ 2–20ms           │
│ Frontend poll                          │ HTTP :3001      │ 0–1500ms (poll)  │
├────────────────────────────────────────┼─────────────────┼──────────────────┤
│ TOTAL end-to-end                       │                 │ ~3–12 seconds    │
│ (submission to leaderboard visible)    │                 │                  │
│                                        │                 │                  │
│ MEASUREMENT WINDOW ONLY               │                 │ 50ms–5s          │
│ (depends on order count × concurrency) │                 │ (1000 orders)    │
└────────────────────────────────────────┴─────────────────┴──────────────────┘
```

The measurement uncertainty of each individual order's round-trip latency is dominated by the software timestamp resolution (~1µs on x86_64 with `rdtsc`-backed `Instant::now()`) plus kernel scheduling jitter on the host side (0–100µs without `isolcpus`, 0–10µs with `isolcpus`). See Section 6 for full analysis.
## 5. Sandbox Engine

### 5.1 Isolation Technology Selection: The Irreversible Decision

The choice of isolation technology is the single most consequential architectural decision in this platform. It determines the security boundary, the measurement overhead floor, the boot latency budget, and the operational complexity of the entire system. We evaluated three candidates: Docker containers (shared-kernel, `runc`-based), gVisor (user-space kernel), and Firecracker microVMs (hardware-virtualized).

### 5.2 CVE-2026-31431 — Why Shared-Kernel Containers Are Disqualified

In a shared-kernel container model, the contestant binary executes syscalls directly against the host kernel. A single kernel vulnerability — such as the hypothetical CVE-2026-31431 — grants container escape to any contestant binary that can trigger the vulnerable code path. Since we execute *arbitrary, untrusted binaries uploaded by anonymous participants*, the threat model demands that a kernel vulnerability in the guest environment does not compromise the host.

Docker's defense-in-depth (seccomp profiles, AppArmor, capability dropping, user namespaces) reduces the attack surface but does not eliminate it. Every one of these mitigations operates as a filter on the shared syscall interface. A bypass of any single filter layer — which is what a kernel CVE provides — collapses the entire security boundary.

Firecracker's KVM-based virtualization interposes a hardware-enforced boundary (VMX non-root mode on Intel, SVM guest mode on AMD) between the guest and host kernels. A guest kernel vulnerability compromises the guest. Reaching the host requires a separate KVM escape — a categorically different and rarer class of vulnerability.

### 5.3 gVisor Rejection — The 10–30% Syscall Tax on Benchmark Integrity

gVisor implements a user-space kernel (the "sentry") that intercepts every guest syscall and re-implements it in Go. For a benchmarking platform, this introduces a fundamental conflict: the measurement infrastructure is adding 10–30% overhead to every syscall the contestant engine makes. A contestant engine that makes 50,000 `write()` + `read()` syscall pairs during a benchmark run accumulates hundreds of microseconds of overhead that is invisible to the contestant but visible to the measurement.

The measured latency of a gVisor-isolated contestant does not reflect the contestant's actual performance — it reflects the contestant's performance convolved with gVisor's syscall emulation overhead. This violates the core measurement integrity constraint: the infrastructure must not perturb the measurement.

### 5.4 Firecracker MicroVM Architecture

Firecracker is a Virtual Machine Monitor (VMM) built on KVM that provides a minimal device model: virtio-net, virtio-block, virtio-vsock, a serial console, and a minimal PCI-less architecture (`pci=off` in kernel args). The guest boots a minimal Linux kernel (`vmlinux.bin`, 41.4 MiB) with a read-only root filesystem (`rootfs.ext4`, 300 MiB) containing a minimal userspace.

The Firecracker binary (`firecracker-bin`, 2.7 MiB) is invoked by the Go SDK's `VMCommandBuilder`, which constructs the process with stdout/stderr piped to the host for serial console debugging. The VMM process lifetime is managed by a detached `context.Background()` — deliberately not the boot timeout context — to prevent the VM from being killed when the boot function returns.

Each VM is configured via the `VMConfig` struct:

```go
type VMConfig struct {
    VMID             string
    VCPUCount        int64     // Default: 1
    MemoryMiB        int64     // Default: 256
    KernelImagePath  string    // ../../sandbox/vmlinux.bin
    FirecrackerBin   string    // ../../sandbox/firecracker-bin
    SocketDir        string    // /tmp
    TapName          string    // tapXXXXXXXX (FNV-32a hash)
    AllocatedIP      string    // 172.16.0.N
    SubmissionDisk   string    // /tmp/submission-<id>.ext4
    VsockPath        string    // /tmp/vsock-<id>.sock
    CID              uint32    // ipIndex + 10 (unique per VM)
}
```

### 5.5 Dual-Drive Submission Model (rootfs + /dev/vdb)

The Firecracker config attaches two block devices:

- **Drive `rootfs`** (`/dev/vda`): The base operating system image. Mounted read-only. Shared across all VMs. Contains the minimal Linux userspace and the `init` binary.
- **Drive `submission`** (`/dev/vdb`): A per-submission 64 MiB ext4 image containing the contestant binary (`strategy_bin`) and a bootstrap script (`run.sh`). Mounted read-write. Created fresh for each submission and destroyed during teardown.

The submission image is created by `CreateSubmissionImage()` in `disk.go`:

1. Create a sparse file with `truncate -s 64M`.
2. Format with `mkfs.ext4 -F`.
3. Loop-mount the image to a temporary directory.
4. Copy `run.sh` and `strategy_bin` into the mount.
5. Unmount.

The decision to use loop-mount instead of `debugfs` was deliberate: `debugfs` bypasses the journal, which causes the kernel inside the VM to not see the files when it mounts the block device. Loop-mounting writes through the filesystem journal and guarantees visibility.

### 5.6 Guest Bootstrap Protocol

The guest init process mounts `/dev/vdb` and executes `run.sh`, which:

```bash
#!/bin/sh
if [ -f ./strategy_bin ]; then
    chmod +x ./strategy_bin
    ./strategy_bin
else
    echo "[GUEST SBOX] Error: No strategy_bin payload discovered."
fi
sleep 10
reboot -f
```

The `sleep 10` provides a teardown safety window — it prevents the VM from rebooting before the benchmark has completed and the orchestrator has received the results. The `reboot -f` is a forced reboot that triggers Firecracker's `panic=1` kernel parameter, causing a clean VM shutdown.

The contestant binary (`strategy_bin.c`, compiled statically with `gcc -O2 -static`) is a production-grade matching engine that:

- Binds to both `AF_INET` port 8080 (TCP) and `AF_VSOCK` port 8080 simultaneously.
- Uses `epoll` with edge-triggered (`EPOLLET`) mode for connection multiplexing.
- Implements a full price-time priority limit order book with the same matching semantics as the shadow engine.
- Parses newline-delimited JSON, matches orders, and returns JSON responses with trade arrays.
- Handles up to 256 concurrent clients and 131,072 active orders.

### 5.7 CPU Pinning & IRQ Affinity Implementation

CPU pinning happens in two phases:

**Phase 1: VMM Process Pinning** (`pinFirecrackerProcess()` in `firecracker.go`). After Firecracker boots, the orchestrator calls `machine.PID()` to get the VMM process ID. It then invokes `taskset -pc <cores> <pid>` to set the CPU affinity mask. A global atomic counter (`vmCoreIndex`) distributes VMs across the available `FC_VCPU_CORES` in round-robin fashion. The function also iterates `/proc/<pid>/task/` to pin each individual thread (vCPU thread, API thread, VMM thread).

**Phase 2: Cgroup v2 Hard Limits** (`applyCgroupLimits()` in `firecracker.go`). A cgroup is created at `/sys/fs/cgroup/hft-bench/vm-<id>/` with:
- `cpu.max = "100000 100000"` — exactly 1 CPU core's worth of execution time per 100ms period.
- `memory.max = "268435456"` — 256 MiB hard memory limit.
The Firecracker PID is written to `cgroup.procs` to move the entire process tree into the cgroup.

### 5.8 Warmup Gate — Eliminating Cold-Start Measurement Pollution

The submission API inserts a 100ms delay between `SpawnVM` returning and `StartBenchmark` being called (`time.Sleep(100 * time.Millisecond)` in `runBenchmarkSync()`). This allows:

- The Firecracker VM to complete kernel initialization and start the init process.
- The contestant binary to bind to its listening socket.
- The VSOCK proxy to establish its Unix domain socket on the host.

The bot fleet workers additionally implement a connection retry loop with 50ms backoff:

```rust
let mut us = loop {
    match tokio::time::timeout(Duration::from_millis(100),
        tokio::net::UnixStream::connect(vsock_path)).await {
        Ok(Ok(s)) => s,
        _ => { tokio::time::sleep(Duration::from_millis(50)).await; continue; }
    };
    // ... VSOCK handshake ...
};
```

This retry loop serves as an implicit warmup gate: the benchmark does not begin timing until the connection is established and the handshake completes. Orders sent before the engine is ready result in connection failures that trigger retries, not erroneous latency measurements.

### 5.9 Teardown & Ephemeral Storage Cleanup

Teardown is triggered by the submission API via `TeardownVM` gRPC after the benchmark completes (in a `defer` block, ensuring cleanup even on errors). The teardown sequence:

1. `machine.StopVMM()` — sends `SIGTERM` to the Firecracker process, which triggers a clean KVM shutdown.
2. Delete the VM from the in-memory registry.
3. `CleanupTapInterface()` — `ip link delete <tap>` removes the TAP device and implicitly detaches it from `br0`.
4. `CleanupSubmissionImage()` — `os.Remove()` deletes the per-submission ext4 image from `/tmp`.

The `shutdownAllVMs()` method iterates the entire VM registry on orchestrator shutdown (SIGINT/SIGTERM), performing the same cleanup for every active VM. This prevents TAP interface and disk image leaks across restarts.

### 5.10 Security Boundary Analysis

The Firecracker security boundary provides:

- **Hardware-enforced memory isolation**: KVM's Extended Page Tables (EPT on Intel, NPT on AMD) prevent the guest from accessing host physical memory.
- **Restricted device model**: No PCI passthrough, no USB, no GPU — the attack surface is limited to virtio-net, virtio-block, virtio-vsock, and the serial console.
- **Cgroup resource limits**: CPU and memory ceilings prevent a malicious guest from monopolizing host resources.
- **Network bandwidth limits**: `tc` rate limiting on the TAP interface prevents bandwidth-based denial of service.
- **Ephemeral storage**: The submission disk is created fresh and destroyed after each run. No persistent state survives across submissions.

What the boundary does *not* provide: protection against a KVM escape exploit. A zero-day in KVM's instruction emulation or device model code could allow a guest to execute code on the host. This is the residual attack surface documented in Section 13.6.

---

## 6. The Measurement Architecture

### 6.1 What Tick-to-Trade Latency Actually Measures in This System

The bot fleet measures the elapsed time between writing an order's JSON payload to the persistent connection and completing the read of the response. In code:

```rust
let start = Instant::now();
stream.write_all(payload.as_bytes()).await;  // ← timestamp T1
// ...
stream.read(&mut buffer).await;              // ← timestamp T2
let latency = start.elapsed().as_micros();   // T2 - T1
```

This measurement includes:
- Host-side kernel TCP/VSOCK stack processing (outbound).
- VSOCK proxy traversal (host UDS → Firecracker VMM → guest virtio-vsock driver).
- Guest kernel network stack processing (inbound).
- Contestant engine processing time (JSON parse → order book match → JSON serialize).
- Guest kernel network stack processing (outbound response).
- VSOCK proxy traversal (return path).
- Host-side kernel stack processing (inbound).

It does *not* measure:
- Wire propagation delay (negligible on localhost/VSOCK).
- Time spent in the bot fleet's own code between `Instant::now()` and the `write_all()` call (negligible, no allocation in the path).

### 6.2 Timestamp Capture Points & Error Sources

| Error Source | Magnitude | Notes |
|---|---|---|
| `Instant::now()` resolution | ~25–100ns | Uses `CLOCK_MONOTONIC` backed by `rdtsc` on x86_64. The TSC invariant on modern Intel/AMD ensures monotonicity. |
| Tokio task scheduling jitter | 0–50µs | The `write_all()` and `read()` are async. If the Tokio executor is busy, the task may be delayed between the `Instant::now()` capture and the actual syscall. |
| Host kernel VSOCK stack | 5–30µs | Context switch into the kernel, virtio ring buffer processing, data copy between host and VMM address spaces. |
| Firecracker VMM VSOCK proxy | 2–10µs | The VMM forwards data between the host-side Unix domain socket and the guest-side virtio-vsock device via a userspace copy loop. |
| Guest kernel virtio-vsock driver | 2–10µs | Interrupt delivery to guest, virtio ring processing, data copy into guest userspace buffer. |
| Host CPU scheduling (without isolcpus) | 0–100µs | If the bot-fleet's Tokio worker thread is preempted by a kernel task or IRQ during the measurement window, the elapsed time includes the preemption. |
| Host CPU scheduling (with isolcpus) | 0–5µs | With `isolcpus=2-7 nohz_full=2-7`, the kernel does not schedule tasks or deliver timer ticks on the trading cores. Residual jitter is from NMIs only. |

### 6.3 Measurement Uncertainty Quantification

The total measurement uncertainty floor — the minimum error that cannot be eliminated with the current software timestamp approach — is approximately ±10–50µs per individual order measurement. This is the sum of the Tokio scheduling jitter, the VSOCK stack traversal, and the host kernel scheduling jitter.

For aggregate percentiles computed over 1000 orders, the law of large numbers reduces the impact of random jitter on the median. However, tail percentiles (p99, p99.9) are by definition influenced by the worst-case scheduling events. A single 100µs preemption during 1000 orders will appear in the p99.9 (the single worst measurement out of 1000).

### 6.4 Why Two Submissions Within the Uncertainty Band Cannot Be Ranked

If contestant A has a p99 of 150µs and contestant B has a p99 of 170µs, the 20µs difference falls within the ±50µs measurement uncertainty band. The leaderboard would rank A above B, but this ranking is not statistically meaningful — it could be entirely an artifact of scheduling noise on the host during A's benchmark run.

The scoring algorithm does not currently implement statistical tie detection. This is documented as a known limitation in Section 10.7. A production system would require multiple benchmark runs per submission with confidence interval computation to distinguish real performance differences from measurement noise.

### 6.5 Production Path: Hardware PTP + PHC + XDP

To reduce the measurement floor below 1µs, the production hardening path requires:

- **Hardware PTP (Precision Time Protocol)** with a PTP-capable NIC that provides hardware timestamps at the PHY layer, eliminating kernel stack traversal from the measurement path entirely.
- **PHC (PTP Hardware Clock)** synchronization between the load generator and the system under test, providing sub-microsecond clock alignment.
- **XDP (eXpress Data Path)** or **DPDK** for kernel-bypass packet processing, eliminating the kernel's TCP/IP stack from the measurement path.

With these components, the measurement uncertainty floor drops to ±100ns — sufficient to meaningfully discriminate contestants at the single-microsecond level that production HFT systems operate at.

---

## 7. Bot Fleet

### 7.1 Architecture & Concurrency Model

The bot fleet is a Rust service built on `tokio` (multi-threaded runtime) and `tonic` (gRPC). It exposes a single gRPC service `BenchmarkController` on `[::1]:50052` with one RPC: `StartBenchmark`. The service is designed to handle multiple concurrent benchmark runs, limited by a global semaphore of 8 (`GLOBAL_MAX_ACTIVE_BENCHMARKS`).

Each benchmark invocation spawns `concurrency` (default: 1) async worker tasks that share a single MPSC channel for job distribution. The main task generates all orders sequentially and sends them through the channel; workers receive orders, dispatch them to the contestant engine, forward them to the shadow engine, validate the response, and report success/failure through a separate result channel.

The concurrency model uses a shared `Arc<Mutex<mpsc::Receiver>>` pattern for work-stealing across workers. While this introduces contention on the mutex for each receive, it provides natural load balancing — a worker that finishes processing an order faster will pick up the next one before a slower worker. At concurrency 1 (the current default), there is no contention.

### 7.2 Order Generation: Market Microstructure Simulation

The `generate_single_order()` function produces a synthetic order stream designed to exercise all code paths in a matching engine. Each order is assigned a monotonically increasing `order_id = index + 1`. The function maintains a `Vec<u64>` of active limit order IDs that grows as limits are placed and shrinks as cancels remove entries.

Prices are uniformly distributed in the range 90–109 (`rng.gen_range(90..110)`). This narrow range ensures frequent price crossing between bids and asks, which generates trades and exercises the matching engine's core hot path. Quantities are uniformly distributed in 1–49 for limit orders and 1–19 for market orders.

### 7.3 The 60/25/15 Order Mix — Rationale & Implementation

The order type distribution is determined by a `roll = rng.gen_range(1..=100)`:

- **Rolls 1–60 (60%)**: Limit orders. These build the order book depth and represent the dominant order type in real markets. Each limit order is tracked in `active_limit_orders` for potential future cancellation.
- **Rolls 61–85 (25%)**: Market orders. These cross the spread and generate trades. A higher market order percentage would drain the book too quickly; 25% maintains sufficient resting liquidity for meaningful matching.
- **Rolls 86–100 (15%)**: Cancel orders. The target order ID is selected randomly from `active_limit_orders` and removed. If no active orders exist, the cancel targets the current order ID (which will be a cancel-not-found on the contestant side — see Section 8.7).

This 60/25/15 mix produces a realistic order book that builds depth through limit orders, generates trades through market orders, and tests cancellation handling — all three code paths that a production matching engine must support.

### 7.4 Persistent Connection Strategy — The 34ms → 4ms Engineering Decision

Each worker establishes a single persistent TCP or VSOCK connection to the contestant engine at the start of the benchmark and reuses it for all orders. The alternative — opening a new connection per order — would add TCP three-way handshake latency (~34ms on loopback under load due to SYN retransmit backoff) to every measurement, dwarfing the actual order processing time.

For VSOCK connections, the worker connects to the host-side Unix domain socket path returned by the orchestrator and performs a text-based handshake:

```
Client: CONNECT 8080\n
Server: OK ...\n
```

This `CONNECT` handshake is a Firecracker VSOCK proxy protocol — the host-side UDS multiplexes connections to the guest's `AF_VSOCK` port. The handshake establishes the guest-port binding. After the handshake, the connection behaves as a bidirectional byte stream.

For TCP connections, `TCP_NODELAY` is set immediately after connection to disable Nagle's algorithm. Without this, the kernel would buffer small writes (our JSON order payloads are typically 50–100 bytes) and delay sending until either 200ms elapse or a full MSS accumulates — adding 200ms of latency to every order.

### 7.5 Parallel Dispatch: Contestant Engine + Shadow Engine Simultaneously

For each order, the worker sends the order to the contestant engine over the persistent connection and *also* sends the same order to the shadow engine over gRPC — sequentially, not in parallel. The shadow gRPC call happens after the contestant response is received, which means the shadow engine's processing time does not overlap with the contestant's measured latency. This is intentional: measuring the contestant and shadow concurrently could cause resource contention on the shadow engine's cores that feeds back into the measurement.

The shadow engine connection uses a cloned `BenchmarkWorkerClient` per worker. The submission ID is passed as a gRPC metadata header (`submission-id`) to route the order to the correct per-submission order book.

### 7.6 Thundering Herd Synchronization

When `concurrency > 1`, all workers start their connection retry loops simultaneously, which could cause a thundering herd on the contestant engine's accept queue. The current implementation does not stagger worker startup — all workers attempt to connect in parallel with 50ms retry intervals. For the default concurrency of 1, this is not an issue. At higher concurrency values, this would stress the contestant's connection acceptance path as part of the benchmark — which is arguably the correct behavior, since a production matching engine must handle concurrent connection establishment.

### 7.7 SPSC Queue Design & Cache-Line Isolation

The Cargo.toml includes `crossbeam-queue`, `crossbeam-channel`, and `crossbeam-utils` as dependencies, indicating an intention to use lock-free SPSC queues with cache-line padding for inter-thread communication. However, the current implementation uses `tokio::sync::mpsc` channels instead. The crossbeam dependencies are present but unused in the current code — they represent infrastructure for a planned optimization to replace the tokio Mutex-guarded receiver with a lock-free crossbeam channel for lower-latency job distribution. This is documented as a production hardening path item.

### 7.8 gRPC Control Protocol

The bot fleet implements the `BenchmarkController` gRPC service defined in `benchmark.proto`:

```protobuf
service BenchmarkController {
    rpc StartBenchmark(BenchmarkRequest) returns (BenchmarkResponse);
}
```

The `BenchmarkRequest` carries the target VM's IP, port, total order count, concurrency level, mode (`"burst"`), and VSOCK path. The `BenchmarkResponse` returns success/failure, total execution time in milliseconds, successful and failed order counts, and latency percentiles (p50, p90, p99, p99.9).

The bot fleet also acts as a *client* of the shadow engine's `BenchmarkWorker` service for `MatchOrder` and `CancelOrder` RPCs.

---

## 8. Shadow Order Book & Correctness Validation

### 8.1 Why Correctness Validation Cannot Be Skipped

A leaderboard that ranks only on latency incentivizes contestants to return empty trade arrays instantly — achieving sub-microsecond p99 by doing no work. Correctness validation closes this attack: every order's response from the contestant engine is compared field-by-field against the shadow engine's deterministic response. A contestant that returns incorrect trades is penalized through the `success_rate` metric (Section 10.5).

### 8.2 Limit Order Book Implementation

The shadow engine (`shadow-engine/src/main.rs`) implements a price-time priority limit order book using:

- **Bids**: `BTreeMap<Reverse<u64>, VecDeque<Order>>` — prices sorted in descending order (highest bid first). The `Reverse` wrapper inverts the `BTreeMap`'s natural ascending sort.
- **Asks**: `BTreeMap<u64, VecDeque<Order>>` — prices sorted in ascending order (lowest ask first). The `BTreeMap`'s natural ordering provides the correct ask-side priority.
- **Order Index**: `AHashMap<u64, OrderIndexEntry>` — O(1) lookup by order ID for cancellation and duplicate detection. Pre-allocated with capacity 131,072 (`AHashMap::with_capacity(131_072)`).

The choice of `BTreeMap` over a `HashMap` is dictated by the access pattern: the matching algorithm requires ordered iteration from the best price level, which `BTreeMap` provides in O(log n) via `first_key_value()`. A `HashMap` would require sorting all keys on every match — O(n log n) per order. A skip list would provide similar O(log n) access but with higher constant factors due to pointer chasing; `BTreeMap`'s cache-friendly node layout (B-tree nodes store multiple keys contiguously) provides better real-world performance for the small number of distinct price levels (typically 20–30 in the 90–109 range).

`AHashMap` (from the `ahash` crate) is chosen over the standard library `HashMap` because `ahash` uses AES hardware instructions for hashing on x86_64, providing ~2x faster hash computation for integer keys compared to `SipHash-2-4`. For an order book that performs a hash lookup on every order submission, cancellation, and match, this saves ~100ns per operation.

### 8.3 Price-Time Priority: Exact Matching Algorithm

For a buy order, the matching algorithm:

1. Peeks at the best ask price via `self.asks.first_key_value()` — O(log n).
2. If the incoming order is a limit order and its price is below the best ask, matching stops (no price improvement).
3. Uses a split-borrow pattern (`split_asks_and_index()`) to obtain simultaneous mutable references to both `asks` and `order_index` — necessary because Rust's borrow checker cannot prove that `self.asks` and `self.order_index` are disjoint fields without explicit splitting.
4. Iterates the `VecDeque` at the best ask level in FIFO order (time priority), executing partial fills until either the incoming order's quantity is exhausted or the level is drained.
5. Removes fully filled maker orders from both the `VecDeque` and the `order_index`.
6. If the level's `VecDeque` is empty, removes the price level from the `BTreeMap`.
7. If the incoming order has remaining quantity and is a limit order, inserts it into the bid side.

Sell-side matching is symmetric, operating against the bid `BTreeMap` with `Reverse<u64>` keys.

### 8.4 The ABA Problem — Detection Strategy

The ABA problem in a lock-free order book occurs when:

1. Thread A reads order O at memory location M with value V1.
2. Thread B removes O from location M.
3. Thread C inserts a *different* order O' at the same memory location M with value V1' (which happens to have the same pointer/index).
4. Thread A's CAS on M succeeds because the pointer matches, but the underlying data has changed.

The shadow engine avoids this problem entirely by not using lock-free data structures. It uses `parking_lot::Mutex<OrderBook>` to serialize all access to the order book for a given submission. Every `match_order` and `cancel_order` call acquires the mutex, performs the operation, and releases it. This eliminates the ABA problem at the cost of serializing all operations on a single submission's order book.

The per-submission mutex is wrapped in `Arc<parking_lot::RwLock<AHashMap<String, Arc<Mutex<OrderBook>>>>>` — the outer `RwLock` protects the map of submission IDs to order books, while the inner `Mutex` protects each individual order book. This allows concurrent benchmarks to proceed in parallel (different submissions acquire different inner mutexes) while individual benchmark's orders are serialized.

For the current system, where each benchmark runs with concurrency 1 and orders are dispatched sequentially through a single MPSC channel, serialization is the correct behavior — the shadow engine must process orders in the same sequence as the contestant engine to produce deterministic results. At higher concurrency, where multiple bot fleet workers send orders in parallel, the mutex serialization introduces a bottleneck — but this matches the reality that order book matching is inherently serial (price-time priority requires a total order on incoming events).

### 8.5 Exactly-Once Sequence Deduplication

The shadow engine rejects duplicate order IDs via the `order_index` check at the top of `submit_order()`:

```rust
if self.order_index.contains_key(&incoming.id) {
    return Vec::new();
}
```

If a network retry causes the bot fleet to re-send an order with the same ID, the shadow engine returns an empty trade vector rather than double-processing the order. The contestant engine is expected to implement equivalent deduplication — the reference implementation in `strategy_bin.c` uses `max_order_id_processed` to track the highest processed order ID and skips duplicates.

### 8.6 Correctness Rate: What It Measures and What It Misses

The `compare_trades()` function in the bot fleet compares the contestant's trade response against the shadow engine's response:

```rust
fn compare_trades(shadow: &[TradeSignal], contestant: &[ContestantTrade]) -> bool {
    if shadow.len() != contestant.len() { return false; }
    for (s, c) in shadow.iter().zip(contestant.iter()) {
        if s.maker_order_id != c.maker_order_id
            || s.taker_order_id != c.taker_order_id
            || s.price != c.price
            || s.quantity != c.quantity
        { return false; }
    }
    true
}
```

This comparison is **field-by-field and order-sensitive**: the trades must appear in the same sequence with identical maker/taker IDs, prices, and quantities. A contestant that produces the correct set of trades but in a different order will fail the correctness check.

What the correctness rate **misses**: it does not validate the contestant's order book *state* between orders — only the trade outputs. A contestant engine that produces correct trades but maintains an internally inconsistent order book (e.g., stale entries that should have been removed) will pass the correctness check until a future order triggers a divergence.

### 8.7 Cancel-Not-Found: Expected Behavior Analysis

When the `active_limit_orders` vector in `generate_single_order()` is empty and a cancel roll (86–100) occurs, the function falls back to canceling the current order ID — an order that was never placed as a limit:

```rust
let target_order_id = if active_limit_orders.is_empty() {
    order_id  // cancel a non-existent order
} else {
    let idx = rng.gen_range(0..active_limit_orders.len());
    active_limit_orders.remove(idx)
};
```

The shadow engine handles this gracefully — `cancel_order()` returns `false` when the order ID is not found in the `order_index`. The bot fleet does not compare cancel responses for correctness (the cancel RPC is fire-and-forget from a validation perspective). A contestant engine that returns an error or empty response for a cancel-not-found will not be penalized.

### 8.8 Known Limitations of the Current Validator

1. **Sequential ordering assumption**: The bot fleet sends each order to the contestant and shadow engine sequentially. At concurrency > 1, the order in which workers process orders is non-deterministic (depends on Tokio task scheduling), meaning the shadow engine may receive orders in a different sequence than the contestant engine. This can cause false correctness failures. The current system operates at concurrency 1, which eliminates this issue.

2. **No state comparison**: The validator compares outputs (trades) but not internal state (order book depth, best bid/ask). A contestant that silently corrupts its internal state will only be detected when the corruption produces a trade divergence.

3. **Memory growth cap**: The shadow engine clears all order books when the map exceeds 100 entries (`if books.len() > 100 { books.clear(); }`). This is a safety valve against memory exhaustion under continuous load, but it means that long-running continuous benchmarks could lose shadow engine state for active submissions.
## 9. Telemetry & Validation Pipeline

### 9.1 Why Redpanda Over Kafka

We chose Redpanda (v23.2.1) over Apache Kafka for the telemetry data plane. Redpanda is a Kafka-API-compatible streaming platform written in C++ (Seastar framework) that operates without the JVM. On the 6-core host where this system runs, Kafka's JVM would compete for heap memory and trigger GC pauses that create latency spikes observable in the telemetry pipeline's flush timing. Redpanda's thread-per-core architecture and zero-copy I/O path eliminate GC entirely and reduce tail latency on produce/consume operations by approximately 10x at the p99 compared to Kafka on equivalent hardware.

The Docker Compose configuration runs Redpanda in `dev-container` mode with `--smp=1 --memory=1G --reserve-memory=0M --overprovisioned` — a single Seastar reactor thread with 1 GiB of memory, pinned to cores 10,11 via `cpuset`. The `--overprovisioned` flag disables Seastar's CPU resource management, which is unnecessary when the process is externally pinned.

The trade-off: Redpanda's dev-container mode disables replication and persistence guarantees. If the Redpanda process crashes, uncommitted messages in the ring buffer are lost. For benchmark telemetry, this is acceptable — a lost telemetry message means one benchmark result is not recorded, which triggers a re-run rather than data corruption.

### 9.2 Topic Architecture & Message Schema

The system uses a single Kafka topic: `benchmark-results`. The bot fleet produces messages keyed by `submission_id` (ensuring all messages for a submission land on the same partition for ordering) with a JSON payload:

```json
{
  "submission_id": "example-submission-01",
  "total_orders": 1000,
  "successful_orders": 985,
  "failed_orders": 15,
  "success_rate": 0.985,
  "p50_micros": 120,
  "p90_micros": 280,
  "p99_micros": 450,
  "total_time_ms": 2340,
  "timestamp": 1718384400
}
```

The producer is configured with LZ4 compression (`compression.type = "lz4"`) for bandwidth efficiency — at ~200 bytes per message, compression provides minimal size reduction but LZ4's encode/decode overhead is negligible (~100ns per message). `acks = "1"` requires acknowledgment from the partition leader only (not replicas), which is the correct setting for a single-node deployment. `message.timeout.ms = "5000"` and `queue.buffering.max.messages = "100000"` provide backpressure — if the queue fills (e.g., Redpanda is down), produce calls will block for up to 5 seconds before failing.

### 9.3 HDR Histogram Aggregation for Tail Latency

The bot fleet computes percentiles using a sorted vector approach rather than HDR Histograms:

```rust
fn calculate_percentiles(latencies: &mut Vec<u64>) -> LatencyPercentiles {
    latencies.sort_unstable();
    let len = latencies.len();
    LatencyPercentiles {
        p50_micros: percentile(latencies, len, 50),
        p90_micros: percentile(latencies, len, 90),
        p99_micros: percentile(latencies, len, 99),
        p99_9_micros: latencies[((len * 999) / 1000).min(len - 1)],
    }
}
```

`sort_unstable()` uses pattern-defeating quicksort (pdqsort), which is O(n log n) with good cache locality. For 1000 elements, this completes in ~10µs — negligible compared to the benchmark execution time. The percentile function uses `(len * p) / 100` as the index, which is the nearest-rank method.

The decision to use sorted vector over HDR Histogram is driven by the data volume: at 1000 orders per benchmark, the entire latency vector fits in ~8 KiB (1000 × 8 bytes). HDR Histogram's value is in its fixed-memory representation of millions of samples — overkill for this data volume. The production hardening path would introduce HDR Histograms when scaling to 1M+ orders per benchmark run.

### 9.4 SQLite Storage — Tradeoffs vs TimescaleDB

The system originally used SQLite for telemetry storage (the `telemetry.db` file and `DBFile` constants are still present in the codebase). The current implementation has been migrated to TimescaleDB (PostgreSQL with the TimescaleDB extension), as evidenced by:

- `database/sql` with `_ "github.com/lib/pq"` driver imports in the telemetry ingester, submission API, and leaderboard.
- `DATABASE_URL` environment variables pointing to `postgres://postgres:password@localhost:5432/hft_telemetry`.
- `create_hypertable('benchmark_metrics', 'timestamp', chunk_time_interval => 86400000)` call in the telemetry ingester's schema initialization.
- A `timescaledb` service in `docker-compose.yml` using `timescale/timescaledb-ha:pg15`.

The migration from SQLite to TimescaleDB was driven by concurrent access requirements: SQLite's single-writer lock would serialize the telemetry ingester's batch INSERTs against the leaderboard's read queries, creating contention under load. TimescaleDB supports concurrent readers and writers natively through MVCC, and the hypertable's automatic time-based partitioning improves query performance for the leaderboard's `MAX(id) GROUP BY submission_id` aggregation pattern.

The `benchmark_metrics` table schema:

```sql
CREATE TABLE IF NOT EXISTS benchmark_metrics (
    id SERIAL,
    submission_id TEXT NOT NULL,
    p50_micros BIGINT NOT NULL,
    p90_micros BIGINT NOT NULL,
    p99_micros BIGINT NOT NULL,
    total_orders BIGINT NOT NULL,
    success_rate DOUBLE PRECISION NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (id, timestamp)
);
```

The composite primary key `(id, timestamp)` is required by TimescaleDB for hypertable partitioning — the partition key (`timestamp`) must be part of the primary key.

### 9.5 Backpressure Behavior & Cascade Prevention

The telemetry pipeline implements backpressure at three levels:

1. **Kafka producer queue**: The bot fleet's `rdkafka` producer buffers up to 100,000 messages. If the buffer fills (Redpanda unreachable), `send()` blocks for up to 2 seconds. The benchmark still completes — telemetry loss does not block scoring because the submission API falls back to the `BenchmarkResponse` latency values.

2. **Consumer → storage channel**: The telemetry ingester uses a bounded channel of 10,000 records (`make(chan StorageRecord, 10_000)`). If the storage worker falls behind (PostgreSQL slow), the consumer blocks on channel send, which propagates backpressure to the Kafka poll loop — the consumer stops fetching new records until the channel has space.

3. **Batch flush**: The storage worker batches up to 500 records or 2 seconds, whichever comes first. If the PostgreSQL INSERT fails, the records are dropped (logged but not retried), and Kafka offsets are not committed. On the next consumer restart, the records are re-fetched from the last committed offset. This provides at-least-once semantics with potential duplicates — acceptable for metrics where a duplicate data point is harmless.

---

## 10. Composite Scoring Algorithm

### 10.1 Why Single-Metric Leaderboards Are Gameable

A latency-only leaderboard rewards contestants who return empty responses instantly. A throughput-only leaderboard rewards contestants who process orders without matching them. Any single-metric leaderboard has a degenerate strategy that achieves a perfect score without actually implementing the required functionality.

The composite score combines four orthogonal metrics — latency, throughput, correctness, and stability — each measuring a different dimension of matching engine quality. Gaming any one dimension without maintaining the others produces a suboptimal composite score.

### 10.2 Formula Derivation: 40/30/20/10 Weight Rationale

The composite score is:

```
score = 0.40 × latencyScore + 0.30 × tpsScore + 0.20 × correctnessScore + 0.10 × stabilityScore
```

**40% latency** is the dominant weight because this is an HFT benchmarking platform — latency is the primary differentiator. **30% throughput** rewards engines that maintain low latency under load (if p99 is already low, the throughput proxy is already high — this creates alignment). **20% correctness** provides a significant penalty for incorrect matching but does not dominate — a contestant with 95% correctness and 100µs p99 should rank above a contestant with 100% correctness and 1000µs p99. **10% stability** rewards consistent performance (low p50/p99 ratio) but has lower weight because some jitter is inherent in the measurement infrastructure.

### 10.3 Latency Score Component

```go
latencyScore := (400.0 / float64(p99Micros)) * 100.0
if latencyScore > 100.0 {
    latencyScore = 100.0
}
```

A p99 of 400µs scores 100 points (the ceiling). A p99 of 800µs scores 50 points. A p99 of 4000µs scores 10 points. The 400µs reference point was chosen as an achievable target for a well-optimized matching engine running inside a Firecracker VM with VSOCK communication — the reference implementation (`strategy_bin.c`) achieves approximately 100–300µs p99 under the default 1000-order workload.

### 10.4 Throughput Score Component

```go
tpsScore := (600.0 / float64(p99Micros)) * 100.0
if tpsScore > 100.0 {
    tpsScore = 100.0
}
```

This is a throughput *proxy* derived from latency, not a direct TPS measurement. The assumption: if an engine can process a single order in p99 = 600µs, it can sustain ~1667 orders/second at the p99 level. A p99 of 600µs scores 100 points. This metric is correlated with the latency score but uses a more lenient reference point (600µs vs 400µs), which means an engine with p99 between 400–600µs gets full latency credit but slightly reduced throughput credit.

### 10.5 Correctness Score Component

```go
correctnessScore := successRate * 100.0
```

The `successRate` is the ratio of successfully validated orders to total orders (`success / (success + failed)`). An order is "successful" if the contestant's trade response matches the shadow engine's response field-by-field (see Section 8.6). A 100% correctness rate scores 100 points. A 95% rate scores 95 points.

### 10.6 Stability Score: p50/p99 Ratio as Jitter Proxy

```go
stabilityScore := (float64(p50Micros) / float64(p99Micros)) * 100.0
if stabilityScore > 100.0 {
    stabilityScore = 100.0
}
```

This metric measures how close the median latency is to the tail latency. A ratio of 1.0 (p50 equals p99) indicates perfectly consistent performance — no jitter. A ratio of 0.1 (p50 is 10% of p99) indicates high variance — the engine is fast on average but has severe outliers.

This is an imperfect proxy for jitter. It does not distinguish between an engine with consistently moderate latency (e.g., p50=200µs, p99=200µs → stability=100) and an engine with bimodal behavior that happens to have matching p50 and p99. A more robust stability metric would use the coefficient of variation or the interquartile range. This is a known limitation.

### 10.7 Statistical Tie Detection

The current scoring algorithm does not implement statistical tie detection. Two submissions with identical composite scores are ranked by p99 as a tiebreaker:

```go
sort.Slice(leaderboard, func(i, j int) bool {
    if leaderboard[i].Score != leaderboard[j].Score {
        return leaderboard[i].Score > leaderboard[j].Score
    }
    return leaderboard[i].P99Micros < leaderboard[j].P99Micros
})
```

Floating-point equality comparison (`!=`) is fragile — scores that differ by less than `float64` machine epsilon will be treated as equal. A production system would define a tie threshold (e.g., scores within 0.5 points are tied) and rank tied submissions alphabetically or by submission time.

### 10.8 Score Implementation: Bug Found, Root Cause, Fix

The scoring formula contains a latent bug in the interplay between the `* 100.0` scaling and the `> 100.0` ceiling.

**The bug**: Both `latencyScore` and `tpsScore` multiply the ratio by `100.0` before clamping:

```go
latencyScore := (400.0 / float64(p99Micros)) * 100.0
if latencyScore > 100.0 { latencyScore = 100.0 }
```

For any `p99Micros <= 400`, the raw `latencyScore` exceeds 100.0 and is clamped to 100.0. Similarly, for any `p99Micros <= 600`, `tpsScore` is clamped to 100.0. This means **every contestant with p99 below 400µs receives the same latency score (100.0) regardless of whether their p99 is 50µs or 399µs**. The formula loses all discriminating power in the exact range where discrimination matters most — the top of the leaderboard.

**Root cause**: The `* 100.0` is applied as a scaling factor to convert the ratio to a percentage scale (0–100), but the reference point (400µs for latency, 600µs for throughput) was tuned assuming a different scale. The formula should either:

(a) Use a reference point that represents the *best expected* performance (e.g., 50µs), so that the 100-point ceiling is only hit by truly exceptional engines, or
(b) Remove the ceiling entirely and let scores exceed 100 on individual components, relying on the weighted sum to produce the final ranking.

**Why it went undetected**: The default benchmark runs the reference implementation (`strategy_bin.c`), which achieves p99 well below 400µs. Since the reference implementation immediately hits the 100.0 ceiling on both latency and throughput, all test runs produced near-identical composite scores — the bug manifests only when comparing contestants in the sub-400µs range, which is the entire competitive field.

**The fix**: Replace the linear scaling with a logarithmic scoring function that provides continuous discrimination across the full performance range:

```go
// Before (bugged)
latencyScore := (400.0 / float64(p99Micros)) * 100.0

// After (fixed — example, not yet in codebase)
latencyScore := math.Max(0, 100.0 - 20.0*math.Log10(float64(p99Micros)/50.0))
```

This gives 100 points at 50µs, 80 points at 500µs, 60 points at 5000µs — a smooth curve with no clamping discontinuity.

---

## 11. Real-Time Leaderboard

### 11.1 Backend API Design

The leaderboard backend (`services/leaderboard/main.go`) is a Go HTTP server on port 3001 with three endpoints:

- `GET /api/scores` — Returns the full leaderboard with all metrics, ranked by composite score. The query joins the latest `benchmark_metrics` row per submission (using `SELECT MAX(id) GROUP BY submission_id`) and computes the composite score in Go rather than SQL.
- `GET /api/scores/:id` — Returns all historical runs for a specific submission ID, with per-run score computation. Enables the drill-down view.
- `GET /api/leaderboard` — Legacy alias returning a simplified leaderboard format for backward compatibility.
- `GET /health` — Health check returning uptime in seconds.

CORS headers are set to `Access-Control-Allow-Origin: *` to allow the Next.js frontend (running on port 3000) to make cross-origin requests. The `Cache-Control: no-store` header prevents caching of leaderboard data.

The leaderboard also estimates TPS as a proxy metric: `tps = min(totalOrders × 1e6 / p99Micros, 1_000_000)` — the theoretical maximum throughput if every order took p99 latency, capped at 1M ops/sec.

### 11.2 Next.js Frontend Architecture

The leaderboard UI is a Next.js 14 application (`services/leaderboard-ui/`) built with TypeScript and Tailwind CSS. The application structure follows the App Router pattern:

- `src/app/page.tsx` — Main page component that orchestrates the dashboard layout.
- `src/components/Header.tsx` — Top navigation bar with last-updated timestamp and active benchmark count.
- `src/components/StatsStrip.tsx` — Four-card summary strip showing total submissions, best p99, average correctness, and active VMs.
- `src/components/Podium.tsx` — Top-3 visualization with gold/silver/bronze styling.
- `src/components/LeaderboardTable.tsx` — Full-width sortable table with all submissions.
- `src/components/SubmissionDrawer.tsx` — Side panel showing per-submission run history when a row is clicked.
- `src/components/EmptyState.tsx` — Placeholder shown when no benchmark data exists.
- `src/lib/useLeaderboard.ts` — Custom React hook encapsulating the polling logic and rank change detection.

### 11.3 Rank Change Detection & Animation Protocol

The `useLeaderboard` hook polls `/api/scores` at the interval specified (1500ms) and compares the current rank of each submission against the previous poll's rank. Rank changes (up or down) are tracked in a `rankChanges` map passed to the `LeaderboardTable` component, which renders visual indicators (arrows, color changes) for entries whose rank changed between polls.

### 11.4 Podium, Stats Strip & Drill-Down Drawer

The **Podium** component renders the top 3 submissions with their scores and a visual hierarchy (gold/silver/bronze). Clicking a podium card opens the drill-down drawer.

The **Stats Strip** computes aggregate metrics from the scores array: total submission count, minimum non-zero p99 across all submissions (best p99), average correctness rate, and active benchmark count from the API response.

The **Submission Drawer** is a side panel that, when a submission is selected, fetches `GET /api/scores/:id` and displays the submission's run history — each run's p50/p90/p99, success rate, score, TPS estimate, and timestamp.

### 11.5 Polling Strategy & Update Frequency

The frontend polls every 1500ms (`useLeaderboard(1500)`). This is a deliberate trade-off: faster polling (e.g., 500ms) would increase load on the leaderboard API and TimescaleDB, while slower polling (e.g., 5s) would make the dashboard feel stale during active benchmarking. 1500ms provides a visually responsive update cadence without overloading the backend.

The polling approach was chosen over WebSockets because the leaderboard data is updated infrequently (only when a benchmark completes, which takes 3–12 seconds end-to-end). WebSocket push would provide marginally faster updates but adds connection management complexity for no meaningful user experience improvement at this update frequency.

---

## 12. Hardware Sympathy & Host Tuning

### 12.1 CPU Isolation Philosophy

The tuning philosophy is layered defense-in-depth:

1. **Docker `cpuset`** pins containerized services to specific CPU cores — a coarse-grained constraint that prevents the kernel from scheduling Docker workloads on protected cores.
2. **`taskset`** pins non-containerized processes (orchestrator, Firecracker VMMs) to specific cores — process-level affinity.
3. **IRQ masking** (`/proc/irq/*/smp_affinity`) steers hardware interrupts away from trading cores — prevents interrupt-driven preemption.
4. **`isolcpus` kernel parameter** (recommended, not enforced at runtime) removes trading cores from the kernel scheduler's runqueue entirely — no kernel tasks, no timer ticks, no workqueue threads on isolated cores.

Each layer addresses a different class of interference. Without all four, there are gaps: `cpuset` alone doesn't prevent kernel threads; `taskset` alone doesn't prevent IRQs; IRQ masking alone doesn't prevent scheduler decisions; `isolcpus` alone doesn't prevent Docker containers.

### 12.2 IRQ Affinity — Protecting Benchmark Cores

The `tune_system.sh` script writes `$IRQ_ALLOWED_MASK` (`0xF03`, corresponding to cores 0,1,8,9,10,11) to `/proc/irq/default_smp_affinity` and iterates over every `/proc/irq/[0-9]*/smp_affinity` to steer existing IRQs. This ensures that NIC interrupts, disk I/O completions, timer interrupts, and all other hardware events are delivered exclusively to non-trading cores.

On the 12-thread topology, this means cores 2–7 (Firecracker vCPUs, bot-fleet workers, shadow-engine) receive zero hardware interrupts. The only remaining interrupts on these cores are NMIs (non-maskable interrupts from the watchdog) — which are addressed by restricting the watchdog to cores 0,1,10,11 via `/proc/sys/kernel/watchdog_cpumask`.

### 12.3 Hugepage Pre-allocation for Firecracker Guests

The tuning script enables Transparent Hugepages (`echo "always" > /sys/kernel/mm/transparent_hugepage/enabled`) and pre-allocates 512 × 2 MiB hugepages (1 GiB total) via `/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages`. Firecracker maps guest memory using `mmap()`, and THP allows the kernel to back these mappings with 2 MiB pages instead of 4 KiB pages, reducing TLB misses by up to 512x for sequential memory access patterns within the guest.

The `khugepaged/alloc_sleep_millisecs` is set to 0, making the khugepaged kernel thread collapse 4 KiB pages into 2 MiB pages as aggressively as possible — reducing the window during which the guest operates on small pages after boot.

### 12.4 Kernel Network Stack Tuning

The script applies the following sysctl settings:

- `net.core.busy_read=50` / `net.core.busy_poll=50` — Enables busy-polling on sockets for 50µs before falling back to the interrupt-driven path. This eliminates the ~5µs wake-from-idle latency when a packet arrives on a socket that was blocking in `epoll_wait()`.
- `net.core.somaxconn=65535` — Increases the listen backlog to prevent SYN drops during concurrent VM connection establishment.
- `net.core.netdev_max_backlog=250000` — Increases the per-CPU backlog queue for incoming packets, preventing drops during burst traffic from multiple VMs.
- `net.ipv4.tcp_slow_start_after_idle=0` — Prevents TCP congestion window collapse between order bursts. Without this, the kernel resets `cwnd` to the initial window after an idle period, causing the first few orders in each burst to be rate-limited.

RPS (Receive Packet Steering) is configured to route packet processing to non-trading cores (mask `0xc03`, cores 0,1,10,11), preventing softirq processing from interrupting the bot-fleet or shadow-engine threads.

### 12.5 Scheduler & Memory Subsystem Configuration

- `sched_migration_cost_ns=5000000` (5ms) — Increases the cost threshold for migrating a task between CPUs, reducing unnecessary cross-core migrations that invalidate L1/L2 cache.
- `sched_autogroup_enabled=0` — Disables CFS autogroup, which can cause unpredictable latency spikes when the scheduler rebalances across autogroups.
- `sched_min_granularity_ns=10000000` (10ms) — Increases the minimum time slice, reducing preemption frequency on shared cores.
- `sched_wakeup_granularity_ns=15000000` (15ms) — Increases the granularity at which a waking task can preempt a running task.
- `vm.swappiness=0` — Prevents the kernel from paging out Firecracker guest memory to swap, which would cause catastrophic latency spikes when the guest accesses swapped-out pages.
- C-state disable on trading cores — Writes `1` to `/sys/devices/system/cpu/cpu<N>/cpuidle/state2+/disable` to prevent deep idle states (C3+) that cause 50–200µs wake latency.

---

## 13. Security Model

### 13.1 Threat Model: Adversarial Anonymous Binary Execution

The platform executes compiled binaries uploaded by anonymous users. The threat model assumes the contestant is adversarial — the binary may attempt to:

- Escape the VM to access host resources.
- Exhaust CPU, memory, disk, or network resources to deny service to other contestants.
- Read other contestants' submissions or results.
- Manipulate the measurement infrastructure to inflate their own score.
- Persist state across benchmark runs to detect and adapt to the order stream.

### 13.2 Firecracker Security Boundary

Firecracker provides the primary security boundary via KVM hardware virtualization. The guest runs in VMX non-root mode (Intel) or SVM guest mode (AMD), with EPT/NPT enforcing memory isolation. The Firecracker VMM process runs as a regular user-space process with minimal capabilities — it does not require root on the host (though the orchestrator requires root for TAP interface creation).

Firecracker's device model is deliberately minimal: no PCI, no USB, no GPU, no IDE — only virtio-net, virtio-block, virtio-vsock, and a serial console. Each excluded device is an eliminated attack surface vector.

### 13.3 CVE-2026-31431 Mitigation Analysis

A kernel vulnerability in the guest kernel's syscall interface (the hypothetical CVE-2026-31431) affects only the guest kernel. The host kernel is separate. Exploitation of such a CVE would grant the attacker control over the guest kernel — but the guest kernel is already controlled by the contestant (they can execute arbitrary code). The security boundary that matters is between the guest kernel and the host, which is enforced by KVM, not by kernel hardening.

### 13.4 Denial-of-Wallet Attack Vector & Circuit Breaker

A malicious contestant could upload a binary that consumes maximum CPU and memory for the duration of its execution, preventing other submissions from being benchmarked. The system mitigates this through:

- **Cgroup CPU limit** (`cpu.max = "100000 100000"`): The VM gets exactly 1 CPU core and cannot exceed it.
- **Cgroup memory limit** (`memory.max = 256 MiB`): The VM is OOM-killed if it exceeds 256 MiB.
- **Boot timeout** (20s): The orchestrator cancels VM boot if it exceeds 20 seconds.
- **Benchmark timeout**: The submission API's `RequestTimeout` (90s) caps total benchmark duration.
- **Concurrent benchmark limit**: The bot fleet's semaphore limits concurrent benchmarks to 8.
- **TC rate limiting** (100 Mbit/s per TAP): Prevents network bandwidth exhaustion.

The remaining attack vector: a contestant binary that runs within all resource limits but produces degenerate behavior (e.g., accepting connections but never responding), which would cause the bot fleet workers to hang on `read()` until the `SOCKET_TIMEOUT` (3 seconds) expires for each order. At 1000 orders, this could consume up to 50 minutes of bot fleet time. A production circuit breaker would impose a per-order latency ceiling (e.g., kill the benchmark if any single order exceeds 10 seconds) — this is not yet implemented.

### 13.5 Network Egress Isolation Per Submission

Each VM receives a dedicated TAP interface attached to the `br0` bridge. Traffic control (`tc`) limits bandwidth to 100 Mbit/s in both directions. The `iptables` NAT masquerade rule provides outbound connectivity (for contestant binaries that need to download dependencies), but this is a security concern — a malicious binary could use the outbound connectivity for data exfiltration or to participate in DDoS attacks.

A production deployment would drop the NAT rule and block all outbound traffic from VMs — contestant binaries should be fully self-contained with no network dependencies.

### 13.6 Residual Attack Surface

1. **KVM escape**: A zero-day in KVM's instruction emulation or virtio device model could allow a guest to execute code on the host. Mitigation: Firecracker runs with a minimal device model, reducing the attack surface compared to QEMU. Defense-in-depth: run Firecracker inside a jailer process with seccomp filters (not yet configured in this deployment).
2. **Side-channel attacks**: A contestant could use cache timing, branch prediction, or speculative execution side channels to observe other VMs running on the same physical core's SMT sibling. Mitigation: `mitigations=off` is in the GRUB params (for performance), which *disables* spectre/meltdown mitigations and *increases* this risk. A production deployment would need to evaluate the trade-off between speculative execution mitigations and measurement overhead.
3. **VSOCK information leak**: The VSOCK Unix domain socket path (`/tmp/vsock-<id>.sock`) is predictable. A contestant binary running inside the VM cannot access host filesystem paths, but a compromised orchestrator could be manipulated to expose socket paths of other VMs. Mitigation: each VM's VSOCK socket is removed during teardown.
## 14. Inter-Service Communication

### 14.1 gRPC — Control Plane Protocol

Two gRPC services define the control plane:

**VMController** (`orchestrator.proto`, port 50051):
- `SpawnVM(SpawnRequest) → SpawnResponse` — Creates a Firecracker microVM with the specified vCPU count, memory, and submission ID. Returns the VM ID (prefixed with `vm-`), allocated IP address, and VSOCK socket path.
- `TeardownVM(TeardownRequest) → TeardownResponse` — Stops the VMM process, deletes the TAP interface, and removes the submission disk image.

**BenchmarkController** (`benchmark.proto`, port 50052):
- `StartBenchmark(BenchmarkRequest) → BenchmarkResponse` — Executes the full benchmark run: connects to the contestant engine, generates and dispatches orders, validates against the shadow engine, computes percentiles, publishes telemetry, and returns aggregate results.

**BenchmarkWorker** (`benchmark.proto`, port 50053):
- `StartBenchmark(StartRequest) → StartResponse` — Resets the shadow engine's order book for a submission.
- `MatchOrder(MatchOrderRequest) → MatchOrderResponse` — Submits an order to the shadow order book and returns resulting trades. The `submission-id` is passed via gRPC metadata header.
- `CancelOrder(CancelOrderRequest) → CancelOrderResponse` — Cancels a resting order in the shadow book.

All gRPC connections use `insecure.NewCredentials()` — no TLS. This is acceptable for a single-host deployment where all traffic traverses localhost. A multi-node deployment would require mTLS.

### 14.2 Redpanda — Data Plane Messaging

The data plane uses Redpanda's Kafka-compatible API on port 9092. The bot fleet produces to the `benchmark-results` topic using the `rdkafka` crate (librdkafka bindings). The telemetry ingester consumes using the `franz-go` library with consumer group `telemetry-ingester-group`.

The choice of different Kafka client libraries across services is pragmatic: `rdkafka` is the mature Rust Kafka client with production-proven performance characteristics. `franz-go` is a pure-Go client that avoids CGO dependencies in the telemetry ingester's build, simplifying the Docker image.

Message flow is unidirectional: bot-fleet → Redpanda → telemetry-ingester → TimescaleDB. There are no consumer acknowledgments back to the bot fleet — the telemetry pipeline is fire-and-forget from the producer's perspective.

### 14.3 REST — Submission Ingestion API

The submission API exposes three HTTP endpoints on port 8080:

- `POST /submit` — Multipart form upload with `submission_id` and `binary` fields. Returns `202 Accepted` with the submission ID and `PENDING` status. The actual benchmark is executed asynchronously by the worker pool.
- `GET /status?submission_id=<id>` — Returns the current status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`), score, p99, and success rate from the `benchmark_jobs` table.
- `GET /health` — Returns `{"status": "ok"}`.

The HTTP server is configured with `ReadTimeout: 15s`, `WriteTimeout: 120s` (high to accommodate long-polling scenarios), and `IdleTimeout: 60s`. Middleware layers provide panic recovery (`withRecovery`) and request logging (`withLogging`).

### 14.4 SQLite — Shared Read / Single Writer Pattern

The legacy SQLite path (preserved in the codebase as `DBFile = "../telemetry-ingester/telemetry.db"` constants and a `telemetry.db` file) used SQLite's default journal mode, which provides a single-writer / multiple-reader concurrency model. The telemetry ingester was the sole writer; the submission API and leaderboard were readers. This worked for serial benchmark execution but became a bottleneck when the worker pool introduced concurrent writes.

The migration to TimescaleDB (PostgreSQL) eliminated this constraint. PostgreSQL's MVCC provides true concurrent read/write access without write serialization, and the `FOR UPDATE SKIP LOCKED` pattern in the worker pool enables safe concurrent job claiming.

---

## 15. Infrastructure as Code

### 15.1 Docker Compose Service Graph & Dependency Order

The `docker-compose.yml` defines seven services with the following dependency graph:

```
timescaledb (health: pg_isready)
    ├── telemetry-ingester (depends_on: timescaledb healthy)
    ├── submission-api (depends_on: bot-fleet started, timescaledb healthy)
    └── leaderboard (depends_on: timescaledb healthy)

redpanda (health: rpk cluster health)
    └── bot-fleet (depends_on: redpanda healthy)

leaderboard
    └── leaderboard-ui (depends_on: leaderboard)

shadow-engine (no dependencies)
```

Boot order: TimescaleDB and Redpanda start first (health checks ensure readiness). Shadow engine starts independently. Bot fleet starts after Redpanda is healthy. Telemetry ingester starts after TimescaleDB is healthy. Submission API starts after both bot fleet and TimescaleDB. Leaderboard starts after TimescaleDB. Leaderboard UI starts after leaderboard.

All services use `network_mode: "host"` — no Docker bridge networking. This eliminates Docker's userland proxy overhead (which adds ~100µs per packet on the NAT path) and allows services to bind directly to localhost ports.

All services use `restart: unless-stopped` for automatic recovery from crashes.

### 15.2 Hardware Tuning Bootstrap (run-orchestrator.sh)

The `run-orchestrator.sh` script is the entry point for the entire system on bare metal. It must be run as root (`sudo`). It performs six steps in sequence:

1. **Source `cpu_layout.env`** — Loads the CPU core allocation map.
2. **Apply `tune_system.sh`** — Executes the hardware sympathy tuning (IRQ masking, governor, C-states, hugepages, network stack, scheduler).
3. **Create `br0` bridge** — Sets up the bridge at `172.16.0.1/24` for VM networking.
4. **Enable IP forwarding** — `sysctl net.ipv4.ip_forward=1` + iptables NAT masquerade.
5. **Compile the orchestrator** — `CGO_ENABLED=1 go build -o orchestrator-bin -ldflags "-s -w" main.go disk.go network.go firecracker.go`. CGO is required for the Firecracker Go SDK's dependencies.
6. **Start the orchestrator** — Pinned to `$ORCH_CORES` via `taskset -c`. The `exec` replaces the shell process with the orchestrator, ensuring signal delivery works correctly.

The script also compiles the reference strategy binary (`gcc -O2 -static -o strategy_bin strategy_bin.c`) from the C source if available.

### 15.3 Firecracker Orchestrator — Why It Runs Outside Docker

The sandbox orchestrator cannot run inside Docker because it requires:

- **CAP_NET_ADMIN** for creating TAP interfaces (`ip tuntap add`).
- **Access to `/dev/kvm`** for Firecracker's KVM-based virtualization.
- **Loop device mounting** (`mount -o loop`) for creating submission disk images.
- **Cgroup v2 manipulation** for applying resource limits to Firecracker processes.
- **Direct process management** for `taskset` CPU pinning of Firecracker PIDs.

While Docker can expose `/dev/kvm` and grant `CAP_NET_ADMIN` via `--privileged`, running a privileged container that manages other containers/VMs creates a nested isolation problem where Docker's security boundary becomes meaningless. Running the orchestrator directly on the host with `sudo` provides a clearer security model: the orchestrator is a trusted component with root access, and the untrusted code runs inside Firecracker VMs that the orchestrator manages.

---

## 16. Failure Mode Registry

### 16.1 Failure Analysis Methodology

Each failure mode was identified by tracing the code path and asking: "What happens when this component returns an error, times out, or crashes?" The detection latency is the time between the failure occurring and the system becoming aware of it. Recovery behavior describes what happens automatically without human intervention.

### 16.2 Complete Failure Mode Table

| Failure | Trigger Condition | Detection Latency | Data Lost | Recovery Behavior | Prevention |
|---|---|---|---|---|---|
| Firecracker boot timeout | Kernel panic, missing rootfs, resource exhaustion | 20s (`bootTimeout`) | None — benchmark not started | SpawnVM returns error; submission marked FAILED; TAP + disk cleaned up | Validate rootfs/kernel before deploy |
| VSOCK handshake failure | Contestant binary not listening, VSOCK socket not created | 100ms retry × N | None — benchmark not started | Worker retries with 50ms backoff indefinitely until connection or parent timeout | 100ms settle delay before benchmark start |
| Contestant engine crash mid-benchmark | Segfault, OOM, unhandled exception | 3s (`SOCKET_TIMEOUT` per order) | Partial telemetry for completed orders | read() returns 0 or error; order counted as failed; benchmark continues with remaining orders | Cgroup memory limit prevents OOM |
| Shadow engine unreachable | Process crash, port conflict | Immediate (gRPC connect fails) | No correctness validation | Bot fleet logs warning, sets `shadow_client = None`; all orders count as failed (no validation possible) | Docker restart policy; health checks |
| Redpanda crash | OOM, disk full | 5s (`message.timeout.ms`) | Uncommitted messages in ring buffer | Bot fleet produce returns error; telemetry lost for this run; submission API falls back to BenchmarkResponse values | Volume mount for persistence; memory limits |
| TimescaleDB crash | Disk full, OOM, connection limit | 0.5-2s (next query fails) | Unflushed ingester batch (up to 500 records) | Ingester: batch INSERT fails, records dropped, offsets not committed (re-fetched on restart). Submission API: job status update fails, job stuck in RUNNING | PostgreSQL WAL ensures committed data survives |
| Telemetry ingester crash | Panic, OOM | 2s (`FlushInterval`) | Current batch (up to 500 records) | Docker restart; consumer re-joins group; re-fetches from last committed offset | Bounded channel prevents memory growth |
| TAP interface leak | Orchestrator crash during VM lifecycle | Until next orchestrator restart | None | `shutdownAllVMs()` cleans up on restart; `interfaceExists()` check prevents duplicates | Defer-based cleanup in SpawnVM error paths |
| Submission disk leak | Orchestrator crash after image creation but before VM registration | Until manual cleanup | None — orphaned /tmp files | No automatic recovery; `/tmp` cleaned on reboot | `defer os.RemoveAll(workspaceDir)` in CreateSubmissionImage |
| Worker pool starvation | All workers blocked on slow benchmarks | 1s polling interval | None — jobs accumulate in PENDING | New jobs queue in PostgreSQL; processed when workers free up | Increase `concurrency` parameter; add worker pool scaling |
| IP address exhaustion | 253 concurrent VMs (172.16.0.2–254) | Immediate | None | `findFreeIPIndex()` returns error; SpawnVM fails | Theoretical limit; practical limit is ~8 concurrent benchmarks |
| Cgroup creation failure | Missing cgroup v2 support, permission denied | Immediate (logged as WARNING) | None | Non-fatal; VM runs without resource limits | Verify cgroup v2 mount before deploy |
| Kafka offset commit failure | Redpanda unreachable during commit | Immediate (logged) | None — data already persisted | Records re-delivered on next consumer restart (duplicates) | At-least-once semantics by design |

### 16.3 Tarpit Engine — Attack Execution & Circuit Breaker Response

A tarpit attack occurs when a contestant binary accepts connections but deliberately delays responses to consume bot fleet worker time. With `SOCKET_TIMEOUT = 3s` per order and 1000 orders, a single tarpit submission can hold a bot fleet worker for up to 3000 seconds (~50 minutes).

The current system's defense is the `SOCKET_TIMEOUT` — if a read doesn't complete within 3 seconds, the order is marked as failed and the worker moves to the next order. This bounds the worst case to 3000 seconds per benchmark, which is long but finite.

A production circuit breaker would:
- Track per-submission failure rate in real-time during the benchmark.
- Abort the benchmark if the failure rate exceeds a threshold (e.g., 50% of orders timeout) within the first 100 orders.
- Blacklist the submission ID to prevent re-submission.

This is not yet implemented.

### 16.4 Cascade Failure Chain Analysis

The most dangerous cascade failure path:

1. **TimescaleDB fills disk** → All INSERTs fail.
2. **Telemetry ingester batch fails** → Records dropped, offsets not committed.
3. **Submission API `pollBenchmarkMetrics()` query fails** → Falls back to BenchmarkResponse values (degraded but functional).
4. **Submission API `UPDATE benchmark_jobs` fails** → Job stuck in `RUNNING` state permanently.
5. **Worker never picks up new jobs** (the stuck job holds no lock, but if all jobs are stuck, the `PENDING` queue appears empty).

Mitigation: The `FOR UPDATE SKIP LOCKED` pattern in the worker pool means stuck `RUNNING` jobs don't block other `PENDING` jobs from being claimed. However, stuck jobs are never retried — a production system needs a reaper goroutine that marks `RUNNING` jobs older than a threshold (e.g., 5 minutes) as `FAILED`.
## 17. Architecture Decision Records

### ADR-001: Firecracker over Docker & gVisor

**Status:** Accepted.

**Context:** The platform executes untrusted, anonymous binaries that could exploit kernel vulnerabilities, exhaust host resources, or interfere with concurrent submissions. The isolation technology must provide a hardware-enforced security boundary without introducing measurement overhead that corrupts the benchmarking results.

**Decision:** We chose Firecracker microVMs as the isolation layer.

**Alternatives Considered:**
- **Docker (runc):** Shared-kernel isolation. A single kernel CVE grants container escape. Eliminated by Constraint 1 (Section 2.2). Measurement overhead is low (~0µs added), but the security boundary is insufficient.
- **gVisor (runsc):** User-space kernel. Provides a stronger security boundary than Docker but adds 10–30% overhead to every syscall. Eliminated by Constraint 2 — the measurement infrastructure would corrupt the measurement.
- **Kata Containers:** KVM-based, similar to Firecracker but with a larger device model (QEMU-based). Larger attack surface and slower boot times (~500ms vs ~125ms for Firecracker). Rejected for the larger attack surface.

**Consequences:** Firecracker adds ~125ms boot latency per submission, ~5–20µs per-order VSOCK traversal overhead, and operational complexity (requires root, KVM access, TAP interface management). The orchestrator must run outside Docker. The boot latency is amortized across the entire benchmark run and does not affect per-order measurement.

**Production Evolution Path:** With 6 months of engineering time, we would add Firecracker's jailer for process-level sandboxing of the VMM itself, implement snapshot/restore for sub-10ms VM provisioning (eliminating boot latency entirely), and evaluate Cloud Hypervisor as an alternative VMM with a smaller TCB.

### ADR-002: Rust for Hot Path Services

**Status:** Accepted.

**Context:** The bot fleet and shadow engine are on the measurement hot path — any latency added by these services directly affects the benchmark results. The bot fleet must sustain high connection concurrency with low per-order overhead. The shadow engine must match orders at a rate that keeps pace with the bot fleet's dispatch rate.

**Decision:** We chose Rust with `tokio` for the bot fleet and the shadow engine.

**Alternatives Considered:**
- **Go:** Goroutine scheduling introduces 1–10µs of overhead per context switch. The Go GC introduces stop-the-world pauses of 50–500µs that would appear as latency spikes in the bot fleet's per-order measurements. Eliminated for the GC pauses.
- **C++:** Equivalent performance to Rust but without memory safety guarantees. Given that the bot fleet and shadow engine process untrusted input (contestant responses, gRPC messages), memory safety is a security requirement.
- **Java/JVM:** GC pauses of 1–50ms (G1/ZGC) are unacceptable for a microsecond-precision measurement tool.

**Consequences:** Rust's borrow checker adds development friction. The async/await model with `tokio` requires careful reasoning about `Send + Sync` bounds. Compilation times are ~30s for incremental builds.

**Production Evolution Path:** Replace `tokio::sync::Mutex`-guarded MPSC channels with lock-free `crossbeam` channels for the bot fleet's work distribution. Use `io_uring` via `tokio-uring` for kernel-bypass I/O on the VSOCK path.

### ADR-003: Redpanda over Apache Kafka

**Status:** Accepted.

**Context:** The telemetry pipeline needs a message broker between the bot fleet (Rust) and the telemetry ingester (Go). The broker runs on the same host as all other services, competing for CPU and memory resources.

**Decision:** We chose Redpanda v23.2.1.

**Alternatives Considered:**
- **Apache Kafka:** Requires the JVM, which on a 6-core host with 16 GiB RAM would consume 1–4 GiB of heap and introduce GC pauses that affect co-resident services. The JVM's memory footprint is disproportionate for a single-topic, single-partition workload.
- **NATS JetStream:** Lightweight, Go-based. Lacks the Kafka-compatible API that allows us to use `rdkafka` (Rust) and `franz-go` (Go) — two of the most battle-tested Kafka client libraries in their respective ecosystems. Would require custom client integration.
- **Direct gRPC streaming:** Eliminates the broker entirely but couples the bot fleet to the telemetry ingester's availability — if the ingester is down, telemetry is lost. The broker provides temporal decoupling.

**Consequences:** Redpanda in dev-container mode does not replicate data. A Redpanda crash loses uncommitted messages. This is acceptable for telemetry data that can be regenerated by re-running the benchmark.

**Production Evolution Path:** Deploy Redpanda in production mode with 3-node Raft replication for durability. Enable TLS between clients and brokers. Use Redpanda's built-in schema registry for Avro-encoded messages instead of JSON.

### ADR-004: Software Timestamps with Quantified Uncertainty

**Status:** Accepted.

**Context:** The bot fleet measures per-order latency using `Instant::now()` (Rust), which calls `clock_gettime(CLOCK_MONOTONIC)`, which on x86_64 reads the TSC register via the vDSO. This provides ~25–100ns resolution but is subject to scheduling jitter.

**Decision:** We chose software timestamps with explicit uncertainty quantification rather than claiming false precision.

**Alternatives Considered:**
- **Hardware PTP timestamps:** NIC-level timestamps at the PHY layer provide sub-microsecond accuracy. Requires a PTP-capable NIC (e.g., Intel X710/XXV710), a PTP grandmaster clock, and kernel support (`SO_TIMESTAMPING`). Not available on the current hardware.
- **XDP/eBPF timestamps:** Capture timestamps in the kernel's XDP hook, before the packet reaches userspace. Provides ~1µs accuracy. Requires custom XDP programs and does not work with VSOCK (which is not a network device).

**Consequences:** The measurement uncertainty floor is ±10–50µs per order. Two submissions with p99 values within this band cannot be meaningfully distinguished. This is documented honestly in Section 6.

**Production Evolution Path:** Deploy on hardware with Intel X710 NICs, configure PTP with a grandmaster clock (e.g., Timebeat or Meinberg), use `SO_TIMESTAMPING` with `SOF_TIMESTAMPING_TX_HARDWARE | SOF_TIMESTAMPING_RX_HARDWARE` for per-packet hardware timestamps. This reduces the measurement floor to ±100ns.

### ADR-005: TAP/Bridge Networking over SR-IOV VFs

**Status:** Accepted.

**Context:** Each Firecracker VM needs a network interface. The two options are TAP devices attached to a bridge or SR-IOV Virtual Functions (VFs) passed through to the VM.

**Decision:** We chose TAP devices attached to a Linux bridge (`br0`).

**Alternatives Considered:**
- **SR-IOV VFs:** Each VF is a hardware NIC partition passed directly to the VM via VFIO. Provides near-native network performance (kernel bypass in the guest). Requires an SR-IOV-capable NIC (e.g., Intel X710), IOMMU support, and limits the number of concurrent VMs to the NIC's VF count (typically 64–128). More complex to manage and not available on the development hardware.
- **macvtap:** A hybrid that creates virtual interfaces directly from the physical NIC. Simpler than SR-IOV but doesn't provide the same level of isolation — all macvtap interfaces share the physical NIC's bandwidth without hardware-level QoS.

**Consequences:** TAP interfaces add kernel-stack latency (~5–20µs per packet traversal) compared to SR-IOV's near-zero overhead. The `tc` rate limiting is applied in software, which is less precise than SR-IOV's hardware QoS. For the current measurement uncertainty band (±10–50µs), the TAP overhead is within the noise floor.

**Production Evolution Path:** On bare metal with Intel X710, deploy SR-IOV VFs with VFIO passthrough. This eliminates the host kernel's network stack from the data path entirely, providing true hardware-level network isolation and sub-microsecond network latency.

### ADR-006: SQLite over TimescaleDB

**Status:** Superseded — migrated to TimescaleDB.

**Context:** Benchmark telemetry data needs persistent storage for leaderboard queries. The original design used SQLite for its zero-configuration deployment model.

**Decision:** We initially chose SQLite, then migrated to TimescaleDB (PostgreSQL + TimescaleDB extension).

**Alternatives Considered:**
- **SQLite:** Zero configuration, embedded, single-file database. Works well for single-writer workloads. Failed under concurrent writes from the worker pool + telemetry ingester.
- **PostgreSQL (vanilla):** Full MVCC, concurrent access. The `benchmark_metrics` table grows unboundedly with time-series data, making range queries slower as data accumulates.
- **ClickHouse:** Column-oriented, excellent for analytical queries. Overkill for the data volume (thousands of rows, not millions). Adds significant operational complexity.

**Consequences:** TimescaleDB adds a PostgreSQL dependency (managed via Docker) but provides automatic time-based partitioning (`create_hypertable`) that keeps query performance constant as data grows. The `FOR UPDATE SKIP LOCKED` pattern for job claiming is PostgreSQL-specific and would need replacement on other databases.

**Production Evolution Path:** Enable continuous aggregates in TimescaleDB for pre-computed leaderboard views, reducing query latency from ~20ms to ~1ms for the main leaderboard endpoint.

### ADR-007: Shadow LOB via gRPC

**Status:** Accepted.

**Context:** The shadow engine must process orders in parallel with the contestant engine to validate correctness. It must be accessible from the bot fleet, which runs in a Docker container.

**Decision:** We chose to expose the shadow engine as a gRPC service (`BenchmarkWorker`) running as a separate process.

**Alternatives Considered:**
- **In-process shadow engine:** Embed the matching engine directly in the bot fleet's Rust binary. Eliminates the gRPC overhead (~50–200µs per call) but couples the shadow engine's lifecycle to the bot fleet's. A shadow engine crash would kill the bot fleet and all active benchmarks.
- **Shared memory:** Map the order book into shared memory between the bot fleet and shadow engine. Eliminates serialization overhead but creates a tight coupling between the two processes' memory layouts and requires careful synchronization.

**Consequences:** The gRPC call adds ~50–200µs per order to the benchmark's total execution time (but not to the measured contestant latency, since the shadow call happens after the contestant response is received). The separate process provides fault isolation — a shadow engine crash (e.g., from a malformed gRPC message) does not kill the bot fleet.

**Production Evolution Path:** Implement the shadow engine as an in-process library for the hot path, with a separate gRPC shadow engine for cross-language compatibility. Use a persistent gRPC stream (server-side streaming) instead of unary RPCs to amortize connection overhead.

### ADR-008: Go for Control Plane Services

**Status:** Accepted.

**Context:** The submission API, sandbox orchestrator, telemetry ingester, and leaderboard are control plane services that do not appear on the measurement hot path. They perform I/O-bound work (HTTP serving, gRPC calls, database queries, file system operations).

**Decision:** We chose Go for all control plane services.

**Alternatives Considered:**
- **Rust:** Would provide lower latency for gRPC calls, but the development velocity trade-off is not justified for services that are not on the measurement path. Go's standard library provides production-ready HTTP servers, gRPC clients, and database drivers.
- **Python/Node.js:** Would provide faster development velocity but introduce runtime overhead (GIL, event loop) that could affect the orchestrator's ability to manage Firecracker VMs with low-latency gRPC responses.

**Consequences:** Go's GC can introduce ~100µs pauses in the orchestrator and submission API, but these pauses do not affect the benchmark measurement — they only affect the orchestrator's response time to SpawnVM/TeardownVM calls, which is not latency-critical.

**Production Evolution Path:** No change planned. Go is the correct choice for control plane services.

### ADR-009: Persistent TCP Connections in Bot Fleet

**Status:** Accepted.

**Context:** The bot fleet sends 1000+ orders to the contestant engine during each benchmark run. Each order is a ~50–100 byte JSON payload followed by a ~50–500 byte JSON response.

**Decision:** We chose to establish a single persistent TCP/VSOCK connection per worker at the start of each benchmark and reuse it for all orders.

**Alternatives Considered:**
- **Per-order connection:** Open a new TCP connection for each order. The three-way handshake adds ~34ms on loopback under load (SYN-ACK processing + kernel connection establishment). At 1000 orders, this adds ~34 seconds of pure handshake overhead — dwarfing the actual order processing time.
- **HTTP/2 multiplexing:** Use gRPC (which uses HTTP/2) for the contestant protocol. Adds HTTP/2 framing overhead and requires the contestant to implement a gRPC server — a significantly higher barrier to entry than a newline-delimited JSON TCP server.

**Consequences:** Persistent connections require the bot fleet to handle connection drops mid-benchmark (the contestant engine crashes). Currently, a broken connection causes all subsequent orders on that worker to fail, which is counted against the contestant's correctness rate. A production system would implement reconnection logic.

**Production Evolution Path:** Implement connection pooling with health checking and automatic reconnection. Add support for HTTP/2 as an optional contestant protocol for contestants who prefer gRPC.

### ADR-010: Docker Compose over Kubernetes

**Status:** Accepted.

**Context:** The system runs on a single host. All services communicate over localhost. The deployment target is a bare-metal machine, not a cloud environment.

**Decision:** We chose Docker Compose for service orchestration.

**Alternatives Considered:**
- **Kubernetes (k8s):** Adds a control plane (etcd, kube-apiserver, kube-scheduler) that consumes 1–2 CPU cores and 2–4 GiB RAM on the single host. Pod scheduling introduces 2–10ms latency. Network policies add iptables overhead. The operational complexity is disproportionate for a single-node deployment.
- **systemd units:** Lower overhead than Docker but lacks health checks, restart policies, and dependency ordering. Would require manual management of container images, volumes, and logging.
- **Podman Compose:** API-compatible with Docker Compose but runs containers rootless (without a daemon). The orchestrator's requirement for root access makes rootless containers irrelevant for the most privileged component.

**Consequences:** Docker Compose does not support multi-node scaling. The entire system runs on a single machine, which caps the number of concurrent benchmarks. The `network_mode: "host"` eliminates Docker's network namespace overhead but prevents port conflict detection.

**Production Evolution Path:** For multi-node deployment, migrate to Kubernetes with Firecracker running via the Kata Containers runtime class (or a custom CRI implementation). Use Kubernetes node affinity to pin latency-critical pods to specific nodes with `isolcpus` configured.

---

## 18. Performance Characteristics

### 18.1 Firecracker Boot Time Breakdown

| Phase | Duration | Notes |
|---|---|---|
| TAP interface creation + tc setup | 10–50ms | `ip tuntap add` + `ip link set` + `tc qdisc add` (3 shell commands) |
| Submission disk creation (truncate + mkfs.ext4 + mount + copy + umount) | 100–500ms | Dominated by `mkfs.ext4` formatting |
| Firecracker process start + KVM init | 50–100ms | VMM process creation, KVM VM/vCPU setup |
| Guest kernel boot (vmlinux.bin) | 50–200ms | Kernel decompression, init, driver probing |
| CPU pinning (taskset) | 5–20ms | PID lookup + per-thread taskset calls |
| Cgroup v2 setup | 2–5ms | mkdir + 3 file writes |
| **Total SpawnVM latency** | **~250–900ms** | Varies with host load and disk I/O |

### 18.2 End-to-End Submission Pipeline Latency

| Phase | Duration |
|---|---|
| Binary upload + save to disk | 5–50ms |
| INSERT benchmark_jobs | 0.5–2ms |
| Worker poll delay (avg) | ~500ms |
| SpawnVM | 250–900ms |
| Settle delay | 100ms (fixed) |
| VSOCK connection + handshake | 1–50ms |
| Benchmark execution (1000 orders, concurrency 1) | 500ms–5s |
| Telemetry publish (Kafka produce) | 1–5ms |
| TeardownVM | 50–200ms |
| Telemetry ingester flush + DB poll | 0–2500ms |
| **Total (submission to score)** | **~2–10 seconds** |
| Frontend poll cadence | +0–1500ms |

### 18.3 Bot Fleet Throughput at Various Concurrency Levels

At concurrency 1 with the reference implementation achieving ~150µs p99, the bot fleet processes approximately 1000 orders in ~500ms (including shadow engine validation), yielding ~2000 orders/second effective throughput.

At higher concurrency (untested in current configuration), throughput would scale near-linearly up to the point where:
- The MPSC channel's `Arc<Mutex<Receiver>>` becomes contended (~4–8 workers).
- The shadow engine's per-submission mutex serializes matching operations.
- The contestant engine's matching throughput saturates.

### 18.4 Shadow Engine Throughput vs Contestant Engine

The shadow engine uses `parking_lot::Mutex` (which spins briefly before parking, reducing syscall overhead for low-contention cases) and `AHashMap` (AES-NI accelerated hashing). For the benchmark's price range (90–109, ~20 price levels), the `BTreeMap` has a depth of ~4–5 nodes, making `first_key_value()` an O(1) operation in practice.

Expected shadow engine throughput: 500,000–1,000,000 orders/second for the benchmark workload (narrow price range, shallow book depth). This exceeds the bot fleet's dispatch rate by 2–3 orders of magnitude, ensuring the shadow engine is never the bottleneck.

### 18.5 Telemetry Ingestion Rate

The telemetry ingester processes one message per benchmark completion (not per order). With benchmarks completing every 2–10 seconds, the ingestion rate is ~0.1–0.5 messages/second — negligible load for both Redpanda and TimescaleDB.

The batch flush at 500 records or 2 seconds is designed for a future state where per-order telemetry is streamed (1000 messages per benchmark). At that scale, the ingester would need to sustain ~200–500 messages/second, which is well within PostgreSQL's write capacity.

### 18.6 Leaderboard Update Latency

The leaderboard API's `handleScores()` executes a SQL query that selects the latest row per submission and computes scores in Go. For 100 submissions, this query takes ~2–20ms on TimescaleDB (dominated by the `GROUP BY` aggregation). The frontend's 1500ms poll interval means a new benchmark result appears on the leaderboard within 1500ms of the leaderboard API having access to the data.

Total latency from benchmark completion to leaderboard visibility: benchmark finish → Kafka produce (~5ms) → ingester flush (~0–2000ms) → DB poll (~0–5000ms) → score computation (~1ms) → DB update (~2ms) → leaderboard query (~20ms) → frontend poll (~0–1500ms) = **~0.5–9 seconds**.
## 19. Contestant Guide

### 19.1 Submission Requirements

Contestants must submit a statically-linked Linux x86_64 ELF binary. The binary will be placed at `/strategy_bin` on a 64 MiB ext4 filesystem mounted at `/dev/vdb` inside a Firecracker microVM running a minimal Linux kernel. The binary must:

- Be self-contained — no shared library dependencies (`.so` files are not available in the guest rootfs beyond glibc basics).
- Start within 10 seconds of execution (the teardown safety window).
- Listen for connections on port 8080 (TCP and/or VSOCK).
- Accept newline-delimited JSON order messages.
- Return newline-delimited JSON responses containing a `trades` array.

The VM is provisioned with 1 vCPU and 256 MiB of RAM. There is no swap.

### 19.2 Port Contract & API Surface

The contestant engine must implement a JSON-over-TCP protocol on port 8080:

**Input (one JSON object per line):**

```json
{"type":"limit","price":100,"qty":10,"side":"buy","order_id":1}
{"type":"market","qty":5,"side":"sell","order_id":2}
{"type":"cancel","order_id":1}
```

**Output (one JSON object per line, in response to each input):**

```json
{"trades":[]}
{"trades":[{"maker_order_id":1,"taker_order_id":2,"price":100,"quantity":5}]}
{"trades":[]}
```

Every input line must produce exactly one output line. The `trades` array must contain all trades executed by this order, in execution order. Each trade must specify `maker_order_id`, `taker_order_id`, `price`, and `quantity`. For cancel and non-crossing limit orders, the `trades` array is empty.

**Side encoding:** `"buy"` or `"sell"` (lowercase strings).

**Order ID contract:** Order IDs are monotonically increasing positive integers starting from 1. Duplicate order IDs will not be sent (the bot fleet enforces this).

### 19.3 Warmup Phase Behavior

The platform does not send explicit warmup orders. However, the connection establishment phase (VSOCK handshake + first order) serves as an implicit warmup. The first order's latency will include any JIT compilation, page fault handling, or lazy initialization in the contestant engine — this latency is included in the measured percentiles.

Contestants who want to optimize their p99 should ensure their engine's hot paths are fully initialized before the first order arrives. Strategies include:
- Pre-allocating order book data structures to their expected capacity.
- Pre-faulting memory pages (e.g., iterating through allocated arrays to trigger page faults during startup rather than during order processing).
- Avoiding lazy initialization patterns that defer work to the first order.

### 19.4 Scoring Interpretation

The composite score ranges from 0 to ~100 (theoretically higher is possible due to the clamping bug documented in Section 10.8). Higher scores are better.

- **Score > 80:** Competitive — low latency, high correctness, stable performance.
- **Score 50–80:** Functional but with room for optimization.
- **Score < 50:** Significant performance or correctness issues.

The score breakdown is available via `GET /status?submission_id=<id>`:
- `p99_micros`: The 99th percentile round-trip latency in microseconds.
- `success_rate`: The fraction of orders whose trade output matched the shadow engine.
- `score`: The composite score.

### 19.5 Common Failure Modes in Contestant Engines

1. **Not listening on port 8080:** The bot fleet retries the VSOCK handshake indefinitely. The benchmark times out and the submission is marked FAILED.
2. **Not setting TCP_NODELAY:** Nagle's algorithm buffers small writes, adding up to 200ms delay per order. This inflates p99 dramatically.
3. **Blocking on accept() in a single-threaded event loop:** If the engine blocks on `accept()` while processing an order, subsequent connections queue up. With concurrency 1, this is not an issue, but the engine should use non-blocking I/O.
4. **Incorrect price-time priority:** Matching the wrong maker order (e.g., second-best price instead of best price) causes correctness failures that reduce the success rate.
5. **Not handling market orders:** Market orders have `price: 0` and must match against the best available price. An engine that rejects price=0 orders will fail 25% of the order stream.
6. **Not handling cancel for non-existent orders:** Cancel orders may target order IDs that were never placed (see Section 8.7). The engine must handle this gracefully, not crash.
7. **Buffer overflow on large responses:** A single order can produce up to 128 trades (if it crosses many price levels). The response JSON can exceed 1 KiB. Engines with fixed-size response buffers must account for this.

---

## 20. Known Limitations & Production Hardening Path

### 20.1 Measurement Accuracy Ceiling

The software timestamp approach (`Instant::now()`) provides a measurement floor of ±10–50µs per order. This is sufficient to distinguish engines with p99 differences of 100µs+ but insufficient for sub-10µs discrimination. The ceiling is imposed by three factors that cannot be eliminated without hardware changes:

1. **Tokio async scheduling**: The gap between `Instant::now()` and the actual `write()` syscall is non-deterministic. Moving to `io_uring` with registered buffers would eliminate the async scheduling gap by submitting the write directly from the measurement point.
2. **VSOCK traversal**: The Firecracker VMM copies data between host and guest address spaces in userspace. This adds 5–20µs that is included in every measurement but is not attributable to the contestant engine. Switching to `AF_VSOCK` with `vhost-vsock` in the kernel would eliminate the userspace copy.
3. **Host kernel scheduling**: Without `isolcpus`, the kernel can preempt the bot fleet's measurement thread. With `isolcpus`, residual NMI jitter contributes ~0–5µs.

**What production requires:** Hardware PTP timestamps, `vhost-vsock` kernel module, `isolcpus` on the boot command line, and `io_uring` for system call submission.

### 20.2 The Path to Hardware PTP

Hardware PTP requires:
- A PTP-capable NIC (Intel X710, XXV710, E810) with hardware timestamping support.
- A PTP grandmaster clock (GPS-disciplined or atomic) on the same network segment.
- `ptp4l` and `phc2sys` daemons for clock synchronization.
- Kernel support for `SO_TIMESTAMPING` with `SOF_TIMESTAMPING_TX_HARDWARE` and `SOF_TIMESTAMPING_RX_HARDWARE`.
- Replacement of the VSOCK data path with a network-based data path so that PTP timestamps can be captured at the NIC PHY layer.

Estimated measurement floor with hardware PTP: ±100ns — three orders of magnitude improvement over the current software approach.

### 20.3 The Path to DPDK Kernel Bypass

DPDK eliminates the kernel's TCP/IP stack entirely, processing packets in user-space with poll-mode drivers. For the bot fleet, this would mean:
- Direct access to the NIC's RX/TX rings via hugepage-backed memory-mapped I/O.
- Elimination of system calls (`write()`/`read()`) in the measurement path.
- Deterministic latency without kernel scheduling interference.

The trade-off: DPDK requires dedicating the NIC to user-space (removing it from the kernel's network stack), which prevents other services from using the network. On a single-host deployment, this would require a second NIC for management traffic.

### 20.4 The Path to Multi-Node Bot Fleet

The current bot fleet runs on a single host, sharing CPU cores with the rest of the infrastructure. At scale, the bot fleet's Tokio worker threads contend with the shadow engine for LLC (Last-Level Cache) bandwidth.

A multi-node deployment would:
- Dedicate a separate physical machine to the bot fleet, with its own isolated CPU cores and NIC.
- Use RDMA (Remote Direct Memory Access) for inter-node communication, eliminating kernel stack overhead on the network path.
- Run multiple bot fleet instances for horizontal scaling of concurrent benchmarks.

### 20.5 Shadow Engine Contention at Scale

The shadow engine serializes all operations on a per-submission order book behind a `parking_lot::Mutex`. At concurrency 1, this is not a bottleneck. At higher concurrency (e.g., 16 workers), the mutex becomes the throughput ceiling — workers block waiting for the lock while the shadow engine processes the current order.

Solutions:
- **Lock-free order book**: Replace the `Mutex<OrderBook>` with a lock-free concurrent data structure. This introduces the ABA problem (Section 8.4) and requires epoch-based reclamation or hazard pointers for safe memory management.
- **Partitioned matching**: Shard the order book by price range and assign each shard to a dedicated thread. This eliminates contention for non-overlapping price levels but requires cross-shard coordination for orders that cross multiple price levels.
- **Sequencer pattern**: Funnel all orders through a single sequencer thread that assigns sequence numbers, then distribute to matching threads by sequence number. This preserves total order (required for deterministic matching) while allowing parallel execution of non-conflicting orders.

### 20.6 What Production Deployment Requires

A production deployment of this platform — one that could run as a public-facing benchmarking service — requires the following beyond the current implementation:

1. **Authentication and authorization**: API keys for submission, rate limiting per user, and submission ownership validation.
2. **Binary scanning**: Static analysis of uploaded binaries for known malware signatures before execution.
3. **Firecracker jailer**: Running the VMM process inside Firecracker's jailer with seccomp filters, reducing the attack surface of the VMM itself.
4. **Network egress blocking**: Drop the NAT masquerade rule and block all outbound traffic from VMs.
5. **Multi-run scoring**: Execute each submission 3–5 times and report the median score with confidence intervals.
6. **Job reaper**: A background goroutine that marks `RUNNING` jobs older than 5 minutes as `FAILED`, preventing stuck jobs from accumulating.
7. **Observability**: Prometheus metrics for every service, Grafana dashboards for operational monitoring, distributed tracing with OpenTelemetry for end-to-end request tracing.
8. **Horizontal scaling**: Kubernetes deployment with per-node Firecracker orchestrators, centralized TimescaleDB, and a load-balanced submission API.
9. **Scoring recalibration**: Replace the linear scoring formula with a logarithmic curve (Section 10.8) and tune the reference points based on empirical data from a diverse set of contestant engines.
10. **Audit trail**: Immutable log of every submission, benchmark run, and score computation for dispute resolution.
