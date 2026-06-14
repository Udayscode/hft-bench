# HFT Benchmarking Platform

A production-grade, bare-metal high-frequency trading (HFT) matching engine benchmarking platform. 

This system provisions hardware-isolated, ephemerally-booted microVMs to execute anonymous, untrusted C/C++/Rust matching engine binaries. It subjects them to a deterministic limit order book blast via AF_VSOCK, validates every single trade against a Rust shadow engine, and scores them on p99 latency, throughput, and correctness.

For the definitive, 20-section technical deep-dive into the design philosophy, observer effect mitigations, and microsecond-level hardware sympathy tuning, see [ARCHITECTURE.md](./ARCHITECTURE.md).

## Core Architecture

We explicitly avoid shared-kernel containers (Docker/runc) due to isolation flaws, and `gVisor` due to its 10-30% syscall measurement penalty. 

- **Isolation:** Firecracker microVMs (`sandbox-orchestrator`), bound by Cgroup v2 and `tc` limits.
- **Load Gen & Hot Path:** Tokio/Rust (`bot-fleet`), injecting a 60/25/15 (Limit/Market/Cancel) order mix.
- **Correctness Validator:** High-throughput Rust `BTreeMap` Limit Order Book (`shadow-engine`).
- **Telemetry Data Plane:** Redpanda (Kafka) LZ4 streams into a TimescaleDB hypertable.
- **Control Plane & UI:** Go REST APIs (`submission-api`, `leaderboard`) and a Next.js frontend.

## Prerequisites

You cannot run this accurately on a Macbook or an oversubscribed cloud VM. It requires:
- Bare-metal Linux with `cgroup_v2` enabled.
- Hardware virtualization (`/dev/kvm`).
- `taskset`, `tc` (Traffic Control), and standard Linux networking utilities.
- Docker and `docker-compose` (for the control plane and DBs).
- Rust toolchain and Go 1.21+.

## Quick Start

1. **Bootstrap the Sandbox Dependencies**
   Firecracker binaries and the rootfs are not tracked in git. Run the fetch script to download the VMM, kernel, and build the custom Alpine rootfs:
   ```bash
   ./scripts/bootstrap-sandbox.sh
   ```

2. **Bootstrap the Data/Control Plane**
   Start TimescaleDB, Redpanda, the Go APIs, and the Next.js UI.
   ```bash
   docker-compose up -d
   ```

3. **Run the Bare-Metal Orchestrator**
   The Firecracker orchestrator MUST run as root outside Docker to manipulate TAP interfaces and apply CPU pinning.
   ```bash
   sudo ./run-orchestrator.sh
   ```
   *Note: This script automatically sources `cpu_layout.env` and executes `tune_system.sh` to mask IRQs, disable C-states, allocate Hugepages, and set up the `br0` bridge.*

4. **Submit a Contestant Engine**
   Compile your statically-linked Linux x86_64 ELF matching engine and submit it:
   ```bash
   curl -X POST -F "submission_id=my-engine" -F "binary=@strategy_bin" http://localhost:8080/submit
   ```

5. **View the Leaderboard**
   Navigate to `http://localhost:3000` to watch the real-time podium and run drill-downs.

## Contestant API Contract

Your binary is placed at `/strategy_bin` inside a 64 MiB ext4 Firecracker rootfs. It must listen on port `8080`, disable Nagle's algorithm (`TCP_NODELAY`), and process newline-delimited JSON.

**Input:**
```json
{"type":"limit","price":100,"qty":10,"side":"buy","order_id":1}
```

**Output:**
```json
{"trades":[{"maker_order_id":1,"taker_order_id":2,"price":100,"quantity":5}]}
```
*(If an order rests without crossing the spread, simply return `{"trades":[]}`)*

## Known Limitations

The current software timestamping (`Instant::now()` + `CLOCK_MONOTONIC`) provides a measurement floor of **±10–50µs**. Rankings within this band are not statistically meaningful. For sub-microsecond precision, the platform must be hardened with Hardware PTP timestamps, `isolcpus` kernel parameters, and a DPDK/io_uring data path. 

See Section 20 of `ARCHITECTURE.md` for the full production hardening roadmap.
