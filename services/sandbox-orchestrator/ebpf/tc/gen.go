// Package tc provides the compiled eBPF TC latency prober objects.
// The bpf2go tool reads the //go:generate directive below and emits:
//   tc_latency_bpf*.go  — Go types wrapping the compiled BPF objects
//   tc_latency_bpf*.o   — compiled BPF ELF objects (embedded via go:embed)
//
// Regenerate after editing bpf/tc_latency.c:
//   cd services/sandbox-orchestrator && go generate ./ebpf/tc/...
//
// Prerequisites (install once on the build host):
//   sudo apt-get install clang-12 llvm-12 libbpf-dev linux-headers-$(uname -r)
//   go install github.com/cilium/ebpf/cmd/bpf2go@latest
//
//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang-12 -cflags "-O2 -g -Wall -target bpf -D__TARGET_ARCH_x86" TcLatency bpf/tc_latency.c -- -I/usr/include -I/usr/include/x86_64-linux-gnu
package tc
