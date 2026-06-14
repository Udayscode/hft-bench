#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <vm_id>"
    exit 1
fi

VM_ID=$1
TAP_DEV="tap-${VM_ID:0:8}"
FC_PID=$(cat "/tmp/firecracker-${VM_ID}.pid" 2>/dev/null || pgrep -f "firecracker-${VM_ID}.sock" | head -n 1)

if [ -z "$FC_PID" ]; then
    echo "Could not find PID for Firecracker VM: $VM_ID"
    exit 1
fi

echo "Hardening Isolation for VM $VM_ID (PID: $FC_PID, TAP: $TAP_DEV)"

# 1. Network Traffic Control (tc)
# Limit inbound/outbound bandwidth to 100Mbit/s to prevent network starvation attacks
echo "Setting 100Mbit/s bandwidth limit on $TAP_DEV..."
# Clear existing rules
tc qdisc del dev "$TAP_DEV" root 2>/dev/null || true
# Add Token Bucket Filter (TBF) for egress (host -> vm)
tc qdisc add dev "$TAP_DEV" root tbf rate 100mbit burst 32kbit latency 400ms

# Add ingress policing (vm -> host)
tc qdisc add dev "$TAP_DEV" handle ffff: ingress
tc filter add dev "$TAP_DEV" parent ffff: protocol all u32 match u32 0 0 police rate 100mbit burst 32kbit drop flowid :1

# 2. Cgroups v2 CPU Hard-Limiting
# Prevent the Firecracker process itself from spinning and starving the host
CGROUP_DIR="/sys/fs/cgroup/hft-bench/vm-${VM_ID}"
mkdir -p "$CGROUP_DIR"

# Limit CPU to 100000 microseconds per 100000 (1 full CPU core max)
echo "100000 100000" > "$CGROUP_DIR/cpu.max"
# Limit memory to 256MB for the hypervisor process overhead
echo "268435456" > "$CGROUP_DIR/memory.max"

# Move the Firecracker process into this cgroup
echo "$FC_PID" > "$CGROUP_DIR/cgroup.procs"

echo "Isolation hardened successfully."
