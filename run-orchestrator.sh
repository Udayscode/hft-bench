#!/usr/bin/env bash
# =============================================================================
# SANDBOX ORCHESTRATOR LAUNCHER — hft-bench
# =============================================================================
# Usage: sudo ./run-orchestrator.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Root check ────────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    echo "ERROR: Please run this script with sudo (e.g., sudo $0)"
    exit 1
fi

# ── Source core allocation layout ─────────────────────────────────────────────
if [[ -f "$SCRIPT_DIR/scripts/cpu_layout.env" ]]; then
    source "$SCRIPT_DIR/scripts/cpu_layout.env"
else
    # Fallback defaults if cpu_layout.env is missing
    export ORCH_CORES="10,11"
    export FC_VCPU_CORES="2,3"
    export OS_CORES="0,1"
fi

# ── Step 1: Apply OS-level hardware sympathy tuning ───────────────────────────
echo "=== Step 1: Applying Hardware Tuning ==="
if [[ -f "$SCRIPT_DIR/scripts/tune_system.sh" ]]; then
    bash "$SCRIPT_DIR/scripts/tune_system.sh"
else
    echo "[WARN] tune_system.sh not found — skipping OS tuning"
fi

# ── Step 2: Bridge network setup ──────────────────────────────────────────────
echo ""
echo "=== Step 2: Configuring br0 Bridge Network ==="
if ip link show br0 > /dev/null 2>&1; then
    echo "Bridge 'br0' already exists."
else
    echo "Creating bridge 'br0'..."
    ip link add name br0 type bridge
    ip addr add 172.16.0.1/24 dev br0
    ip link set dev br0 up
    echo "Bridge 'br0' created successfully."
fi

# Ensure bridge interface is UP
ip link set dev br0 up

# ── Step 3: Enable IP forwarding for Firecracker guest networking ─────────────
echo ""
echo "=== Step 3: Enabling IP Forwarding ==="
sysctl -qw net.ipv4.ip_forward=1
iptables -t nat -A POSTROUTING -o "$(ip route get 8.8.8.8 2>/dev/null | awk '/dev/{print $5}' | head -1)" \
    -j MASQUERADE 2>/dev/null || true
echo "IP forwarding enabled."

# ── Step 4: Compile sandbox-orchestrator ─────────────────────────────────────
echo ""
echo "=== Step 4: Compiling sandbox-orchestrator ==="
ORCHESTRATOR_DIR="$SCRIPT_DIR/services/sandbox-orchestrator"

if [[ ! -d "$ORCHESTRATOR_DIR" ]]; then
    echo "ERROR: Orchestrator directory not found at $ORCHESTRATOR_DIR"
    exit 1
fi

(
    cd "$ORCHESTRATOR_DIR"
    echo "Building Go orchestrator binary..."
    CGO_ENABLED=1 go build -o orchestrator-bin -ldflags "-s -w" .
    echo "Build successful."
)

# ── Step 5: Compile guest strategy binary ────────────────────────────────────
echo ""
echo "=== Step 5: Compiling guest strategy binary ==="
STRATEGY_C="$SCRIPT_DIR/sandbox/strategy_bin.c"
STRATEGY_BIN="$SCRIPT_DIR/sandbox/strategy_bin"

if [[ -f "$STRATEGY_C" ]]; then
    gcc -O2 -static -o "$STRATEGY_BIN" "$STRATEGY_C"
    echo "strategy_bin compiled: $(file "$STRATEGY_BIN")"
else
    echo "[WARN] strategy_bin.c not found — using existing binary"
fi

# ── Step 6: Export FC vCPU pinning config for the orchestrator ────────────────
# sandbox-orchestrator reads FC_VCPU_CORES and pins each Firecracker VMM
# process to these cores immediately after boot via machine.PID() + taskset.
echo ""
echo "=== Step 6: Starting sandbox-orchestrator (pinned to cores $ORCH_CORES) ==="

export FC_VCPU_CORES
export RUST_LOG=info  # propagate log level if needed

cd "$ORCHESTRATOR_DIR"

# Check whether taskset is available for process pinning
if command -v taskset &>/dev/null; then
    echo "Pinning orchestrator to CPU cores: $ORCH_CORES"
    # exec replaces this shell with the pinned orchestrator process
    exec taskset -c "$ORCH_CORES" ./orchestrator-bin
else
    echo "[WARN] taskset not found — starting orchestrator without CPU pinning"
    exec ./orchestrator-bin
fi
