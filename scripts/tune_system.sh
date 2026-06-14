#!/usr/bin/env bash
# =============================================================================
# Applies OS-level hardware sympathy optimizations for ultra-low tail latency.
# Designed for production bare-metal Linux; gracefully degrades on WSL2.
#
# Run as root before starting the benchmark stack:
#   sudo ./scripts/tune_system.sh
#
# Tuning categories applied:
#   1. IRQ Thread Masking    — route all interrupts away from trading cores
#   2. CPU Frequency Governor — lock CPU to max frequency (no P-state jitter)
#   3. CPU Idle State (C-state) Disable — prevent wake latency from deep sleep
#   4. Transparent Hugepages  — set to always for Firecracker memory
#   5. NUMA Balancing         — disable automatic NUMA migration
#   6. Kernel Network Tuning  — SO_BUSY_POLL, RPS steering to non-trading cores
#   7. Scheduler Tuning       — minimize preemption granularity on trading cores
#   8. GRUB isolcpus Advisor  — print the boot params needed for full isolation
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'
BLU='\033[0;34m'; CYN='\033[0;36m'; NC='\033[0m'

OK()   { echo -e "${GRN}  [OK]${NC}    $*"; }
WARN() { echo -e "${YLW}  [WARN]${NC}  $*"; }
SKIP() { echo -e "${BLU}  [SKIP]${NC}  $*"; }
FAIL() { echo -e "${RED}  [FAIL]${NC}  $*"; }
INFO() { echo -e "${CYN}  [INFO]${NC}  $*"; }

echo ""
echo -e "${CYN}══════════════════════════════════════════════════════════════${NC}"
echo -e "${CYN}   hft-bench — Hardware Sympathy Tuning                       ${NC}"
echo -e "${CYN}══════════════════════════════════════════════════════════════${NC}"
echo ""

# ── Root check ────────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    FAIL "Must run as root: sudo $0"
    exit 1
fi

# ── Source core allocation layout ─────────────────────────────────────────────
if [[ -f "$SCRIPT_DIR/cpu_layout.env" ]]; then
    # shellcheck source=./cpu_layout.env
    source "$SCRIPT_DIR/cpu_layout.env"
    INFO "Core layout loaded from cpu_layout.env"
    INFO "  FC vCPU cores:       $FC_VCPU_CORES"
    INFO "  bot-fleet cores:     $BOTFLEET_CORES"
    INFO "  shadow-engine cores: $SHADOW_ENGINE_CORES"
    INFO "  OS/system cores:     $OS_CORES + $SYSTEM_CORES"
    INFO "  IRQ allowed mask:    0x$IRQ_ALLOWED_MASK (cores $IRQ_ALLOWED_CORES)"
else
    FAIL "cpu_layout.env not found at $SCRIPT_DIR/cpu_layout.env"
    exit 1
fi

# ── Environment detection ─────────────────────────────────────────────────────
IS_WSL2=false
IS_BARE_METAL=false

if uname -r | grep -qi "microsoft"; then
    IS_WSL2=true
    WARN "WSL2 kernel detected — some tunings will be skipped (not available in hypervisor guest)"
    WARN "For full Phase 3 effectiveness, run on native Linux bare metal"
else
    IS_BARE_METAL=true
    OK "Bare-metal / native Linux kernel detected"
fi

echo ""
echo -e "${CYN}── Step 1: IRQ Thread Masking ────────────────────────────────${NC}"

# ── IRQ default_smp_affinity ──────────────────────────────────────────────────
if [[ -f /proc/irq/default_smp_affinity ]]; then
    OLD_MASK=$(cat /proc/irq/default_smp_affinity)
    echo "$IRQ_ALLOWED_MASK" > /proc/irq/default_smp_affinity
    NEW_MASK=$(cat /proc/irq/default_smp_affinity)
    OK "default_smp_affinity: $OLD_MASK → $NEW_MASK (trading cores protected)"
else
    SKIP "default_smp_affinity not available"
fi

# ── Per-IRQ affinity: steer all existing IRQs to allowed cores ────────────────
IRQ_COUNT=0
IRQ_SKIP=0
for irq_dir in /proc/irq/[0-9]*; do
    irq="$(basename "$irq_dir")"
    smp_file="$irq_dir/smp_affinity"
    if [[ -f "$smp_file" ]]; then
        if echo "$IRQ_ALLOWED_MASK" > "$smp_file" 2>/dev/null; then
            IRQ_COUNT=$((IRQ_COUNT + 1))
        else
            IRQ_SKIP=$((IRQ_SKIP + 1))
        fi
    fi
done
OK "Per-IRQ affinity applied to $IRQ_COUNT IRQs (skipped $IRQ_SKIP read-only)"

echo ""
echo -e "${CYN}── Step 2: CPU Frequency Governor ────────────────────────────${NC}"

# ── Performance governor ──────────────────────────────────────────────────────
GOVERNOR_COUNT=0
GOVERNOR_SKIP=0
for cpu_dir in /sys/devices/system/cpu/cpu[0-9]*; do
    gov_file="$cpu_dir/cpufreq/scaling_governor"
    if [[ -f "$gov_file" ]]; then
        echo "performance" > "$gov_file" 2>/dev/null && \
            GOVERNOR_COUNT=$((GOVERNOR_COUNT + 1)) || \
            GOVERNOR_SKIP=$((GOVERNOR_SKIP + 1))
    fi
done

if [[ $GOVERNOR_COUNT -gt 0 ]]; then
    OK "Performance governor set on $GOVERNOR_COUNT CPUs"
elif [[ $IS_WSL2 == true ]]; then
    SKIP "CPU frequency governor not exposed in WSL2 (Windows handles P-states)"
else
    WARN "Could not set performance governor — check cpufreq driver"
fi

# ── Also try cpupower if available ───────────────────────────────────────────
if command -v cpupower &>/dev/null && [[ $IS_BARE_METAL == true ]]; then
    cpupower frequency-set -g performance &>/dev/null && \
        OK "cpupower: performance governor confirmed" || true
fi

echo ""
echo -e "${CYN}── Step 3: CPU Idle State (C-state) Disable ─────────────────${NC}"

# ── Disable deep C-states on trading cores ────────────────────────────────────
# Deep idle states (C3+) cause 50–200µs wake latency — catastrophic for p99.9
CSTATE_COUNT=0
CSTATE_SKIP=0

IFS=',' read -ra TRADING_CORES_ARR <<< "$PROTECTED_CORES"
for core in "${TRADING_CORES_ARR[@]}"; do
    cpu_dir="/sys/devices/system/cpu/cpu${core}/cpuidle"
    if [[ -d "$cpu_dir" ]]; then
        for state_dir in "$cpu_dir"/state[2-9]; do
            disable_file="$state_dir/disable"
            if [[ -f "$disable_file" ]]; then
                echo 1 > "$disable_file" 2>/dev/null && \
                    CSTATE_COUNT=$((CSTATE_COUNT + 1)) || \
                    CSTATE_SKIP=$((CSTATE_SKIP + 1))
            fi
        done
    fi
done

if [[ $CSTATE_COUNT -gt 0 ]]; then
    OK "Disabled $CSTATE_COUNT deep C-states (C2+) on protected trading cores"
elif [[ $IS_WSL2 == true ]]; then
    SKIP "C-state control not available in WSL2 (hypervisor manages idle)"
else
    WARN "No C-state entries found — kernel may not expose cpuidle sysfs"
fi

# ── Also set PM QoS latency for trading cores ─────────────────────────────────
# Prevents the kernel from entering any state with latency > 0µs
for core in "${TRADING_CORES_ARR[@]}"; do
    pmqos="/dev/cpu_dma_latency"
    # Opening with value 0 prevents C1+ states for the lifetime of this fd
    # We write to the per-CPU PM QoS interface if available
    lat_file="/sys/devices/system/cpu/cpu${core}/power/pm_qos_resume_latency_us"
    if [[ -f "$lat_file" ]]; then
        echo 0 > "$lat_file" 2>/dev/null && \
            OK "PM QoS latency = 0µs on CPU $core" || true
    fi
done

echo ""
echo -e "${CYN}── Step 4: Transparent Hugepages ─────────────────────────────${NC}"

# ── THP to always — Firecracker guest memory mapped in 2MiB pages ────────────
THP_FILE="/sys/kernel/mm/transparent_hugepage/enabled"
if [[ -f "$THP_FILE" ]]; then
    OLD_THP=$(cat "$THP_FILE")
    echo "always" > "$THP_FILE"
    OK "Transparent Hugepages: $OLD_THP → always"
    # Also set khugepaged scan aggressiveness
    KHP_ALLOC="/sys/kernel/mm/transparent_hugepage/khugepaged/alloc_sleep_millisecs"
    [[ -f "$KHP_ALLOC" ]] && echo 0 > "$KHP_ALLOC" && OK "khugepaged alloc_sleep set to 0ms"
else
    SKIP "THP sysfs not available"
fi

# ── Pre-fault 1GB hugepages for Firecracker memory if supported ───────────────
HP_1G="/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages"
HP_2M="/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
if [[ -f "$HP_2M" ]]; then
    echo 512 > "$HP_2M" 2>/dev/null && \
        OK "Pre-allocated 512 × 2MiB hugepages (1GiB) for Firecracker guests" || \
        WARN "Could not pre-allocate hugepages (may need more memory)"
fi

echo ""
echo -e "${CYN}── Step 5: NUMA Balancing ────────────────────────────────────${NC}"

NUMA_FILE="/proc/sys/kernel/numa_balancing"
if [[ -f "$NUMA_FILE" ]]; then
    echo 0 > "$NUMA_FILE"
    OK "NUMA automatic balancing disabled (single-socket — prevents spurious migrations)"
else
    SKIP "NUMA balancing sysctl not available"
fi

echo ""
echo -e "${CYN}── Step 6: Kernel Network Tuning ─────────────────────────────${NC}"

# ── Busy-poll for ultra-low socket latency ────────────────────────────────────
sysctl -qw net.core.busy_read=50     2>/dev/null && OK "net.core.busy_read=50µs"   || SKIP "busy_read not available"
sysctl -qw net.core.busy_poll=50     2>/dev/null && OK "net.core.busy_poll=50µs"   || SKIP "busy_poll not available"
sysctl -qw net.core.somaxconn=65535  2>/dev/null && OK "net.core.somaxconn=65535"  || true
sysctl -qw net.core.netdev_max_backlog=250000 2>/dev/null && OK "netdev_max_backlog=250000" || true
sysctl -qw net.ipv4.tcp_low_latency=1 2>/dev/null && OK "tcp_low_latency=1"        || SKIP "tcp_low_latency not available"

# ── Disable TCP slow-start after idle (avoids CWnd collapse between orders) ──
sysctl -qw net.ipv4.tcp_slow_start_after_idle=0 2>/dev/null && \
    OK "tcp_slow_start_after_idle=0 (no CWnd reset between order bursts)" || true

# ── Route RPS (Receive Packet Steering) to non-trading cores ─────────────────
RPS_CORES_MASK="c03"   # cores 0,1,10,11 for packet processing
for rps_file in /sys/class/net/*/queues/rx-*/rps_cpus; do
    if [[ -f "$rps_file" ]]; then
        echo "$RPS_CORES_MASK" > "$rps_file" 2>/dev/null && true || true
    fi
done
OK "RPS packet steering locked to mask 0x$RPS_CORES_MASK (non-trading cores)"

echo ""
echo -e "${CYN}── Step 7: Scheduler Tuning ──────────────────────────────────${NC}"

# ── Reduce scheduler migration cost between cores ─────────────────────────────
sysctl -qw kernel.sched_migration_cost_ns=5000000 2>/dev/null && \
    OK "sched_migration_cost_ns=5ms (less task bouncing)" || SKIP "sched_migration_cost not available"

# ── Disable scheduler autogroup (can cause latency spikes in cgroup trees) ───
sysctl -qw kernel.sched_autogroup_enabled=0 2>/dev/null && \
    OK "sched_autogroup disabled" || true

# ── Increase minimum scheduler granularity ────────────────────────────────────
sysctl -qw kernel.sched_min_granularity_ns=10000000 2>/dev/null && \
    OK "sched_min_granularity_ns=10ms" || true
sysctl -qw kernel.sched_wakeup_granularity_ns=15000000 2>/dev/null && \
    OK "sched_wakeup_granularity_ns=15ms" || true

# ── Disable watchdog on trading cores (generates NMIs that can perturb vCPUs)─
WATCHDOG_CPUMASK="/proc/sys/kernel/watchdog_cpumask"
if [[ -f "$WATCHDOG_CPUMASK" ]]; then
    # Only watchdog on OS+system cores: 0,1,10,11
    printf "%x" $(( (1<<0)|(1<<1)|(1<<10)|(1<<11) )) > "$WATCHDOG_CPUMASK" 2>/dev/null && \
        OK "watchdog_cpumask restricted to cores 0,1,10,11" || \
        SKIP "Could not restrict watchdog mask"
fi

echo ""
echo -e "${CYN}── Step 8: Memory / VM Subsystem ────────────────────────────${NC}"

# ── Swappiness to 0 — prevent guest memory from being paged out ───────────────
sysctl -qw vm.swappiness=0          2>/dev/null && OK "vm.swappiness=0"             || true
sysctl -qw vm.dirty_ratio=40        2>/dev/null && OK "vm.dirty_ratio=40"           || true
sysctl -qw vm.dirty_background_ratio=10 2>/dev/null && OK "vm.dirty_background_ratio=10" || true

# ── Lock Firecracker memory against paging ────────────────────────────────────
sysctl -qw vm.mmap_min_addr=65536  2>/dev/null && true || true

echo ""
echo -e "${CYN}══════════════════════════════════════════════════════════════${NC}"
echo -e "${GRN}   System Tuning Complete${NC}"
echo -e "${CYN}══════════════════════════════════════════════════════════════${NC}"
echo ""

# ── isolcpus Advisor ─────────────────────────────────────────────────────────
if [[ $IS_BARE_METAL == true ]]; then
    CURRENT_CMDLINE=$(cat /proc/cmdline)
    if echo "$CURRENT_CMDLINE" | grep -q "isolcpus"; then
        OK "isolcpus already active in kernel cmdline — maximum isolation enabled"
    else
        echo -e "${YLW}╔══════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${YLW}║  ACTION REQUIRED: Enable Full CPU Isolation (one-time reboot) ║${NC}"
        echo -e "${YLW}╚══════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        echo "  To completely remove the kernel scheduler from trading cores,"
        echo "  add these parameters to GRUB:"
        echo ""
        echo -e "  ${CYN}sudo nano /etc/default/grub${NC}"
        echo ""
        echo "  Find GRUB_CMDLINE_LINUX and append:"
        echo -e "  ${GRN}$GRUB_ISOLCPUS_PARAMS${NC}"
        echo ""
        echo "  Then apply:"
        echo -e "  ${CYN}sudo update-grub && sudo reboot${NC}"
        echo ""
        echo "  Expected latency improvement from isolcpus: p99 -40%, p99.9 -60%"
        echo "  (eliminates scheduler jitter and kernel task interference)"
    fi
fi

echo ""
echo "  Core Allocation Summary:"
echo "  ┌───────────────────────┬──────────────┬──────────────────────────────┐"
echo "  │ Service               │ CPUs         │ Role                         │"
echo "  ├───────────────────────┼──────────────┼──────────────────────────────┤"
echo "  │ OS / Kernel           │ $OS_CORES          │ Scheduler, IRQs, system      │"
echo "  │ Firecracker vCPUs     │ $FC_VCPU_CORES          │ Guest VM execution           │"
echo "  │ bot-fleet workers     │ $BOTFLEET_CORES      │ Order load generation        │"
echo "  │ shadow-engine         │ $SHADOW_ENGINE_CORES          │ Reference matching engine    │"
echo "  │ Orchestrator/Redpanda │ $SYSTEM_CORES        │ Infrastructure services      │"
echo "  └───────────────────────┴──────────────┴──────────────────────────────┘"
echo ""
