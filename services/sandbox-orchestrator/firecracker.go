package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	firecracker "github.com/firecracker-microvm/firecracker-go-sdk"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

const (
	defaultBootTimeout = 15 * time.Second
)


type VMConfig struct {
	VMID             string
	VCPUCount        int64
	MemoryMiB        int64
	KernelImagePath  string
	FirecrackerBin   string
	SocketDir        string
	TapName          string // Dynamic Interface
	AllocatedIP      string // dynamic ip for vm
	SubmissionDisk   string // Dynamic path pointing to submission-xxx.ext4
	VsockPath        string // Host-side Unix socket exposed by the VSOCK device
	CID              uint32 // Dynamic unique Context Identifier for VSOCK
}

// Helper function to safely parse integer from IP string chunks
func mustParseInt(s string) int {
	res, err := strconv.Atoi(s)
	if err != nil {
		return 2
	}
	return res
}

// CreateAndBootVM creates and starts a Firecracker microVM.
func CreateAndBootVM(
	parentCtx context.Context,
	cfg *VMConfig,
) (*firecracker.Machine, error) {

	if err := validateConfig(*cfg); err != nil {
		return nil, err
	}

	socketPath := filepath.Join(
		cfg.SocketDir,
		fmt.Sprintf("firecracker-%s.sock", cfg.VMID),
	)

	vsockPath := filepath.Join(
		cfg.SocketDir,
		fmt.Sprintf("vsock-%s.sock", cfg.VMID),
	)
	// Remove stale VSOCK socket if any
	if err := os.Remove(vsockPath); err != nil && !os.IsNotExist(err) {
		log.Printf("[vsock] stale socket cleanup warn path=%s err=%v", vsockPath, err)
	}

	// Remove stale socket if previous process crashed
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"failed removing stale socket %s: %w",
			socketPath,
			err,
		)
	}

	// 1. This timeout context is ONLY for the API handshake/boot phase duration
	bootCtx, bootCancel := context.WithTimeout(
		parentCtx,
		defaultBootTimeout,
	)
	defer bootCancel()

	// 2. Create a long-lived detached context for the physical OS process execution boundary
	vmCtx := context.Background()

	ipParts := strings.Split(cfg.AllocatedIP, ".")
	lastOctet := "02"
	if len(ipParts) == 4 {
		lastOctet = fmt.Sprintf("%02x", mustParseInt(ipParts[3]))
	}
	dynamicMac := fmt.Sprintf("AA:FC:00:00:00:%s", lastOctet)

	fcCfg := firecracker.Config{
		SocketPath:      socketPath,
		KernelImagePath: cfg.KernelImagePath,

		// Storage pipeline link mapping
		Drives: []models.Drive{
			{
				DriveID:      firecracker.String("rootfs"),
				PathOnHost:   firecracker.String("../../sandbox/rootfs.ext4"),
				IsRootDevice: firecracker.Bool(true),
				IsReadOnly:   firecracker.Bool(true),
			},
			{
				DriveID:      firecracker.String("submission"), // contestant payload slice injection target
				PathOnHost:   firecracker.String(cfg.SubmissionDisk),
				IsRootDevice: firecracker.Bool(false),
				IsReadOnly:   firecracker.Bool(false),
			},
		},

		// Network Pipeline Link Mapping (kept for legacy compatibility / control-plane traffic)
		NetworkInterfaces: []firecracker.NetworkInterface{
			{
				StaticConfiguration: &firecracker.StaticNetworkConfiguration{
					MacAddress:  dynamicMac,
					HostDevName: cfg.TapName,
				},
			},
		},

		// VSOCK Device
		VsockDevices: []firecracker.VsockDevice{
			{
				ID:   "vsock0",
				Path: vsockPath,
				CID:  cfg.CID,
			},
		},

		MachineCfg: models.MachineConfiguration{
			VcpuCount:  firecracker.Int64(cfg.VCPUCount),
			MemSizeMib: firecracker.Int64(cfg.MemoryMiB),
		},

		KernelArgs: strings.Join([]string{
			"console=ttyS0",
			"reboot=k",
			"panic=1",
			"pci=off",
			"root=/dev/vda",
			"rw",
			fmt.Sprintf("ip=%s::172.16.0.1:255.255.255.0:microvm-%s:eth0:off", cfg.AllocatedIP, cfg.VMID),
			"init=/init",
		}, " "),
	}

	cfg.VsockPath = vsockPath

	// PASS THE LONG-LIVED vmCtx HERE, so the binary process doesn't get killed on function return
	// WithStdout pipes the VM's ttyS0 serial console to the host orchestrator stdout for debugging.
	var stdout, stderr io.Writer = os.Stdout, os.Stderr
	if logFile, err := os.OpenFile("/tmp/firecracker-debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
		stdout = io.MultiWriter(os.Stdout, logFile)
		stderr = io.MultiWriter(os.Stderr, logFile)
	}
	cmd := firecracker.VMCommandBuilder{}.
		WithBin(cfg.FirecrackerBin).
		WithSocketPath(socketPath).
		WithStdout(stdout).
		WithStderr(stderr).
		Build(vmCtx)

	startTime := time.Now()

	log.Printf(
		"starting microvm vm_id=%s vcpus=%d memory_mib=%d",
		cfg.VMID,
		cfg.VCPUCount,
		cfg.MemoryMiB,
	)

	// 4. Use bootCtx for machine initialization API calls, but the process runner retains vmCtx
	machine, err := firecracker.NewMachine(
		vmCtx,
		fcCfg,
		firecracker.WithProcessRunner(cmd),
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed creating machine vm_id=%s: %w",
			cfg.VMID,
			err,
		)
	}

	// 5. We must use vmCtx for Start() because the Firecracker SDK attaches a lifecycle
	// goroutine to the context passed to Start(). If we pass bootCtx, the VM will be
	// terminated as soon as bootCtx is cancelled (which happens on function return).
	errCh := make(chan error, 1)
	go func() {
		errCh <- machine.Start(vmCtx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf(
				"failed starting machine vm_id=%s: %w",
				cfg.VMID,
				err,
			)
		}
	case <-bootCtx.Done():
		machine.StopVMM()
		return nil, fmt.Errorf("timeout starting machine vm_id=%s", cfg.VMID)
	}

	log.Printf(
		"microvm started vm_id=%s boot_time=%s vsock=%s",
		cfg.VMID,
		time.Since(startTime),
		vsockPath,
	)

	// Pin Firecracker VMM process to dedicated vCPU cores.
	if err := pinFirecrackerProcess(machine, cfg.VMID); err != nil {
		// Non-fatal — log and continue. Pinning is a best-effort optimization.
		log.Printf("[cpu-pin] WARNING: vCPU pinning skipped vm_id=%s err=%v", cfg.VMID, err)
	}

	// Apply cgroups v2 resource hard limits
	if err := applyCgroupLimits(machine, cfg.VMID); err != nil {
		log.Printf("[cgroup] WARNING: cgroup isolation skipped vm_id=%s err=%v", cfg.VMID, err)
	}

	return machine, nil
}

func validateConfig(cfg VMConfig) error {

	if cfg.VMID == "" {
		return errors.New("vm_id is required")
	}

	if cfg.VCPUCount <= 0 {
		return errors.New("vcpu_count must be greater than 0")
	}

	if cfg.MemoryMiB <= 0 {
		return errors.New("memory_mib must be greater than 0")
	}

	if cfg.KernelImagePath == "" {
		return errors.New("kernel_image_path is required")
	}

	if cfg.FirecrackerBin == "" {
		return errors.New("firecracker_bin is required")
	}

	if cfg.SocketDir == "" {
		return errors.New("socket_dir is required")
	}

	return nil
}

// pinFirecrackerProcess pins the Firecracker VMM process (and all its vCPU
// OS threads) to the CPU cores specified in the FC_VCPU_CORES environment
// variable. Falls back gracefully when taskset is not available or FC_VCPU_CORES
// is not set.
//
// How Firecracker vCPU threading works:
//   Firecracker models each guest vCPU as a dedicated OS thread (pthread).
//   Pinning the parent VMM process propagates the CPU affinity mask to all
//   child threads, so every vCPU thread is bound to the same isolated physical
//   core(s). Combined with isolcpus on bare metal, the guest vCPU threads
//   never preempt each other and never share a core with kernel tasks.
var vmCoreIndex uint32

func pinFirecrackerProcess(machine *firecracker.Machine, vmID string) error {
	cores := os.Getenv("FC_VCPU_CORES")
	if cores == "" {
		// FC_VCPU_CORES not set — pinning is optional, skip silently.
		return nil
	}

	coreList := strings.Split(cores, ",")
	if len(coreList) > 0 {
		idx := atomic.AddUint32(&vmCoreIndex, 1) - 1
		cores = strings.TrimSpace(coreList[idx%uint32(len(coreList))])
	}

	// Check taskset availability (not present in some minimal containers)
	if _, err := exec.LookPath("taskset"); err != nil {
		return fmt.Errorf("taskset not found in PATH (install util-linux): %w", err)
	}

	pid, err := machine.PID()
	if err != nil {
		return fmt.Errorf("failed getting Firecracker PID: %w", err)
	}
	if pid <= 0 {
		return fmt.Errorf("invalid Firecracker PID: %d", pid)
	}

	// Pin the process: taskset -pc <cores> <pid>
	// -p  → operate on existing process by PID
	// -c  → human-readable CPU list (e.g. "2,3" or "2-5")
	cmd := exec.Command("taskset", "-pc", cores, strconv.Itoa(pid))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("taskset failed pid=%d cores=%s output=%s: %w",
			pid, cores, string(out), err)
	}

	log.Printf(
		"[cpu-pin] Firecracker VMM pinned vm_id=%s pid=%d cores=%s",
		vmID, pid, cores,
	)

	// Belt-and-suspenders: also pin each thread individually via /proc/<pid>/task
	// Some older kernels don't automatically inherit affinity for already-running
	// pthreads when the parent's affinity mask changes.
	taskdir := fmt.Sprintf("/proc/%d/task", pid)
	entries, err := os.ReadDir(taskdir)
	if err != nil {
		// /proc/<pid>/task unavailable — the parent process pin dominates
		return nil
	}

	pinned := 0
	for _, entry := range entries {
		tid := entry.Name()
		tidCmd := exec.Command("taskset", "-pc", cores, tid)
		if err := tidCmd.Run(); err == nil {
			pinned++
		}
	}

	log.Printf(
		"[cpu-pin] Pinned %d/%d vCPU threads vm_id=%s cores=%s",
		pinned, len(entries), vmID, cores,
	)

	return nil
}

// applyCgroupLimits creates a cgroup v2 for the Firecracker VM and hard-limits
// its CPU execution and memory footprint to prevent starvation attacks.
func applyCgroupLimits(machine *firecracker.Machine, vmID string) error {
	pid, err := machine.PID()
	if err != nil || pid <= 0 {
		return fmt.Errorf("invalid PID: %v", err)
	}

	cgroupDir := fmt.Sprintf("/sys/fs/cgroup/hft-bench/vm-%s", vmID)
	if err := os.MkdirAll(cgroupDir, 0755); err != nil {
		return fmt.Errorf("failed creating cgroup directory: %w", err)
	}

	// 1 full CPU core max (100000us per 100000us)
	if err := os.WriteFile(fmt.Sprintf("%s/cpu.max", cgroupDir), []byte("100000 100000"), 0644); err != nil {
		return fmt.Errorf("failed setting cpu.max: %w", err)
	}

	// 256MB max memory
	if err := os.WriteFile(fmt.Sprintf("%s/memory.max", cgroupDir), []byte("268435456"), 0644); err != nil {
		return fmt.Errorf("failed setting memory.max: %w", err)
	}

	// Move process into cgroup
	if err := os.WriteFile(fmt.Sprintf("%s/cgroup.procs", cgroupDir), []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
		return fmt.Errorf("failed moving process to cgroup: %w", err)
	}

	log.Printf("[cgroup] hardened vm_id=%s pid=%d", vmID, pid)
	return nil
}
