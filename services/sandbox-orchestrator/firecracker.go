package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	cfg VMConfig,
) (*firecracker.Machine, error) {

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	socketPath := filepath.Join(
		cfg.SocketDir,
		fmt.Sprintf("firecracker-%s.sock", cfg.VMID),
	)

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

		// Network Pipeline Link Mapping
		NetworkInterfaces: []firecracker.NetworkInterface{
			{
				StaticConfiguration: &firecracker.StaticNetworkConfiguration{
					MacAddress:  dynamicMac,
					HostDevName: cfg.TapName,
				},
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

	// 3. PASS THE LONG-LIVED vmCtx HERE, so the binary process doesn't get killed on function return
	// WithStdout pipes the VM's ttyS0 serial console to the host orchestrator stdout for debugging.
	cmd := firecracker.VMCommandBuilder{}.
		WithBin(cfg.FirecrackerBin).
		WithSocketPath(socketPath).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
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
		"microvm started vm_id=%s boot_time=%s",
		cfg.VMID,
		time.Since(startTime),
	)

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
