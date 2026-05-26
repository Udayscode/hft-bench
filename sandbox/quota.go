package sandbox

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	cgroupRoot = "/sys/fs/cgroup/sandbox"

	cpuPeriodMicros = 100000

	maxVCPUCount = 64

	minMemoryMiB = 16
)

type SandboxLimits struct {
	VMID         string
	MemoryMaxMiB int64
	VCPUCount    int64
}

func EnforceSandboxLimits(
	cfg SandboxLimits,
) error {

	if err := validateSandboxLimits(cfg); err != nil {
		return err
	}

	cgroupPath := filepath.Join(
		cgroupRoot,
		cfg.VMID,
	)

	log.Printf(
		"creating cgroup vm_id=%s memory_mib=%d vcpus=%d",
		cfg.VMID,
		cfg.MemoryMaxMiB,
		cfg.VCPUCount,
	)

	if err := os.MkdirAll(
		cgroupPath,
		0755,
	); err != nil {

		return fmt.Errorf(
			"failed creating cgroup directory %s: %w",
			cgroupPath,
			err,
		)
	}

	// Memory hard limit
	memLimitBytes := cfg.MemoryMaxMiB * 1024 * 1024

	if err := writeCgroupValue(
		filepath.Join(cgroupPath, "memory.max"),
		strconv.FormatInt(memLimitBytes, 10),
	); err != nil {

		return fmt.Errorf(
			"failed configuring memory.max: %w",
			err,
		)
	}

	// Disable swap usage
	if err := writeCgroupValue(
		filepath.Join(cgroupPath, "memory.swap.max"),
		"0",
	); err != nil {

		return fmt.Errorf(
			"failed configuring memory.swap.max: %w",
			err,
		)
	}

	// CPU quota
	cpuQuota := cfg.VCPUCount * cpuPeriodMicros

	cpuMaxValue := fmt.Sprintf(
		"%d %d",
		cpuQuota,
		cpuPeriodMicros,
	)

	if err := writeCgroupValue(
		filepath.Join(cgroupPath, "cpu.max"),
		cpuMaxValue,
	); err != nil {

		return fmt.Errorf(
			"failed configuring cpu.max: %w",
			err,
		)
	}

	log.Printf(
		"cgroup configured vm_id=%s",
		cfg.VMID,
	)

	return nil
}

func AttachProcessToSandbox(
	vmID string,
	pid int,
) error {

	if strings.TrimSpace(vmID) == "" {
		return errors.New("vm_id is required")
	}

	if pid <= 0 {
		return errors.New("invalid pid")
	}

	cgroupPath := filepath.Join(
		cgroupRoot,
		vmID,
	)

	procsFile := filepath.Join(
		cgroupPath,
		"cgroup.procs",
	)

	log.Printf(
		"attaching process to cgroup vm_id=%s pid=%d",
		vmID,
		pid,
	)

	if err := writeCgroupValue(
		procsFile,
		strconv.Itoa(pid),
	); err != nil {

		return fmt.Errorf(
			"failed attaching process pid=%d to cgroup: %w",
			pid,
			err,
		)
	}

	return nil
}

func CleanupSandboxLimits(
	vmID string,
) error {

	if strings.TrimSpace(vmID) == "" {
		return nil
	}

	cgroupPath := filepath.Join(
		cgroupRoot,
		vmID,
	)

	_, err := os.Stat(cgroupPath)
	if err != nil {

		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf(
			"failed checking cgroup existence: %w",
			err,
		)
	}

	log.Printf(
		"cleaning up cgroup vm_id=%s",
		vmID,
	)

	if err := os.Remove(cgroupPath); err != nil {

		return fmt.Errorf(
			"failed removing cgroup %s: %w",
			cgroupPath,
			err,
		)
	}

	return nil
}

func validateSandboxLimits(
	cfg SandboxLimits,
) error {

	if strings.TrimSpace(cfg.VMID) == "" {
		return errors.New(
			"vm_id is required",
		)
	}

	if cfg.MemoryMaxMiB < minMemoryMiB {
		return fmt.Errorf(
			"memory_mib must be at least %d",
			minMemoryMiB,
		)
	}

	if cfg.VCPUCount <= 0 {
		return errors.New(
			"vcpu_count must be greater than 0",
		)
	}

	if cfg.VCPUCount > maxVCPUCount {
		return fmt.Errorf(
			"vcpu_count exceeds maximum allowed: %d",
			maxVCPUCount,
		)
	}

	return nil
}

func writeCgroupValue(
	path string,
	value string,
) error {

	if err := os.WriteFile(
		path,
		[]byte(value),
		0644,
	); err != nil {

		return fmt.Errorf(
			"failed writing cgroup value path=%s value=%s err=%w",
			path,
			value,
			err,
		)
	}

	return nil
}