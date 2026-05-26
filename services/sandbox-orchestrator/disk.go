package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	diskCommandTimeout = 10 * time.Second

	imageSizeMB = 64

	tmpRootDir = "/tmp"
)

type SubmissionImageConfig struct {
	SubmissionID string
	StrategyPath string
}

func CreateSubmissionImage(
	parentCtx context.Context,
	cfg SubmissionImageConfig,
) (string, error) {

	if err := validateSubmissionImageConfig(cfg); err != nil {
		return "", err
	}

	workspaceDir := filepath.Join(
		tmpRootDir,
		fmt.Sprintf("workspace-%s", cfg.SubmissionID),
	)

	imagePath := filepath.Join(
		tmpRootDir,
		fmt.Sprintf("submission-%s.ext4", cfg.SubmissionID),
	)

	log.Printf(
		"creating submission image submission_id=%s",
		cfg.SubmissionID,
	)

	// Cleanup stale artifacts
	_ = os.RemoveAll(workspaceDir)
	_ = os.Remove(imagePath)

	if err := os.MkdirAll(workspaceDir, 0755); err != nil {
		return "", fmt.Errorf(
			"failed creating workspace directory: %w",
			err,
		)
	}

	// Ensure cleanup on failure
	defer func() {
		_ = os.RemoveAll(workspaceDir)
	}()

	runScriptPath := filepath.Join(
		workspaceDir,
		"run.sh",
	)

	strategyDestPath := filepath.Join(
		workspaceDir,
		"strategy_bin",
	)

	if err := os.WriteFile(
		runScriptPath,
		[]byte(buildGuestBootstrapScript()),
		0755,
	); err != nil {

		return "", fmt.Errorf(
			"failed writing guest bootstrap script: %w",
			err,
		)
	}

	strategyContent, err := os.ReadFile(cfg.StrategyPath)
	if err != nil {
		return "", fmt.Errorf(
			"failed reading strategy binary: %w",
			err,
		)
	}

	if err := os.WriteFile(
		strategyDestPath,
		strategyContent,
		0755,
	); err != nil {

		return "", fmt.Errorf(
			"failed copying strategy binary into workspace: %w",
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		parentCtx,
		diskCommandTimeout,
	)

	defer cancel()

	// Create sparse raw image
	if err := runDiskCommand(
		ctx,
		"truncate",
		"-s",
		fmt.Sprintf("%dM", imageSizeMB),
		imagePath,
	); err != nil {

		return "", fmt.Errorf(
			"failed allocating raw image: %w",
			err,
		)
	}

	// Format ext4 filesystem
	if err := runDiskCommand(
		ctx,
		"mkfs.ext4",
		"-F",
		imagePath,
	); err != nil {

		return "", fmt.Errorf(
			"failed formatting ext4 filesystem: %w",
			err,
		)
	}

	// Mount image via loop device and copy files — debugfs bypasses the journal
	// which causes the kernel to not see the files when mounting inside the VM.
	mntDir, err := os.MkdirTemp("", "fc-img-*")
	if err != nil {
		return "", fmt.Errorf("failed creating temp mount dir: %w", err)
	}
	defer os.RemoveAll(mntDir)

	if err := runDiskCommand(ctx, "mount", "-o", "loop", imagePath, mntDir); err != nil {
		return "", fmt.Errorf("failed mounting image: %w", err)
	}

	copyErr := copyFilesIntoMount(mntDir, map[string]string{
		"run.sh":      runScriptPath,
		"strategy_bin": strategyDestPath,
	})

	// Always unmount, even on copy error
	_ = runDiskCommand(context.Background(), "umount", mntDir)

	if copyErr != nil {
		return "", copyErr
	}

	log.Printf(
		"submission image created submission_id=%s image=%s",
		cfg.SubmissionID,
		imagePath,
	)

	return imagePath, nil
}

func CleanupSubmissionImage(imagePath string) error {

	if strings.TrimSpace(imagePath) == "" {
		return nil
	}

	if err := os.Remove(imagePath); err != nil {

		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf(
			"failed removing submission image %s: %w",
			imagePath,
			err,
		)
	}

	log.Printf(
		"submission image removed image=%s",
		imagePath,
	)

	return nil
}

func validateSubmissionImageConfig(
	cfg SubmissionImageConfig,
) error {

	if strings.TrimSpace(cfg.SubmissionID) == "" {
		return errors.New(
			"submission_id is required",
		)
	}

	if strings.TrimSpace(cfg.StrategyPath) == "" {
		return errors.New(
			"strategy_path is required",
		)
	}

	info, err := os.Stat(cfg.StrategyPath)
	if err != nil {
		return fmt.Errorf(
			"strategy binary validation failed: %w",
			err,
		)
	}

	if info.IsDir() {
		return errors.New(
			"strategy_path cannot be a directory",
		)
	}

	return nil
}

func buildGuestBootstrapScript() string {
	return `#!/bin/sh
echo "[GUEST SBOX] Bootstrap initialization successful."
echo "--------------------------------------------------------"

if [ -f ./strategy_bin ]; then
    chmod +x ./strategy_bin
    echo "[GUEST SBOX] Executing strategy binary..."
    ./strategy_bin
    echo "[GUEST SBOX] Binary Exit Code: $?"
else
    echo "[GUEST SBOX] Error: No strategy_bin payload discovered."
fi

echo "--------------------------------------------------------"
echo "[GUEST SBOX] Entering teardown safety hold window..."
sleep 10
reboot -f
`
}

func copyFilesIntoMount(mntDir string, files map[string]string) error {
	for guestName, hostPath := range files {
		content, err := os.ReadFile(hostPath)
		if err != nil {
			return fmt.Errorf("failed reading %s: %w", hostPath, err)
		}
		dst := filepath.Join(mntDir, guestName)
		if err := os.WriteFile(dst, content, 0755); err != nil {
			return fmt.Errorf("failed writing %s into image: %w", guestName, err)
		}
	}
	return nil
}

func runDiskCommand(
	ctx context.Context,
	name string,
	args ...string,
) error {

	cmd := exec.CommandContext(
		ctx,
		name,
		args...,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {

		return fmt.Errorf(
			"command failed cmd=%s args=%v output=%s err=%w",
			name,
			args,
			string(output),
			err,
		)
	}

	return nil
}