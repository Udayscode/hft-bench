package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"
	"time"
)

const (
	networkCommandTimeout = 5 * time.Second
)

type TAPConfig struct {
	TAPName    string
	BridgeName string
}

func SetupTapInterface(
	parentCtx context.Context,
	cfg TAPConfig,
) error {

	if err := validateTAPConfig(cfg); err != nil {
		return err
	}

	// Prevent duplicate interface collisions
	exists, err := interfaceExists(cfg.TAPName)
	if err != nil {
		return fmt.Errorf(
			"failed checking existing interface %s: %w",
			cfg.TAPName,
			err,
		)
	}

	if exists {
		return fmt.Errorf(
			"tap interface already exists: %s",
			cfg.TAPName,
		)
	}

	ctx, cancel := context.WithTimeout(
		parentCtx,
		networkCommandTimeout,
	)

	defer cancel()

	log.Printf(
		"creating tap interface tap=%s bridge=%s",
		cfg.TAPName,
		cfg.BridgeName,
	)

	// Create TAP device
	if err := runCommand(
		ctx,
		"ip",
		"tuntap",
		"add",
		"dev",
		cfg.TAPName,
		"mode",
		"tap",
	); err != nil {

		return fmt.Errorf(
			"failed creating tap interface %s: %w",
			cfg.TAPName,
			err,
		)
	}

	// Attach to bridge
	if err := runCommand(
		ctx,
		"ip",
		"link",
		"set",
		cfg.TAPName,
		"master",
		cfg.BridgeName,
	); err != nil {

		_ = CleanupTapInterface(
			context.Background(),
			cfg.TAPName,
		)

		return fmt.Errorf(
			"failed attaching tap %s to bridge %s: %w",
			cfg.TAPName,
			cfg.BridgeName,
			err,
		)
	}

	// Bring interface UP
	if err := runCommand(
		ctx,
		"ip",
		"link",
		"set",
		cfg.TAPName,
		"up",
	); err != nil {

		_ = CleanupTapInterface(
			context.Background(),
			cfg.TAPName,
		)

		return fmt.Errorf(
			"failed bringing tap interface up %s: %w",
			cfg.TAPName,
			err,
		)
	}

	log.Printf(
		"tap interface ready tap=%s bridge=%s",
		cfg.TAPName,
		cfg.BridgeName,
	)

	return nil
}

func CleanupTapInterface(
	parentCtx context.Context,
	tapName string,
) error {

	if tapName == "" {
		return errors.New("tap_name is required")
	}

	exists, err := interfaceExists(tapName)
	if err != nil {
		return fmt.Errorf(
			"failed checking tap existence %s: %w",
			tapName,
			err,
		)
	}

	// Cleanup should be idempotent
	if !exists {
		return nil
	}

	ctx, cancel := context.WithTimeout(
		parentCtx,
		networkCommandTimeout,
	)

	defer cancel()

	log.Printf(
		"deleting tap interface tap=%s",
		tapName,
	)

	if err := runCommand(
		ctx,
		"ip",
		"link",
		"delete",
		tapName,
	); err != nil {

		return fmt.Errorf(
			"failed deleting tap interface %s: %w",
			tapName,
			err,
		)
	}

	log.Printf(
		"tap interface deleted tap=%s",
		tapName,
	)

	return nil
}

func GenerateIPFromIndex(index int) (string, error) {

	if index <= 0 {
		return "", errors.New(
			"index must be greater than 0",
		)
	}

	if index > 254 {
		return "", errors.New(
			"index exceeds available subnet range",
		)
	}

	ip := fmt.Sprintf(
		"172.16.0.%d",
		index,
	)

	parsed := net.ParseIP(ip)
	if parsed == nil {
		return "", fmt.Errorf(
			"generated invalid ip address: %s",
			ip,
		)
	}

	return ip, nil
}

func interfaceExists(name string) (bool, error) {

	interfaces, err := net.Interfaces()
	if err != nil {
		return false, err
	}

	for _, iface := range interfaces {
		if iface.Name == name {
			return true, nil
		}
	}

	return false, nil
}

func validateTAPConfig(cfg TAPConfig) error {

	if strings.TrimSpace(cfg.TAPName) == "" {
		return errors.New("tap_name is required")
	}

	if strings.TrimSpace(cfg.BridgeName) == "" {
		return errors.New("bridge_name is required")
	}

	return nil
}

func runCommand(
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
