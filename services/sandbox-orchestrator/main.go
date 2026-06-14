package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "hft-core/sandbox-orchestrator/internal/api/proto"

	firecracker "github.com/firecracker-microvm/firecracker-go-sdk"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	serverAddress     = ":50051"
	bootTimeout       = 20 * time.Second
	teardownTimeout   = 10 * time.Second
	kernelImagePath   = "../../sandbox/vmlinux.bin"
	firecrackerBinary = "../../sandbox/firecracker-bin"
	socketDirectory   = "/tmp"
)

type ActiveVM struct {
	Machine        *firecracker.Machine
	VMID           string
	BootTime       time.Time
	IPAddress      string
	TapName        string
	SubmissionDisk string // Track target file for complete automated garbage collection cleanups
	VsockPath      string // Host-side Unix Domain Socket path for the VSOCK device
}

type server struct {
	pb.UnimplementedVMControllerServer

	mu sync.RWMutex

	vmRegistry map[string]*ActiveVM
}

func newServer() *server {
	return &server{
		vmRegistry: make(map[string]*ActiveVM),
	}
}

func (s *server) SpawnVM(
	ctx context.Context,
	req *pb.SpawnRequest,
) (*pb.SpawnResponse, error) {

	if err := validateSpawnRequest(req); err != nil {
		return nil, err
	}

	vmID := req.SubmissionId

	log.Printf(
		"spawn request vm_id=%s vcpus=%d memory_mib=%d",
		vmID,
		req.VcpuCount,
		req.MemoryMib,
	)

	// Thread-safe existence check and tentative registration to reserve IP/ID
	s.mu.Lock()
	_, exists := s.vmRegistry[vmID]
	if exists {
		s.mu.Unlock()
		return &pb.SpawnResponse{
			VmId:         "",
			IpAddress:    "",
			Success:      false,
			ErrorMessage: "vm already exists",
		}, nil
	}

	ipIndex, err := s.findFreeIPIndex()
	if err != nil {
		s.mu.Unlock()
		return nil, status.Error(codes.Internal, "IP allocation failed: "+err.Error())
	}

	computedIP, err := GenerateIPFromIndex(ipIndex)
	if err != nil {
		s.mu.Unlock()
		return nil, status.Error(codes.Internal, "IP generation limits exceeded: "+err.Error())
	}

	// Tentatively register the VM to reserve the IP address
	s.vmRegistry[vmID] = &ActiveVM{
		VMID:      vmID,
		IPAddress: computedIP,
	}
	s.mu.Unlock()

	bootCtx, cancel := context.WithTimeout(
		ctx,
		bootTimeout,
	)
	defer cancel()

	// TAP interface name must be ≤15 chars (Linux IFNAMSIZ-1 limit).
	// Use FNV-32 hash of vmID to generate a deterministic 11-char name:
	// "tap" + 8 hex digits = 11 chars, always within limit.
	computedTap := tapNameForVM(vmID)

	tapCfg := TAPConfig{
		TAPName:    computedTap,
		BridgeName: "br0",
	}

	if err := SetupTapInterface(bootCtx, tapCfg); err != nil {
		s.mu.Lock()
		delete(s.vmRegistry, vmID)
		s.mu.Unlock()
		return nil, status.Error(codes.Internal, "host networking layer provisioning failed: "+err.Error())
	}

	diskCfg := SubmissionImageConfig{
		SubmissionID: vmID,
		StrategyPath: "../../sandbox/strategy_bin",
	}

	computedDisk, err := CreateSubmissionImage(bootCtx, diskCfg)
	if err != nil {
		_ = CleanupTapInterface(context.Background(), computedTap)
		s.mu.Lock()
		delete(s.vmRegistry, vmID)
		s.mu.Unlock()
		return nil, status.Error(codes.Internal, "submission disk provisioning failed: "+err.Error())
	}

	cfg := VMConfig{
		VMID:            vmID,
		VCPUCount:       int64(req.VcpuCount),
		MemoryMiB:       int64(req.MemoryMib),
		KernelImagePath: kernelImagePath,
		FirecrackerBin:  firecrackerBinary,
		SocketDir:       socketDirectory,
		TapName:         computedTap,
		AllocatedIP:     computedIP,
		SubmissionDisk:  computedDisk,
		CID:             uint32(ipIndex + 10),
	}

	machine, err := CreateAndBootVM(bootCtx, &cfg)
	if err != nil {
		log.Printf(
			"spawn failed vm_id=%s error=%v",
			vmID,
			err,
		)
		_ = CleanupTapInterface(context.Background(), computedTap)
		s.mu.Lock()
		delete(s.vmRegistry, vmID)
		s.mu.Unlock()

		return &pb.SpawnResponse{
			VmId:         "",
			IpAddress:    "",
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	activeVM := &ActiveVM{
		Machine:        machine,
		VMID:           vmID,
		BootTime:       time.Now().UTC(),
		IPAddress:      computedIP,
		TapName:        computedTap,
		SubmissionDisk: computedDisk,
		VsockPath:      cfg.VsockPath,
	}

	s.mu.Lock()
	s.vmRegistry[vmID] = activeVM
	s.mu.Unlock()

	log.Printf(
		"spawn successful vm_id=%s ip=%s vsock=%s",
		vmID,
		activeVM.IPAddress,
		activeVM.VsockPath,
	)

	return &pb.SpawnResponse{
		VmId:         "vm-" + vmID,
		IpAddress:    activeVM.IPAddress,
		Success:      true,
		ErrorMessage: "",
		VsockPath:    activeVM.VsockPath,
	}, nil
}

func (s *server) TeardownVM(
	ctx context.Context,
	req *pb.TeardownRequest,
) (*pb.TeardownResponse, error) {

	if req.VmId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"vm_id is required",
		)
	}

	vmID := strings.TrimPrefix(req.VmId, "vm-")

	log.Printf(
		"teardown request vm_id=%s force=%v",
		vmID,
		req.Force,
	)

	s.mu.RLock()
	activeVM, exists := s.vmRegistry[vmID]
	s.mu.RUnlock()

	if !exists {
		return &pb.TeardownResponse{
			Success:      false,
			ErrorMessage: "vm not found",
		}, nil
	}

	stopCtx, cancel := context.WithTimeout(
		ctx,
		teardownTimeout,
	)

	defer cancel()

	_ = stopCtx

	if err := activeVM.Machine.StopVMM(); err != nil {
		log.Printf(
			"teardown failed vm_id=%s error=%v",
			vmID,
			err,
		)
		return &pb.TeardownResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	s.mu.Lock()
	tapToCleanup := activeVM.TapName
	delete(s.vmRegistry, vmID)
	s.mu.Unlock()

	if err := CleanupTapInterface(context.Background(), tapToCleanup); err != nil {
		log.Printf("[Network Warning] Failed purging link interface %s: %v", tapToCleanup, err)
	}

	if err := CleanupSubmissionImage(activeVM.SubmissionDisk); err != nil {
		log.Printf("[Storage Warning] Failed purging scratch image file %s: %v", activeVM.SubmissionDisk, err)
	} else {
		log.Printf("[Storage Cleanup] MicroVM ephemeral disk %s completely purged from host storage layer.", activeVM.SubmissionDisk)
	}

	log.Printf(
		"teardown successful vm_id=%s",
		vmID,
	)

	return &pb.TeardownResponse{
		Success:      true,
		ErrorMessage: "",
	}, nil
}

func validateSpawnRequest(
	req *pb.SpawnRequest,
) error {

	if req.SubmissionId == "" {
		return status.Error(
			codes.InvalidArgument,
			"submission_id is required",
		)
	}

	if req.VcpuCount <= 0 {
		return status.Error(
			codes.InvalidArgument,
			"vcpu_count must be greater than 0",
		)
	}

	if req.MemoryMib <= 0 {
		return status.Error(
			codes.InvalidArgument,
			"memory_mib must be greater than 0",
		)
	}

	return nil
}

func (s *server) shutdownAllVMs() {

	s.mu.Lock()
	defer s.mu.Unlock()

	for vmID, activeVM := range s.vmRegistry {

		log.Printf(
			"shutdown cleanup vm_id=%s",
			vmID,
		)
		
		tapToCleanup := activeVM.TapName
		if err := activeVM.Machine.StopVMM(); err != nil {

			log.Printf(
				"cleanup graceful stop failed vm_id=%s error=%v",
				vmID,
				err,
			)
		}

		delete(s.vmRegistry, vmID)
		_ = CleanupTapInterface(context.Background(), tapToCleanup)

		diskToCleanup := activeVM.SubmissionDisk

		_ = CleanupTapInterface(context.Background(), tapToCleanup)
		_ = CleanupSubmissionImage(diskToCleanup)
	}
}

func main() {

	lis, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf(
			"failed to listen on %s: %v",
			serverAddress,
			err,
		)
	}

	grpcServer := grpc.NewServer()

	srv := newServer()

	pb.RegisterVMControllerServer(
		grpcServer,
		srv,
	)

	log.Printf(
		"sandbox orchestrator listening on %s",
		serverAddress,
	)

	go func() {

		if err := grpcServer.Serve(lis); err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) {

			log.Fatalf(
				"grpc server failed: %v",
				err,
			)
		}
	}()

	stop := make(chan os.Signal, 1)

	signal.Notify(
		stop,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	defer signal.Stop(stop)

	<-stop

	log.Println("shutdown signal received")

	grpcServer.GracefulStop()

	log.Println("cleaning up active microvms")

	srv.shutdownAllVMs()

	log.Println("orchestrator shutdown complete")
}

// tapNameForVM returns a deterministic TAP interface name for a given vmID.
// Linux kernel limits network interface names to IFNAMSIZ-1 = 15 characters.
// We use FNV-32a hash of the vmID to produce "tap" + 8 hex chars = 11 chars,
// which is always within the limit regardless of submission_id length.
func tapNameForVM(vmID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(vmID))
	return fmt.Sprintf("tap%08x", h.Sum32())
}

// findFreeIPIndex scans the vmRegistry for occupied IP addresses and returns the first free index between 2 and 254.
// Must be called with s.mu Lock held.
func (s *server) findFreeIPIndex() (int, error) {
	used := make(map[int]bool)
	for _, vm := range s.vmRegistry {
		if vm != nil && vm.IPAddress != "" {
			var idx int
			_, err := fmt.Sscanf(vm.IPAddress, "172.16.0.%d", &idx)
			if err == nil {
				used[idx] = true
			}
		}
	}
	for i := 2; i <= 254; i++ {
		if !used[i] {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no free IP addresses available in subnet 172.16.0.0/24")
}

