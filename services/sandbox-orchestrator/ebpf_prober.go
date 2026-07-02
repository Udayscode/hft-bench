package main

// ebpf_prober.go — TC eBPF latency prober lifecycle manager.
//
// Design constraints honored:
//   1. The existing TBF root qdisc on egress (set in SetupTapInterface) is
//      LEFT INTACT. The clsact qdisc is an independent, parallel qdisc that
//      coexists with the TBF because:
//        - clsact attaches to a *separate* virtual hook (not the root chain)
//        - Linux kernel routes clsact ingress/egress hooks independently of
//          the root qdisc scheduler — they are not in the same qdisc tree
//        - Reference: kernel/net/sched/cls_bpf.c, sch_clsact.c
//
//   2. The existing `tc filter` ingress police rule (ffff: handle) also remains
//      untouched. We attach an *additional* BPF filter on the clsact ingress
//      hook, which fires independently.
//
//   3. All prober state is scoped to the VM's lifetime. AttachProber returns
//      a TapProber handle that must be passed to DetachProber during teardown.
//
//   4. The ringbuf map fd stays open in the TapProber for the Go consumer.
//      It does NOT need to be pinned to /sys/fs/bpf unless the consumer is
//      a separate process. Since both live in the orchestrator, the fd suffices.

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/vishvananda/netlink"

	tc "hft-core/sandbox-orchestrator/ebpf/tc"
)

// tapLink is a type union: either a proper link.Link (TCX, kernel ≥ 6.6)
// or a legacyTCLink (netlink BPF filter, kernel < 6.6).
// We avoid implementing link.Link directly because it has an unexported method.
type tapLink struct {
	tcx    link.Link      // non-nil when TCX is available
	legacy *legacyTCLink  // non-nil when falling back to netlink filter
}

func (t *tapLink) Close() error {
	if t.tcx != nil {
		return t.tcx.Close()
	}
	if t.legacy != nil {
		return t.legacy.close()
	}
	return nil
}

// TapProber holds the loaded BPF objects and TC filter links for one TAP interface.
// It is embedded into ActiveVM so it's cleaned up on VM teardown.
type TapProber struct {
	objs     *tc.TcLatencyObjects // compiled BPF maps + programs
	egressL  *tapLink             // TC egress hook handle
	ingressL *tapLink             // TC ingress hook handle
	tapName  string
}

// AttachProber loads the compiled BPF programs and attaches them to the
// clsact qdisc on the named TAP interface. Call this immediately after
// SetupTapInterface() returns nil.
//
// The function is idempotent with respect to the clsact qdisc: if one
// already exists (e.g. from a previous crashed run), it is reused.
func AttachProber(tapName string) (*TapProber, error) {
	// 1. Resolve interface index
	iface, err := net.InterfaceByName(tapName)
	if err != nil {
		return nil, fmt.Errorf("ebpf prober: interface %s not found: %w", tapName, err)
	}

	// 2. Ensure clsact qdisc is present.
	//    clsact is a no-op classful qdisc that provides the ingress+egress BPF hooks.
	//    It is *independent* of the root TBF qdisc and does not replace it.
	if err := ensureClsactQdisc(iface.Index, tapName); err != nil {
		return nil, err
	}

	// 3. Ensure the BPF pin directory exists.
	pinDir := fmt.Sprintf("/sys/fs/bpf/hft-bench/%s", tapName)
	if err := os.MkdirAll(pinDir, 0700); err != nil {
		log.Printf("[ebpf-prober] warn: cannot create pin dir %s: %v (maps will not be pinned)", pinDir, err)
		pinDir = "" // disable pinning if the dir can't be created
	}

	// 4. Load compiled BPF objects (programs + maps) from the embedded ELF.
	//    bpf2go embeds the .o file at build time via go:embed.
	objs := &tc.TcLatencyObjects{}
	loadOpts := &ebpf.CollectionOptions{}
	if pinDir != "" {
		loadOpts.Maps = ebpf.MapOptions{PinPath: pinDir}
	}
	if err := tc.LoadTcLatencyObjects(objs, loadOpts); err != nil {
		return nil, fmt.Errorf("ebpf prober: loading BPF objects for %s: %w", tapName, err)
	}

	// 5. Attach tc_egress to the clsact egress hook (TCX preferred, netlink fallback)
	egressL, err := attachBPFProg(iface.Index, objs.TcEgress, ebpf.AttachTCXEgress, netlink.HANDLE_MIN_EGRESS)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("ebpf prober: attaching egress prog to %s: %w", tapName, err)
	}

	// 6. Attach tc_ingress to the clsact ingress hook
	ingressL, err := attachBPFProg(iface.Index, objs.TcIngress, ebpf.AttachTCXIngress, netlink.HANDLE_MIN_INGRESS)
	if err != nil {
		_ = egressL.Close()
		objs.Close()
		return nil, fmt.Errorf("ebpf prober: attaching ingress prog to %s: %w", tapName, err)
	}

	log.Printf("[ebpf-prober] attached tap=%s egress+ingress TC hooks", tapName)

	return &TapProber{
		objs:     objs,
		egressL:  egressL,
		ingressL: ingressL,
		tapName:  tapName,
	}, nil
}

// DetachProber removes all BPF filters and releases all kernel resources.
// Safe to call with a nil pointer. Always call before CleanupTapInterface.
func DetachProber(p *TapProber) {
	if p == nil {
		return
	}

	if p.ingressL != nil {
		if err := p.ingressL.Close(); err != nil {
			log.Printf("[ebpf-prober] warn: detaching ingress link tap=%s: %v", p.tapName, err)
		}
	}

	if p.egressL != nil {
		if err := p.egressL.Close(); err != nil {
			log.Printf("[ebpf-prober] warn: detaching egress link tap=%s: %v", p.tapName, err)
		}
	}

	if p.objs != nil {
		p.objs.Close()
	}

	log.Printf("[ebpf-prober] detached tap=%s", p.tapName)
}

// RingBufMap returns the underlying latency ring buffer map.
// The consumer (ProbeConsumer) holds this reference to drain events.
// Lifetime: valid until DetachProber is called.
func (p *TapProber) RingBufMap() *ebpf.Map {
	return p.objs.LatencyRb
}

// DropCount returns the number of ringbuf overflow drops since attach.
// Useful for telemetry health monitoring.
func (p *TapProber) DropCount() (uint64, error) {
	var perCPUValues []uint64
	key := uint32(0)
	if err := p.objs.DropCounter.Lookup(key, &perCPUValues); err != nil {
		return 0, err
	}
	var total uint64
	for _, v := range perCPUValues {
		total += v
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// attachBPFProg tries TCX first (kernel ≥ 6.6), falls back to legacy netlink
// BPF filter (kernel 4.1+) which covers our 5.15 WSL2 deployment.
func attachBPFProg(
	ifIndex int,
	prog *ebpf.Program,
	tcxDir ebpf.AttachType,
	netlinkParent uint32,
) (*tapLink, error) {

	// Try TCX (preferred — kernel ≥ 6.6, uses bpf_link for safe cleanup)
	tcxL, err := link.AttachTCX(link.TCXOptions{
		Interface: ifIndex,
		Program:   prog,
		Attach:    tcxDir,
	})
	if err == nil {
		return &tapLink{tcx: tcxL}, nil
	}

	// Fallback: legacy netlink BPF filter (works on 5.15)
	leg, err := attachLegacyTCFilter(ifIndex, prog, netlinkParent)
	if err != nil {
		return nil, err
	}
	return &tapLink{legacy: leg}, nil
}

// ensureClsactQdisc adds a clsact qdisc to the interface if not already present.
// clsact is the *only* qdisc type that exposes both ingress and egress BPF hooks.
// It is NOT a root qdisc replacement — it attaches at a separate kernel hook point.
func ensureClsactQdisc(ifIndex int, tapName string) error {
	netlinkIface, err := netlink.LinkByIndex(ifIndex)
	if err != nil {
		return fmt.Errorf("ebpf prober: netlink lookup %s: %w", tapName, err)
	}

	qdiscs, err := netlink.QdiscList(netlinkIface)
	if err != nil {
		return fmt.Errorf("ebpf prober: listing qdiscs on %s: %w", tapName, err)
	}

	for _, q := range qdiscs {
		if q.Type() == "clsact" {
			log.Printf("[ebpf-prober] clsact qdisc already present on %s, reusing", tapName)
			return nil
		}
	}

	// Add clsact qdisc. HANDLE_CLSACT is the magic parent constant for clsact.
	clsact := &netlink.GenericQdisc{
		QdiscAttrs: netlink.QdiscAttrs{
			LinkIndex: ifIndex,
			Handle:    netlink.MakeHandle(0xffff, 0),
			Parent:    netlink.HANDLE_CLSACT,
		},
		QdiscType: "clsact",
	}

	if err := netlink.QdiscAdd(clsact); err != nil {
		return fmt.Errorf("ebpf prober: adding clsact qdisc to %s: %w", tapName, err)
	}

	log.Printf("[ebpf-prober] clsact qdisc added to %s", tapName)
	return nil
}

// legacyTCLink manages a BPF filter attached via netlink.
// Compatible with kernel 4.1+ (clsact) and fully functional on 5.15.
type legacyTCLink struct {
	ifIndex int
	parent  uint32
	handle  uint32
}

// attachLegacyTCFilter attaches a BPF program as a TC filter via netlink.
// Equivalent to: tc filter add dev <tap> <parent> bpf da obj <prog>
func attachLegacyTCFilter(ifIndex int, prog *ebpf.Program, parent uint32) (*legacyTCLink, error) {
	filter := &netlink.BpfFilter{
		FilterAttrs: netlink.FilterAttrs{
			LinkIndex: ifIndex,
			Parent:    parent,
			Handle:    netlink.MakeHandle(0, 1),
			Protocol:  0x0003, // ETH_P_ALL
			Priority:  49152,  // tc prio 49152 — well below any real qdisc priority
		},
		Fd:           prog.FD(),
		Name:         prog.String(),
		DirectAction: true, // DA mode: BPF TC_ACT_OK passes packet unchanged
	}

	if err := netlink.FilterAdd(filter); err != nil {
		return nil, fmt.Errorf("netlink FilterAdd parent=%x: %w", parent, err)
	}

	return &legacyTCLink{
		ifIndex: ifIndex,
		parent:  parent,
		handle:  netlink.MakeHandle(0, 1),
	}, nil
}

func (l *legacyTCLink) close() error {
	// Check interface still exists — if it was already deleted, filters are gone too
	if _, err := netlink.LinkByIndex(l.ifIndex); err != nil {
		return nil
	}

	filter := &netlink.BpfFilter{
		FilterAttrs: netlink.FilterAttrs{
			LinkIndex: l.ifIndex,
			Parent:    l.parent,
			Handle:    l.handle,
			Protocol:  0x0003,
			Priority:  49152,
		},
	}

	if err := netlink.FilterDel(filter); err != nil {
		// Non-fatal: interface deletion itself cleans up all filters
		log.Printf("[ebpf-prober] FilterDel warn parent=%x: %v", l.parent, err)
	}
	return nil
}
