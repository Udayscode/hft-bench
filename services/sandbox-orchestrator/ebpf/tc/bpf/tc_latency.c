// SPDX-License-Identifier: GPL-2.0
//
// tc_latency.c — BPF_PROG_TYPE_SCHED_CLS TC classifier for nanosecond-precision
// round-trip latency measurement on Firecracker TAP interfaces.
//
// Attach model (per TAP device):
//   clsact qdisc → egress hook  → tc_egress  (host→VM, stamp send time)
//   clsact qdisc → ingress hook → tc_ingress (VM→host, compute delta, emit)
//
// Matching key: (src_ip, dst_ip, src_port, dst_port, tcp_seq)
// Only TCP non-SYN/FIN/RST data packets are tracked to avoid protocol
// overhead polluting the data-plane latency distribution.

#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// ---------------------------------------------------------------------------
// Compatibility shims for Ubuntu 20.04 linux-libc-dev (kernel 5.4 headers).
// These are stable kernel/IANA constants that will never change.
// ---------------------------------------------------------------------------

// BPF_MAP_TYPE_RINGBUF was added to linux/bpf.h in kernel 5.8.
// The numeric value 27 is frozen in the kernel uapi — safe to define manually.
#ifndef BPF_MAP_TYPE_RINGBUF
#define BPF_MAP_TYPE_RINGBUF 27
#endif

// IPPROTO_TCP is defined in <linux/in.h> which is not pulled in by the BPF
// include chain on older kernels. Value 6 is permanent IANA assignment.
#ifndef IPPROTO_TCP
#define IPPROTO_TCP 6
#endif

// ---------------------------------------------------------------------------
// Structures
// ---------------------------------------------------------------------------

// flow_key uniquely identifies one in-flight TCP segment.
// Packed to 16 bytes — fits in a single cache line alongside the map value.
struct flow_key {
    __u32 src_ip;
    __u32 dst_ip;
    __u16 src_port;
    __u16 dst_port;
    __u32 seq;      // TCP sequence number — per-segment precision
} __attribute__((packed));

// latency_event is the record emitted to userspace via the ring buffer.
// Fixed 32 bytes — exactly half a cache line, maximises ring buffer density.
struct latency_event {
    __u64 egress_ts_ns;  // absolute egress timestamp (for drift correlation)
    __u64 latency_ns;    // round-trip delta
    __u32 src_ip;
    __u32 dst_ip;
    __u16 src_port;
    __u16 dst_port;
    __u32 seq;
} __attribute__((packed));

// ---------------------------------------------------------------------------
// BPF Maps
// ---------------------------------------------------------------------------

// Egress timestamp store keyed by flow_key.
// LRU eviction handles lost/unanswered packets cleanly without a GC thread.
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 65536);
    __type(key,   struct flow_key);
    __type(value, __u64);           // egress timestamp (ns)
} ts_store SEC(".maps");

// Lock-free, mmap'd ring buffer — zero-copy handoff to Go consumer.
// 256KB is enough for ~8192 outstanding events before consumer must drain.
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 262144);    // 256KB
} latency_rb SEC(".maps");

// Drop counter — userspace can monitor this for ringbuf overflow.
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key,   __u32);
    __type(value, __u64);
} drop_counter SEC(".maps");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Parse ethernet + IP + TCP headers. Returns 0 on success, -1 to skip.
// All offsets are checked against data_end to satisfy the BPF verifier.
static __always_inline int parse_headers(
    struct __sk_buff *skb,
    struct iphdr    **ip_out,
    struct tcphdr   **tcp_out
) {
    void *data     = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    // Ethernet header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return -1;

    // Only handle IPv4
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return -1;

    // IP header
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return -1;

    // Only handle TCP
    if (ip->protocol != IPPROTO_TCP)
        return -1;

    // Variable-length IP header — ihl is in 32-bit words
    __u32 ip_hlen = ip->ihl * 4;
    if (ip_hlen < 20)
        return -1;

    struct tcphdr *tcp = (void *)ip + ip_hlen;
    if ((void *)(tcp + 1) > data_end)
        return -1;

    *ip_out  = ip;
    *tcp_out = tcp;
    return 0;
}

static __always_inline void inc_drop_counter(void) {
    __u32 key = 0;
    __u64 *val = bpf_map_lookup_elem(&drop_counter, &key);
    if (val)
        __sync_fetch_and_add(val, 1);
}

// ---------------------------------------------------------------------------
// TC Egress — host→VM (packet leaving the host toward the microVM)
// Stamp the send timestamp into ts_store keyed by the TCP 5-tuple + seq.
// ---------------------------------------------------------------------------
SEC("tc/egress")
int tc_egress(struct __sk_buff *skb) {
    struct iphdr  *ip;
    struct tcphdr *tcp;

    if (parse_headers(skb, &ip, &tcp) < 0)
        return TC_ACT_OK;

    // Filter: skip SYN, FIN, RST — only measure data-plane segments.
    // This avoids connection setup/teardown noise in the latency distribution.
    __u8 flags = ((__u8 *)tcp)[13]; // TCP flags byte at offset 13
    if (flags & (0x02 | 0x01 | 0x04)) // SYN=0x02, FIN=0x01, RST=0x04
        return TC_ACT_OK;

    struct flow_key key = {
        .src_ip   = ip->saddr,
        .dst_ip   = ip->daddr,
        .src_port = tcp->source,
        .dst_port = tcp->dest,
        .seq      = tcp->seq,
    };

    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&ts_store, &key, &ts, BPF_ANY);

    return TC_ACT_OK;
}

// ---------------------------------------------------------------------------
// TC Ingress — VM→host (response packet arriving from inside the microVM)
// Look up egress timestamp, compute delta, emit to ring buffer.
// ---------------------------------------------------------------------------
SEC("tc/ingress")
int tc_ingress(struct __sk_buff *skb) {
    struct iphdr  *ip;
    struct tcphdr *tcp;

    if (parse_headers(skb, &ip, &tcp) < 0)
        return TC_ACT_OK;

    // Same flag filter — skip control segments
    __u8 flags = ((__u8 *)tcp)[13];
    if (flags & (0x02 | 0x01 | 0x04))
        return TC_ACT_OK;

    // On ingress (response), the src/dst are *reversed* relative to egress.
    // The original egress key had: src=host, dst=vm.
    // The response has:            src=vm,   dst=host.
    // We must reconstruct the original egress key by swapping.
    //
    // Also: for TCP, the response ACK number corresponds to egress seq+payload_len.
    // We match on ack_seq - 1 to correlate back to the original data segment's seq.
    // This is the most reliable correlation strategy without L7 parsing.
    struct flow_key key = {
        .src_ip   = ip->daddr,   // swapped: original egress src
        .dst_ip   = ip->saddr,   // swapped: original egress dst
        .src_port = tcp->dest,   // swapped
        .dst_port = tcp->source, // swapped
        .seq      = bpf_ntohl(bpf_ntohl(tcp->ack_seq) - 1),
    };

    __u64 *egress_ts = bpf_map_lookup_elem(&ts_store, &key);
    if (!egress_ts)
        return TC_ACT_OK; // No matching egress — not our packet or already evicted

    __u64 now       = bpf_ktime_get_ns();
    __u64 latency   = now - *egress_ts;
    __u64 ts_copy   = *egress_ts;

    // Delete the entry — one response consumes one request slot (no double-counting)
    bpf_map_delete_elem(&ts_store, &key);

    // Sanity gate: discard sub-100ns deltas (likely kernel loopback noise) and
    // anything above 100ms (likely a retransmit matching a stale entry).
    if (latency < 100 || latency > 100000000ULL)
        return TC_ACT_OK;

    // Reserve a ring buffer slot — non-blocking, returns NULL if full.
    struct latency_event *evt = bpf_ringbuf_reserve(&latency_rb, sizeof(*evt), 0);
    if (!evt) {
        inc_drop_counter();
        return TC_ACT_OK;
    }

    evt->egress_ts_ns = ts_copy;
    evt->latency_ns   = latency;
    evt->src_ip       = key.src_ip;
    evt->dst_ip       = key.dst_ip;
    evt->src_port     = key.src_port;
    evt->dst_port     = key.dst_port;
    evt->seq          = key.seq;

    // Submit — this makes the record visible to userspace with a single
    // store-release barrier. BPF_RB_FORCE_WAKEUP is omitted for throughput;
    // the consumer polls with a timeout to amortize wakeup cost.
    bpf_ringbuf_submit(evt, 0);

    return TC_ACT_OK;
}

char LICENSE[] SEC("license") = "GPL";
