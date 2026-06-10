//! P0 gossipsub router micro-benchmark (`zig build bench-gossipsub`).
//!
//! Measures the ROUTER hot path in-process — protobuf decode, signature
//! verification (inline vs the async worker pipeline), seen-cache dedup, mesh
//! fan-out through real per-peer writer fibers — against a discard transport,
//! so the numbers isolate router cost from the network. Scenarios:
//!
//!   strict/inline     signed messages, verification ON the router fiber
//!                     (validation_concurrency = 0)
//!   strict/async      same load, verification on validation fibers
//!                     (validation_concurrency = executor count) — the P1 lever
//!   strict/async dupx4 every id delivered 4x (relayed by 4 peers): duplicates
//!                     must skip crypto via the seen check
//!   anonymous         no signatures, content-derived ids (the eth2 StrictNoSign
//!                     shape; app/BLS validation is out of scope here)
//!   heartbeat         heartbeat latency with a loaded seen cache, and the
//!                     expiry tick where every id ages out at once (the P4
//!                     sweep cost)
//!
//! Reported per scenario: delivered msgs/s, µs/msg, end-to-end wall time,
//! deliveries vs offered (a shortfall under strict/async = validation throttle
//! drops, go-parity behaviour), forwarded frames+bytes (discard-sink counters),
//! and allocator op counts (allocs/frees per message — the P7 budget).
//!
//! Run with -Doptimize=ReleaseFast for meaningful numbers. The runtime is zio
//! (production parity: epoll/kqueue), executors = clamp(cpus, 2, 64).

const std = @import("std");
const zio = @import("zio");
const router_mod = @import("protocols/pubsub/router.zig");
const pubsub = @import("protocols/pubsub/pubsub.zig");
const signing = @import("protocols/pubsub/signing.zig");
const rpc = @import("protocols/pubsub/rpc.zig");
const rpc_pb = @import("protobuf.zig").rpc;
const identity = @import("identity.zig");
const io_time = @import("quic/io/time.zig");
const PeerId = @import("peer_id").PeerId;

const topic = "bench";
/// Mesh peers (all grafted). Every accepted message fans out to mesh-1 sinks.
const mesh_peers = 8;
/// Messages per throughput scenario.
const default_msgs: usize = 20_000;
/// Payload size: the eth2 attestation ballpark (well under the 1 KiB IDONTWANT
/// threshold, so duplicates get no IDONTWANT relief — the worst case P1 targets).
const payload_len: usize = 256;
/// Seen-cache load for the heartbeat scenario.
const heartbeat_seen_load: usize = 100_000;

// ---------------------------------------------------------------------------
// Counting allocator: atomic op/byte counters around a parent allocator, so a
// scenario can report allocs-per-message (multi-fiber safe).
// ---------------------------------------------------------------------------

const CountingAllocator = struct {
    parent: std.mem.Allocator,
    allocs: std.atomic.Value(u64) = .init(0),
    frees: std.atomic.Value(u64) = .init(0),
    bytes: std.atomic.Value(u64) = .init(0),

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        _ = self.allocs.fetchAdd(1, .monotonic);
        _ = self.bytes.fetchAdd(len, .monotonic);
        return self.parent.vtable.alloc(self.parent.ptr, len, alignment, ret_addr);
    }
    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        return self.parent.vtable.resize(self.parent.ptr, memory, alignment, new_len, ret_addr);
    }
    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        return self.parent.vtable.remap(self.parent.ptr, memory, alignment, new_len, ret_addr);
    }
    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        _ = self.frees.fetchAdd(1, .monotonic);
        self.parent.vtable.free(self.parent.ptr, memory, alignment, ret_addr);
    }

    fn snapshot(self: *const CountingAllocator) [3]u64 {
        return .{ self.allocs.load(.monotonic), self.frees.load(.monotonic), self.bytes.load(.monotonic) };
    }
};

// ---------------------------------------------------------------------------
// Discard transport: real Router machinery (per-peer writer fibers, lanes,
// frames) writing into counting /dev/null sinks.
// ---------------------------------------------------------------------------

const SinkCounters = struct {
    frames: std.atomic.Value(u64) = .init(0),
    bytes: std.atomic.Value(u64) = .init(0),
};

const BenchConn = struct { dummy: u8 = 0 };

const DiscardSink = struct {
    counters: *SinkCounters,

    pub fn open(self: *DiscardSink, io: std.Io) anyerror!void {
        _ = self;
        _ = io;
    }
    pub fn writeFrame(self: *DiscardSink, io: std.Io, bytes: []const u8) anyerror!void {
        _ = io;
        _ = self.counters.frames.fetchAdd(1, .monotonic);
        _ = self.counters.bytes.fetchAdd(bytes.len, .monotonic);
    }
    pub fn close(self: *DiscardSink, io: std.Io) void {
        _ = self;
        _ = io;
    }
};

const BenchTransport = struct {
    counters: *SinkCounters,

    pub const ConnHandle = *BenchConn;
    pub const Sink = DiscardSink;

    pub fn makeSink(self: *BenchTransport, allocator: std.mem.Allocator, peer: PeerId, conn: ConnHandle) !*DiscardSink {
        _ = peer;
        _ = conn;
        const sink = try allocator.create(DiscardSink);
        sink.* = .{ .counters = self.counters };
        return sink;
    }

    pub fn dial(self: *BenchTransport, io: std.Io, addr: []const u8) void {
        _ = self;
        _ = io;
        _ = addr;
    }
};

const BenchRouter = router_mod.Router(BenchTransport);

const DeliveryCounter = struct {
    delivered: std.atomic.Value(u64) = .init(0),

    fn handler(self: *DeliveryCounter) router_mod.MessageHandler {
        return .{ .ctx = self, .on_message = onMessage };
    }
    fn onMessage(ctx: *anyopaque, t: []const u8, from: []const u8, data: []const u8) void {
        _ = t;
        _ = from;
        _ = data;
        const self: *DeliveryCounter = @ptrCast(@alignCast(ctx));
        _ = self.delivered.fetchAdd(1, .monotonic);
    }
};

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

fn ownedFromRpc(allocator: std.mem.Allocator, frame: rpc_pb.RPC) !pubsub.OwnedRpc {
    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);
    const payload = try allocator.dupe(u8, encoded);
    errdefer allocator.free(payload);
    return .{ .bytes = payload, .reader = try rpc_pb.RPCReader.init(payload) };
}

fn syncRouter(r: *BenchRouter, io: std.Io) void {
    var reply: std.Io.Event = .unset;
    r.inbox.putOneUncancelable(io, .{ .sync = .{ .reply = &reply } }) catch return;
    reply.waitUncancelable(io);
}

/// Pre-encoded inbound publish frames plus the relayer assigned to each.
const Workload = struct {
    rpcs: []pubsub.OwnedRpc,
    relayer: []usize,
    unique: usize,

    fn deinit(w: *Workload, allocator: std.mem.Allocator) void {
        // Posted rpcs are owned (and freed) by the router; this frees only a
        // workload that was never posted. After a run, call forget() instead.
        allocator.free(w.rpcs);
        allocator.free(w.relayer);
    }
};

/// Build `count` publishes with `dup_factor` copies of each unique message
/// (dup copies are byte-identical, attributed to different relayers — the mesh
/// re-delivery shape). Under `signer != null` each unique message is signed by
/// the origin (setup cost, not measured).
fn buildWorkload(
    allocator: std.mem.Allocator,
    signer: ?*const signing.Signer,
    count: usize,
    dup_factor: usize,
    seed: u64,
) !Workload {
    const unique = count / dup_factor;
    const rpcs = try allocator.alloc(pubsub.OwnedRpc, unique * dup_factor);
    const relayer = try allocator.alloc(usize, unique * dup_factor);
    var payload: [payload_len]u8 = undefined;
    @memset(&payload, 0xab);
    var idx: usize = 0;
    for (0..unique) |u| {
        var seqno_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &seqno_buf, seed + u, .big);
        std.mem.writeInt(u64, payload[0..8], seed + u, .big);
        if (signer) |s| {
            const sig = try s.sign(s.fromBytes(), &seqno_buf, topic, &payload);
            defer allocator.free(sig);
            for (0..dup_factor) |d| {
                const msg = rpc_pb.Message{
                    .from = s.fromBytes(),
                    .seqno = &seqno_buf,
                    .topic = topic,
                    .data = &payload,
                    .signature = sig,
                    .key = s.keyBytes(),
                };
                rpcs[idx] = try ownedFromRpc(allocator, .{ .publish = &[_]?rpc_pb.Message{msg} });
                relayer[idx] = (u + d) % mesh_peers;
                idx += 1;
            }
        } else {
            for (0..dup_factor) |d| {
                const msg = rpc_pb.Message{ .topic = topic, .data = &payload };
                rpcs[idx] = try ownedFromRpc(allocator, .{ .publish = &[_]?rpc_pb.Message{msg} });
                relayer[idx] = (u + d) % mesh_peers;
                idx += 1;
            }
        }
    }
    return .{ .rpcs = rpcs, .relayer = relayer, .unique = unique };
}

const Scenario = struct {
    name: []const u8,
    /// null host key = anonymous policy.
    host_key: ?*const identity.KeyPair,
    validation_concurrency: usize,
    dup_factor: usize,
    msgs: usize,
    /// Paced (closed-loop): post in batches of the concurrency cap with a sync
    /// barrier between batches, so verdict re-entries are never starved behind
    /// queued ingress — measures the pipeline's sustained validated capacity.
    /// Unpaced (open-loop) posts everything at memcpy speed and measures burst
    /// behaviour: with the FIFO inbox, queued ingress delays the verdicts that
    /// free in-flight slots, so most of an unpaced burst throttle-drops (this
    /// coupling is itself a finding — go's workers complete without the
    /// processLoop's help).
    paced: bool = false,
};

const ScenarioResult = struct {
    name: []const u8,
    offered: usize,
    unique: usize,
    delivered: u64,
    wall_ns: u64,
    frames: u64,
    frame_bytes: u64,
    allocs: u64,
    frees: u64,
};

fn runScenario(
    counting: *CountingAllocator,
    io: std.Io,
    origin_signer: ?*const signing.Signer,
    sc: Scenario,
) !ScenarioResult {
    const allocator = counting.allocator();
    var counters = SinkCounters{};
    var deliveries = DeliveryCounter{};
    const transport = BenchTransport{ .counters = &counters };

    const local_peer = try PeerId.random();
    const cfg: router_mod.RouterConfig = if (sc.host_key == null)
        .{ .signature_policy = .anonymous, .validation_concurrency = sc.validation_concurrency }
    else
        .{ .validation_concurrency = sc.validation_concurrency };
    const r = try BenchRouter.create(allocator, io, transport, local_peer, deliveries.handler(), 0, sc.host_key, null, cfg);
    var destroyed = false;
    defer if (!destroyed) r.destroy();
    try r.start();

    // Subscribe + connect/graft the mesh peers (setup, untimed).
    {
        const t = try allocator.dupe(u8, topic);
        try r.inbox.putOne(io, .{ .subscribe = .{ .topic = t } });
    }
    var conns: [mesh_peers]BenchConn = undefined;
    var peers: [mesh_peers]PeerId = undefined;
    for (0..mesh_peers) |i| {
        conns[i] = .{};
        peers[i] = try PeerId.random();
        try r.inbox.putOne(io, .{ .peer_connected = .{
            .peer = peers[i],
            .conn = &conns[i],
            .remote_addr = .{ .ip4 = .loopback(@intCast(4000 + i)) },
        } });
        const graft = rpc_pb.ControlMessage{ .graft = &[_]?rpc_pb.ControlGraft{rpc.buildGraft(topic)} };
        try r.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = peers[i],
            .rpc = try ownedFromRpc(allocator, .{ .control = graft }),
        } });
    }
    syncRouter(r, io);

    // Build the workload up front (signing/encoding is setup cost).
    var workload = try buildWorkload(allocator, origin_signer, sc.msgs, sc.dup_factor, 0x9000_0000);
    const offered = workload.rpcs.len;

    const alloc_before = counting.snapshot();
    const t0 = io_time.monotonicNs(io);
    if (sc.paced) {
        const batch = @max(sc.validation_concurrency, 1);
        var i: usize = 0;
        while (i < workload.rpcs.len) {
            const end = @min(i + batch, workload.rpcs.len);
            for (workload.rpcs[i..end], workload.relayer[i..end]) |owned, who| {
                try r.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peers[who], .rpc = owned } });
            }
            syncRouter(r, io);
            i = end;
        }
    } else {
        for (workload.rpcs, workload.relayer) |owned, who| {
            try r.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peers[who], .rpc = owned } });
        }
        syncRouter(r, io);
    }
    // Async verdicts trail the sync barrier: wait until deliveries stop moving.
    var last = deliveries.delivered.load(.acquire);
    var settle_polls: usize = 0;
    while (settle_polls < 200) : (settle_polls += 1) {
        if (last >= workload.unique) break;
        io_time.ms(5).sleep(io) catch break;
        const now = deliveries.delivered.load(.acquire);
        if (now == last) {
            // Two stable polls in a row = the pipeline drained (throttle drops
            // mean delivered < unique and it will never reach it).
            io_time.ms(5).sleep(io) catch break;
            if (deliveries.delivered.load(.acquire) == now) break;
        }
        last = now;
    }
    const t1 = io_time.monotonicNs(io);
    const alloc_after = counting.snapshot();

    // The router consumed (and freed) every posted rpc; free only the arrays.
    workload.deinit(allocator);

    const result = ScenarioResult{
        .name = sc.name,
        .offered = offered,
        .unique = workload.unique,
        .delivered = deliveries.delivered.load(.acquire),
        .wall_ns = t1 - t0,
        .frames = counters.frames.load(.monotonic),
        .frame_bytes = counters.bytes.load(.monotonic),
        .allocs = alloc_after[0] - alloc_before[0],
        .frees = alloc_after[1] - alloc_before[1],
    };
    r.destroy();
    destroyed = true;
    return result;
}

fn printResult(res: ScenarioResult) void {
    const secs = @as(f64, @floatFromInt(res.wall_ns)) / 1e9;
    const delivered_f = @as(f64, @floatFromInt(res.delivered));
    const msgs_per_s = if (secs > 0) delivered_f / secs else 0;
    const us_per_msg = if (res.delivered > 0)
        (@as(f64, @floatFromInt(res.wall_ns)) / 1000.0) / delivered_f
    else
        0;
    const offered_f = @as(f64, @floatFromInt(res.offered));
    const allocs_per = if (res.offered > 0) @as(f64, @floatFromInt(res.allocs)) / offered_f else 0;
    std.debug.print(
        "{s:<22} {d:>9.0} msg/s  {d:>7.2} us/msg  delivered {d}/{d} unique (offered {d})  fwd {d} frames {d:.1} MiB  allocs/offered {d:.1}\n",
        .{ res.name, msgs_per_s, us_per_msg, res.delivered, res.unique, res.offered, res.frames, @as(f64, @floatFromInt(res.frame_bytes)) / (1024.0 * 1024.0), allocs_per },
    );
}

/// Heartbeat latency with a loaded seen cache: load K unique anonymous
/// messages (no peers — pure seen/mcache load), then time (a) steady-state
/// heartbeats (full-map scan, nothing expires) and (b) the expiry tick where
/// every id ages out at once.
fn runHeartbeatScenario(counting: *CountingAllocator, io: std.Io) !void {
    const allocator = counting.allocator();
    var counters = SinkCounters{};
    var deliveries = DeliveryCounter{};
    const transport = BenchTransport{ .counters = &counters };
    const local_peer = try PeerId.random();
    const r = try BenchRouter.create(allocator, io, transport, local_peer, deliveries.handler(), 0, null, null, .{ .signature_policy = .anonymous });
    defer r.destroy();
    try r.start();
    {
        const t = try allocator.dupe(u8, topic);
        try r.inbox.putOne(io, .{ .subscribe = .{ .topic = t } });
    }
    const relay = try PeerId.random();
    var conn = BenchConn{};
    try r.inbox.putOne(io, .{ .peer_connected = .{ .peer = relay, .conn = &conn, .remote_addr = .{ .ip4 = .loopback(5000) } } });
    syncRouter(r, io);

    var workload = try buildWorkload(allocator, null, heartbeat_seen_load, 1, 0xb000_0000);
    for (workload.rpcs) |owned| {
        try r.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = relay, .rpc = owned } });
    }
    syncRouter(r, io);
    workload.deinit(allocator);

    const timedHeartbeat = struct {
        fn run(rt: *BenchRouter, io_: std.Io) u64 {
            const t0 = io_time.monotonicNs(io_);
            rt.inbox.putOneUncancelable(io_, .heartbeat) catch return 0;
            syncRouter(rt, io_);
            return io_time.monotonicNs(io_) - t0;
        }
    }.run;

    std.debug.print("heartbeat, seen={d} ids:\n", .{heartbeat_seen_load});
    for (0..3) |i| {
        std.debug.print("  steady tick {d}: {d:.2} ms\n", .{ i + 1, @as(f64, @floatFromInt(timedHeartbeat(r, io))) / 1e6 });
    }
    // Age the cache to one tick before expiry, untimed (ids were added at the
    // tick the load ran under; they expire seen_ttl ticks later).
    var tick: usize = 3;
    while (tick < 119) : (tick += 1) {
        r.inbox.putOneUncancelable(io, .heartbeat) catch break;
    }
    syncRouter(r, io);
    std.debug.print("  EXPIRY tick (all {d} ids age out): {d:.2} ms\n", .{ heartbeat_seen_load, @as(f64, @floatFromInt(timedHeartbeat(r, io))) / 1e6 });
    std.debug.print("  post-expiry tick: {d:.2} ms\n", .{@as(f64, @floatFromInt(timedHeartbeat(r, io))) / 1e6});
}

pub fn main(init: std.process.Init) !void {
    const base = init.gpa;
    var counting = CountingAllocator{ .parent = base };

    const cpu_count = std.Thread.getCpuCount() catch 2;
    const executor_count: u8 = @intCast(std.math.clamp(cpu_count, 2, 64));
    const runtime = try zio.Runtime.init(base, .{ .executors = .exact(executor_count) });
    defer runtime.deinit();
    const io = runtime.io();

    var origin_key = try identity.KeyPair.generate(.ED25519);
    defer origin_key.deinit();
    var origin_signer = try signing.Signer.init(counting.allocator(), &origin_key);
    defer origin_signer.deinit();
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    std.debug.print(
        "gossipsub bench: executors={d} mesh={d} payload={d}B msgs={d}\n\n",
        .{ executor_count, mesh_peers, payload_len, default_msgs },
    );

    const scenarios = [_]Scenario{
        .{ .name = "strict/inline", .host_key = &host_key, .validation_concurrency = 0, .dup_factor = 1, .msgs = default_msgs },
        .{ .name = "strict/async paced", .host_key = &host_key, .validation_concurrency = executor_count, .dup_factor = 1, .msgs = default_msgs, .paced = true },
        .{ .name = "strict/async burst", .host_key = &host_key, .validation_concurrency = executor_count, .dup_factor = 1, .msgs = default_msgs },
        .{ .name = "strict/async dupx4 paced", .host_key = &host_key, .validation_concurrency = executor_count, .dup_factor = 4, .msgs = default_msgs, .paced = true },
        .{ .name = "anonymous/inline", .host_key = null, .validation_concurrency = 0, .dup_factor = 1, .msgs = default_msgs },
    };
    for (scenarios) |sc| {
        const signer_arg: ?*const signing.Signer = if (sc.host_key != null) &origin_signer else null;
        const res = try runScenario(&counting, io, signer_arg, sc);
        printResult(res);
    }

    std.debug.print("\n", .{});
    try runHeartbeatScenario(&counting, io);
}
