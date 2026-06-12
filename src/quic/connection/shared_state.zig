//! Heap-allocated state shared between a `Connection` handle and its
//! `ConnectionActor` fiber.
//!
//! Holds exactly the things that *both* sides need to touch:
//!   - Channel endpoints (inbox queue, accept queue, optional datagram queue)
//!   - Atomic flags / counters (closed, stats publisher, pending counters)
//!   - The handshake gate (event that callers wait on, actor sets)
//!   - Once-set post-handshake metadata (verified public key, remote addr)
//!
//! Refcount is initialised to 2: one ref held by the handle, one by the
//! actor. Whichever side drops its ref last frees the allocation. There is
//! no per-side teardown ordering — each release is independent.
//!
//! Anything else (quiche_conn, transport, cid registry, stream table,
//! actor bookkeeping) lives in the `ConnectionActor` only and is unreachable
//! from the handle by construction.

const std = @import("std");
const AtomicRc = @import("ref_count").AtomicRc;
const handle_pool = @import("../datagram/handle_pool.zig");
const datagram_queue = @import("../datagram/queue.zig");
const stage_mod = @import("stage.zig");
const connection_stats = @import("stats.zig");
const stream_handle = @import("../stream/handle.zig");
const commands = @import("commands.zig");
const waitset_mod = @import("../io/waitset.zig");

const keys = @import("peer_id").keys;
const Allocator = std.mem.Allocator;

pub const Command = commands.Command;
pub const ConnectionStats = connection_stats.ConnectionStats;
pub const Stream = stream_handle.Stream;
pub const Handshake = stage_mod.Handshake;
pub const WaitSet = waitset_mod.WaitSet;
pub const Ready = waitset_mod.Ready;

pub const InboxQueue = std.Io.Queue(Command);
pub const AcceptQueue = std.Io.Queue(*Stream);
pub const DatagramChannel = datagram_queue.Queue;
pub const DatagramPool = handle_pool.Pool;
/// MPSC queue of stream IDs that handles push to tell the actor "this stream
/// has new outbound bytes." Producers: any thread holding a Stream handle.
/// Consumer: the connection actor.
pub const OutboundPendingQueue = std.Io.Queue(u64);

pub const SharedState = struct {
    allocator: Allocator,
    io: std.Io,
    /// Two holders at birth: one ref for the handle, one for the actor.
    rc: AtomicRc = .initCount(2),

    // Channels (storage owned here; both sides hold pointers to `inbox`,
    // `accept_queue`, etc. but never to other parts of this struct).
    inbox: InboxQueue,
    inbox_buffer: []Command,
    accept_queue: AcceptQueue,
    accept_buffer: []*Stream,
    /// Stream IDs handles have signaled as having new outbound bytes; the
    /// actor pops these and marks the corresponding `StreamRecord` ready.
    /// On overflow producers set `fallback_full_scan` so the actor sweeps
    /// every stream instead.
    outbound_pending_ids: OutboundPendingQueue,
    outbound_pending_buffer: []u64,
    fallback_full_scan: std.atomic.Value(bool) = .init(false),
    datagram_pool: ?*DatagramPool = null,
    datagram_queue: ?*DatagramChannel.State = null,

    // Coarse state visible to handles
    closed: std.atomic.Value(bool) = .init(false),
    max_datagram_payload: std.atomic.Value(usize) = .init(0),
    stats: connection_stats.Publisher = .{},
    handshake: Handshake = .{},

    // Wake mechanism for the actor's main loop. Handles signal this when they
    // post commands or queue outbound stream bytes; the router signals it on
    // packet enqueue. Lives here (not in the actor) so producers do not need
    // a back-pointer into actor-private memory.
    waitset: WaitSet = .{},

    // Live cross-fiber counters: handle-side increments are the source of
    // truth; the actor never folds them into `stats`. `currentStats` reads
    // them directly into the returned snapshot fields. `accepted_stream_pops`
    // is a signal mechanism (consumed via swap-to-zero by the actor in
    // `flushAcceptedStreamPops`), not a stat.
    outbound_stream_queue_full: std.atomic.Value(u64) = .init(0),
    control_inbox_drops: std.atomic.Value(u64) = .init(0),
    accepted_stream_pops: std.atomic.Value(u64) = .init(0),

    // Once-set metadata. Actor stores after handshake; handles atomically
    // load. Storage is heap-allocated and freed when the shared state is
    // released.
    public_key: std.atomic.Value(?*keys.PublicKey) = .init(null),
    remote_addr: std.atomic.Value(?*std.Io.net.IpAddress) = .init(null),

    pub const Config = struct {
        inbox_capacity: usize,
        accept_capacity: usize,
        outbound_pending_capacity: usize = 256,
        datagram_pool_slots: usize = 0,
        datagram_pool_slot_size: usize = 0,
        datagram_queue_capacity: usize = 0,
        initial_stats: ConnectionStats = .{},
        initial_max_datagram_payload: usize = 0,
    };

    pub fn create(allocator: Allocator, io: std.Io, config: Config) Allocator.Error!*SharedState {
        const self = try allocator.create(SharedState);
        errdefer allocator.destroy(self);

        const inbox_buf = try allocator.alloc(Command, config.inbox_capacity);
        errdefer allocator.free(inbox_buf);

        const accept_buf = try allocator.alloc(*Stream, config.accept_capacity);
        errdefer allocator.free(accept_buf);

        const outbound_pending_buf = try allocator.alloc(u64, @max(@as(usize, 1), config.outbound_pending_capacity));
        errdefer allocator.free(outbound_pending_buf);

        var dgram_pool: ?*DatagramPool = null;
        var dgram_queue: ?*DatagramChannel.State = null;
        if (config.datagram_pool_slots > 0 and config.datagram_pool_slot_size > 0) {
            dgram_pool = try DatagramPool.init(allocator, io, config.datagram_pool_slots, config.datagram_pool_slot_size);
            errdefer if (dgram_pool) |pool| pool.close();

            if (config.datagram_queue_capacity > 0) {
                dgram_queue = try DatagramChannel.State.init(allocator, io, config.datagram_queue_capacity, 0);
            }
        }

        self.* = .{
            .allocator = allocator,
            .io = io,
            .inbox = InboxQueue.init(inbox_buf),
            .inbox_buffer = inbox_buf,
            .accept_queue = AcceptQueue.init(accept_buf),
            .accept_buffer = accept_buf,
            .outbound_pending_ids = OutboundPendingQueue.init(outbound_pending_buf),
            .outbound_pending_buffer = outbound_pending_buf,
            .datagram_pool = dgram_pool,
            .datagram_queue = dgram_queue,
            .max_datagram_payload = .init(config.initial_max_datagram_payload),
            .stats = connection_stats.Publisher.init(config.initial_stats),
        };
        return self;
    }

    pub fn retain(self: *SharedState) void {
        self.rc.retainChecked();
    }

    pub fn release(self: *SharedState) void {
        if (!self.rc.releaseChecked()) return;
        self.destroy();
    }

    fn destroy(self: *SharedState) void {
        const allocator = self.allocator;
        const io = self.io;

        // Belt-and-suspenders: in the steady-state actor-exit path the actor
        // closes channels and drains datagrams before releasing its ref, but
        // a partial-init failure or a stack-allocated test runtime may reach
        // here without that happening. close()s are idempotent.
        _ = self.closeControlInbox();
        self.accept_queue.close(io);
        _ = self.drainAcceptedStreams(io);
        self.outbound_pending_ids.close(io);
        if (self.datagram_queue) |q| {
            q.close(io);
            q.discardQueued(io);
            q.release();
        }
        if (self.datagram_pool) |pool| pool.close();

        if (self.public_key.load(.acquire)) |pk| {
            if (pk.data) |data| allocator.free(data);
            allocator.destroy(pk);
        }
        if (self.remote_addr.load(.acquire)) |addr_ptr| {
            allocator.destroy(addr_ptr);
        }

        allocator.free(self.outbound_pending_buffer);
        allocator.free(self.accept_buffer);
        allocator.free(self.inbox_buffer);
        allocator.destroy(self);
    }

    pub fn isClosed(self: *const SharedState) bool {
        return self.closed.load(.acquire);
    }

    pub fn markClosed(self: *SharedState) void {
        self.closed.store(true, .release);
        self.handshake.close(self.io);
        self.waitset.notify(self.io, .{ .shutdown = true });
    }

    pub fn notifyControlCommandsReady(self: *SharedState) void {
        self.waitset.notify(self.io, .{ .control_commands = true });
    }

    pub fn closeControlInbox(self: *SharedState) usize {
        const io = self.io;
        self.inbox.close(io);
        return self.completeQueuedControlCommands(io);
    }

    fn completeQueuedControlCommands(self: *SharedState, io: std.Io) usize {
        var completed: usize = 0;
        var buf: [16]Command = undefined;
        while (true) {
            const n = self.inbox.getUncancelable(io, &buf, 0) catch break;
            if (n == 0) break;
            for (buf[0..n]) |cmd| cmd.completeClosed(io);
            completed += n;
        }
        return completed;
    }

    pub fn notifyStreamOutboundReady(self: *SharedState) void {
        self.waitset.notify(self.io, .{ .stream_outbound = true });
    }

    /// Push a stream id onto the outbound-pending queue. Multi-producer safe.
    /// Returns true on success; on overflow (or close) sets
    /// `fallback_full_scan` so the actor will sweep all streams instead, and
    /// returns false. The waitset bit must be raised separately by the caller
    /// (see `Stream.SharedState.signalOutboundReady`).
    pub fn tryPushOutboundPending(self: *SharedState, io: std.Io, stream_id: u64) bool {
        var buf = [_]u64{stream_id};
        const queued = self.outbound_pending_ids.putUncancelable(io, &buf, 0) catch 0;
        if (queued == 1) return true;
        self.fallback_full_scan.store(true, .release);
        return false;
    }

    pub fn notifyAcceptedStreamPop(self: *SharedState) void {
        self.waitset.notify(self.io, .{ .accepted_stream_pop = true });
    }

    /// Wakes handle-side `acceptStream` waiters via the waitset's epoch bump.
    /// The named bit has no actor consumer, so a parked actor also wakes once
    /// and no-ops — accepted, since a dedicated accept signal would need every
    /// close path to bump it too, and missing one hangs acceptStream forever.
    pub fn notifyAcceptedStreamPush(self: *SharedState) void {
        self.waitset.notify(self.io, .{ .accepted_stream_push = true });
    }

    pub fn tryPutAcceptedStream(self: *SharedState, io: std.Io, stream: *Stream) bool {
        const queued = self.accept_queue.putUncancelable(io, &.{stream}, 0) catch 0;
        if (queued != 1) return false;
        self.notifyAcceptedStreamPush();
        return true;
    }

    pub fn drainAcceptedStreams(self: *SharedState, io: std.Io) usize {
        var drained: usize = 0;
        var buf: [16]*Stream = undefined;
        while (true) {
            const n = self.accept_queue.getUncancelable(io, &buf, 0) catch break;
            if (n == 0) break;
            drained += n;
            for (buf[0..n]) |stream| stream.deinit();
        }
        return drained;
    }

    pub fn currentStats(self: *const SharedState) ConnectionStats {
        var out = self.stats.load(self.io);
        // Live counters: source of truth lives in the atomics, not the
        // published snapshot. Aggregating consumers (Grafana/Prometheus)
        // can sum across fields if they want a combined view.
        out.outbound_stream_queue_full = self.outbound_stream_queue_full.load(.acquire);
        out.control_inbox_drops = self.control_inbox_drops.load(.acquire);
        return out;
    }

    /// Returns a borrowed view of the verified peer public key, valid for
    /// as long as the SharedState is retained. The returned slice references
    /// memory owned by SharedState; do not free.
    pub fn loadPublicKey(self: *const SharedState) ?keys.PublicKey {
        const ptr = self.public_key.load(.acquire) orelse return null;
        return ptr.*;
    }

    pub fn loadRemoteAddr(self: *const SharedState) ?std.Io.net.IpAddress {
        const ptr = self.remote_addr.load(.acquire) orelse return null;
        return ptr.*;
    }

    /// Actor-only: publish the verified public key discovered after
    /// handshake. Takes ownership of `pub_key` (its `data` slice will be
    /// freed on shared-state release).
    pub fn publishPublicKey(self: *SharedState, pub_key: keys.PublicKey) Allocator.Error!void {
        if (self.public_key.load(.acquire) != null) {
            if (pub_key.data) |data| self.allocator.free(data);
            return;
        }
        const owned = try self.allocator.create(keys.PublicKey);
        owned.* = pub_key;
        self.public_key.store(owned, .release);
    }

    /// Actor-only: publish remote IP address after handshake.
    pub fn publishRemoteAddr(self: *SharedState, addr: std.Io.net.IpAddress) Allocator.Error!void {
        if (self.remote_addr.load(.acquire) != null) return;
        const owned = try self.allocator.create(std.Io.net.IpAddress);
        owned.* = addr;
        self.remote_addr.store(owned, .release);
    }
};

test "shared state create/release frees all allocations" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SharedState.create(allocator, io, .{
        .inbox_capacity = 16,
        .accept_capacity = 4,
    });
    state.release();
    state.release();
}

test "shared state two-side release frees on second drop" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SharedState.create(allocator, io, .{
        .inbox_capacity = 8,
        .accept_capacity = 2,
    });
    // refs starts at 2; one release does not free.
    state.release();
    try std.testing.expect(!state.isClosed());
    state.release();
    // (No assertion — testing allocator would catch a leak.)
}

test "shared state with datagram pool" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 2,
        .datagram_pool_slots = 8,
        .datagram_pool_slot_size = 1200,
        .datagram_queue_capacity = 4,
    });
    try std.testing.expect(state.datagram_pool != null);
    try std.testing.expect(state.datagram_queue != null);
    state.release();
    state.release();
}

test "drainAcceptedStreams deinits queued stream handles" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 2,
    });

    const stream_state = try @import("../stream/shared_state.zig").SharedState.create(allocator, io, state, .{
        .stream_id = 0,
        .inbound_queue_bytes = 1024,
        .outbound_queue_bytes = 1024,
    });
    const stream = try @import("../stream/handle.zig").create(allocator, stream_state);

    try std.testing.expect(state.tryPutAcceptedStream(io, stream));
    state.accept_queue.close(io);
    try std.testing.expectEqual(@as(usize, 1), state.drainAcceptedStreams(io));

    state.release();
    state.release();
}

test "closeControlInbox completes queued replies" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 1,
    });
    defer {
        state.release();
        state.release();
    }

    var reply: commands.OpenStreamReply = .{};
    try state.inbox.putOne(io, .{ .open_stream = &reply });

    try std.testing.expectEqual(@as(usize, 1), state.closeControlInbox());
    try std.testing.expect(reply.event.isSet());
    try std.testing.expectError(error.ConnectionClosed, reply.result);
}
