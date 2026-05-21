//! Heap-allocated state shared between a `Stream` handle and the
//! `ConnectionActor` (which keeps the actor-side `Record` in its streams
//! table).
//!
//! Holds exactly the things that *both* sides need to touch:
//!   - Byte queues (inbound: actor → handle; outbound: handle → actor)
//!   - Atomic state flags (closed, write/read shutdown, peer reset)
//!   - Pointer back to the connection's `SharedState` so the handle can
//!     post commands and signal the actor's wake mechanism without ever
//!     holding a raw pointer into actor-private memory.
//!
//! Refcount initialises to 2: one ref held by the `Stream` handle, one by
//! the actor's `Record`. Whichever side drops its ref last frees the
//! allocation. `closeOnHandleDrop` is implemented via a `drop_stream`
//! command on the connection's inbox — the handle never reaches into the
//! actor's data structures.

const std = @import("std");
const byte_queue = @import("byte_queue.zig");
const conn_shared = @import("../connection/shared_state.zig");

const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;

pub const ByteQueue = byte_queue.ByteQueue;
pub const ConnSharedState = conn_shared.SharedState;

pub const SharedState = struct {
    allocator: Allocator,
    io: std.Io,
    /// One ref for the handle. The record retains separately in
    /// `StreamRecord.init` and releases in its `deinit`.
    refs: Atomic(usize) = .init(1),

    /// Connection this stream belongs to. The handle uses this to post
    /// commands (close, reset, ...) and to signal the actor's wake
    /// mechanism. Refcount: the stream's existence keeps the connection
    /// alive (we retain on create, release on destroy).
    conn: *ConnSharedState,

    stream_id: u64,

    // Byte queues (allocated at create time, never re-allocated).
    inbound_queue: ?ByteQueue = null,
    outbound_queue: ?ByteQueue = null,

    // Atomic state flags. Both handle and actor mutate; both observe.
    closed: Atomic(bool) = .init(false),
    write_shutdown: Atomic(bool) = .init(false),
    read_shutdown: Atomic(bool) = .init(false),
    inbound_reset_by_peer: Atomic(bool) = .init(false),
    outbound_reset_by_peer: Atomic(bool) = .init(false),
    /// Set when this stream has been pushed onto the connection's
    /// outbound-pending queue but the actor hasn't popped it yet.
    /// Coalesces redundant pushes from a burst of writes. The handle CASes
    /// false→true when it pushes; the actor stores false after popping.
    outbound_signaled: Atomic(bool) = .init(false),

    pub const Config = struct {
        stream_id: u64,
        inbound_queue_bytes: usize,
        outbound_queue_bytes: usize,
    };

    pub fn create(allocator: Allocator, io: std.Io, conn: *ConnSharedState, config: Config) Allocator.Error!*SharedState {
        const self = try allocator.create(SharedState);
        errdefer allocator.destroy(self);

        var inbound: ?ByteQueue = null;
        errdefer if (inbound) |*q| q.deinit(allocator);
        if (config.inbound_queue_bytes > 0) {
            inbound = try ByteQueue.init(allocator, config.inbound_queue_bytes);
        }

        var outbound: ?ByteQueue = null;
        errdefer if (outbound) |*q| q.deinit(allocator);
        if (config.outbound_queue_bytes > 0) {
            outbound = try ByteQueue.init(allocator, config.outbound_queue_bytes);
        }

        conn.retain();
        errdefer conn.release();

        self.* = .{
            .allocator = allocator,
            .io = io,
            .conn = conn,
            .stream_id = config.stream_id,
            .inbound_queue = inbound,
            .outbound_queue = outbound,
        };
        return self;
    }

    pub fn retain(self: *SharedState) void {
        const previous = self.refs.fetchAdd(1, .acq_rel);
        std.debug.assert(previous > 0);
    }

    pub fn release(self: *SharedState) void {
        const previous = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        self.destroy();
    }

    fn destroy(self: *SharedState) void {
        const allocator = self.allocator;

        // Belt-and-suspenders: idempotent close so a partial-init drop or
        // a stack-test runtime that reaches here without the actor doing
        // its teardown still releases queue resources cleanly.
        if (self.inbound_queue) |*q| {
            q.close(self.io);
            q.deinit(allocator);
            self.inbound_queue = null;
        }
        if (self.outbound_queue) |*q| {
            q.close(self.io);
            q.deinit(allocator);
            self.outbound_queue = null;
        }
        self.conn.release();
        allocator.destroy(self);
    }

    pub fn isClosed(self: *const SharedState) bool {
        return self.closed.load(.acquire);
    }

    pub fn isWriteShutdown(self: *const SharedState) bool {
        return self.write_shutdown.load(.acquire);
    }

    pub fn isReadShutdown(self: *const SharedState) bool {
        return self.read_shutdown.load(.acquire);
    }

    pub fn isInboundResetByPeer(self: *const SharedState) bool {
        return self.inbound_reset_by_peer.load(.acquire);
    }

    pub fn isOutboundResetByPeer(self: *const SharedState) bool {
        return self.outbound_reset_by_peer.load(.acquire);
    }

    /// Handle-side: mark all sides closed and close queues so any
    /// outstanding read/write wakes immediately. Called when the handle
    /// posts a `close` or `reset` command, or when the actor reports
    /// peer-side termination.
    pub fn markClosedLocal(self: *SharedState) void {
        self.closed.store(true, .release);
        self.write_shutdown.store(true, .release);
        self.read_shutdown.store(true, .release);
        if (self.inbound_queue) |*q| q.close(self.io);
        if (self.outbound_queue) |*q| q.close(self.io);
    }

    /// Actor-side: quiche reports the stream is fully finished. No further
    /// writes can make progress, but inbound bytes already queued for the
    /// handle must remain readable until the queue drains to EndOfStream.
    pub fn markFinishedByActor(self: *SharedState) void {
        self.write_shutdown.store(true, .release);
        if (self.outbound_queue) |*q| q.close(self.io);
    }

    pub fn markReadShutdownLocal(self: *SharedState) void {
        self.read_shutdown.store(true, .release);
        if (self.inbound_queue) |*q| q.close(self.io);
    }

    pub fn markWriteShutdownLocal(self: *SharedState) void {
        self.write_shutdown.store(true, .release);
        if (self.outbound_queue) |*q| q.close(self.io);
    }

    /// Actor-side: peer sent RESET_STREAM. Mark inbound reset and close
    /// the inbound queue so any blocked reader wakes with ResetByPeer.
    pub fn markInboundResetByPeer(self: *SharedState) void {
        self.inbound_reset_by_peer.store(true, .release);
        self.read_shutdown.store(true, .release);
        if (self.inbound_queue) |*q| q.close(self.io);
    }

    /// Actor-side: peer sent STOP_SENDING (or quiche reports our writes
    /// were rejected). Block further writes and close the outbound queue
    /// so any blocked writer wakes with ResetByPeer.
    pub fn markOutboundResetByPeer(self: *SharedState) void {
        self.outbound_reset_by_peer.store(true, .release);
        self.write_shutdown.store(true, .release);
        if (self.outbound_queue) |*q| q.close(self.io);
    }

    /// Actor-side: peer sent FIN; no more bytes are coming. Close the
    /// inbound queue so a blocked reader observes EndOfStream.
    pub fn closeInbound(self: *SharedState) void {
        if (self.inbound_queue) |*q| q.close(self.io);
    }

    /// Actor-side: write capacity check before pulling from the outbound
    /// queue. Returns true iff a write to the wire would have flow on
    /// either side (queue has bytes, or FIN is pending).
    pub fn outboundIdle(self: *SharedState) bool {
        if (self.outbound_queue) |*q| return q.used(self.io) == 0;
        return true;
    }

    /// Handle-side: tell the actor this stream has new outbound bytes.
    /// Coalesces concurrent writes via `outbound_signaled` so a burst of
    /// writes results in at most one push to the connection's
    /// outbound-pending queue. The waitset bit is always raised so the
    /// actor will wake even on overflow (where it falls back to a full
    /// stream scan).
    pub fn signalOutboundReady(self: *SharedState, io: std.Io) void {
        if (!self.outbound_signaled.swap(true, .acq_rel)) {
            // We transitioned false→true; we own the push.
            if (!self.conn.tryPushOutboundPending(io, self.stream_id)) {
                // Push failed (overflow set fallback_full_scan). Clear the
                // flag so the next write retries; correctness still holds
                // because the actor will sweep all streams.
                self.outbound_signaled.store(false, .release);
            }
        }
        self.conn.notifyStreamOutboundReady();
    }
};

test "stream shared state create/release frees" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const conn_state = try ConnSharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 2,
    });
    defer {
        conn_state.release();
        conn_state.release();
    }

    // `create` returns refs=1 (the handle ref). Mimic a StreamRecord by
    // taking a second ref so the test exercises the two-release path.
    const stream = try SharedState.create(allocator, io, conn_state, .{
        .stream_id = 0,
        .inbound_queue_bytes = 1024,
        .outbound_queue_bytes = 1024,
    });
    stream.retain();
    stream.release();
    stream.release();
}

test "stream shared state two-side release" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const conn_state = try ConnSharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 2,
    });
    defer {
        conn_state.release();
        conn_state.release();
    }

    const stream = try SharedState.create(allocator, io, conn_state, .{
        .stream_id = 4,
        .inbound_queue_bytes = 0,
        .outbound_queue_bytes = 0,
    });
    stream.retain();
    stream.release();
    try std.testing.expect(!stream.isClosed());
    stream.release();
}

test "stream shared state state flags" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const conn_state = try ConnSharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 2,
    });
    defer {
        conn_state.release();
        conn_state.release();
    }

    const stream = try SharedState.create(allocator, io, conn_state, .{
        .stream_id = 8,
        .inbound_queue_bytes = 64,
        .outbound_queue_bytes = 64,
    });
    stream.retain();
    defer {
        stream.release();
        stream.release();
    }

    try std.testing.expect(!stream.isClosed());
    stream.markReadShutdownLocal();
    try std.testing.expect(stream.isReadShutdown());
    stream.markInboundResetByPeer();
    try std.testing.expect(stream.isInboundResetByPeer());
    stream.markOutboundResetByPeer();
    try std.testing.expect(stream.isOutboundResetByPeer());
}
