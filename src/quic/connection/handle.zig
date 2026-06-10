//! `Connection` handle: the user-facing API for a QUIC connection. Normal
//! handle operations use only `SharedState` (channel + atomic flags +
//! handshake gate). The spawned actor pointer exists only for teardown
//! ownership: `deinit` cancels/awaits the actor and frees its allocation
//! through a concrete pointer instead of a callback erased into shared state.
//! Do not use it for ordinary connection operations.
//!
//! Per-connection metadata (stats, public key, remote addr) is read directly
//! from `SharedState` atomics.

const std = @import("std");
const commands = @import("commands.zig");
const stage_mod = @import("stage.zig");
const connection_actor = @import("actor.zig");
const conn_stats = @import("stats.zig");
const datagram = @import("../datagram/mod.zig");
const io_time = @import("../io/time.zig");
const shared_state_mod = @import("shared_state.zig");
const stream_handle = @import("../stream/handle.zig");

const keys = @import("peer_id").keys;
const Allocator = std.mem.Allocator;

const SharedState = shared_state_mod.SharedState;
const ConnectionActor = connection_actor.ConnectionActor;
const Command = commands.Command;
const VoidReply = commands.VoidReply;
const OpenStreamReply = commands.OpenStreamReply;
const SendDatagramReply = commands.SendDatagramReply;
const CloseConnectionReply = commands.CloseConnectionReply;

pub const Stream = stream_handle.Stream;
pub const Datagram = datagram.Datagram;
pub const ConnectionStats = conn_stats.ConnectionStats;
pub const HandshakeWaitError = stage_mod.HandshakeWaitError;

const OpenStreamErrorImpl = commands.OpenStreamError || error{ConnectionClosed} || std.Io.Cancelable;
const AcceptStreamErrorImpl = error{ ConnectionClosed, Timeout } || std.Io.Cancelable;
const CloseErrorImpl = error{AlreadyClosed} || std.Io.Cancelable || Allocator.Error;
const SendDatagramErrorImpl = commands.SendDatagramError || std.Io.Cancelable || Allocator.Error;
const RecvDatagramErrorImpl = error{ ConnectionClosed, DgramNotEnabled, Timeout } || std.Io.Cancelable;

const AcceptStreamOptionsImpl = struct { timeout: std.Io.Timeout = .none };
const RecvDatagramOptionsImpl = struct { timeout: std.Io.Timeout = .none };

const Impl = struct {
    allocator: Allocator,
    state: ?*SharedState,
    // Teardown-only ownership. Normal handle methods must go through `state`.
    spawned_actor: *ConnectionActor,
    /// Guards only the `state` POINTER: concurrent handle methods read it
    /// atomically, and a deinit nulls it exactly once. It deliberately does
    /// NOT make `deinit` safe against CONCURRENT method calls — `deinit`
    /// frees the Impl, including this very lock, so a fiber still inside (or
    /// entering) a handle method would dereference freed memory. No internal
    /// refcount could deliver that guarantee either: a caller entering after
    /// the free corrupts the count itself. Keeping the handle's MEMORY alive
    /// until all users are done is necessarily the OWNER's job (the same
    /// contract as Rust's Arc-owned quinn handles and go's net.Conn docs).
    handle_lock: std.atomic.Value(u8) = .init(0),

    fn lockHandle(self: *Impl) void {
        while (self.handle_lock.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlockHandle(self: *Impl) void {
        self.handle_lock.store(0, .release);
    }

    fn liveRetained(self: *Impl) ?*SharedState {
        self.lockHandle();
        const state = self.state;
        if (state) |st| st.retain();
        self.unlockHandle();
        return state;
    }

    fn deinit(self: *Impl) void {
        const allocator = self.allocator;
        self.lockHandle();
        const state = self.state;
        const actor = self.spawned_actor;
        self.state = null;
        self.unlockHandle();
        if (state) |st| {
            postFireAndForget(st, .shutdown);
        }
        // `destroySpawned` blocks until the actor future is no longer
        // touching actor-private memory, then frees the actor allocation.
        actor.destroySpawned();
        if (state) |st| {
            st.release();
        }
        allocator.destroy(self);
    }
};

pub const Connection = opaque {
    pub const OpenStreamError = OpenStreamErrorImpl;
    pub const AcceptStreamError = AcceptStreamErrorImpl;
    pub const CloseError = CloseErrorImpl;
    pub const SendDatagramError = SendDatagramErrorImpl;
    pub const RecvDatagramError = RecvDatagramErrorImpl;
    pub const HandshakeWaitError = stage_mod.HandshakeWaitError;
    pub const AcceptStreamOptions = AcceptStreamOptionsImpl;
    pub const RecvDatagramOptions = RecvDatagramOptionsImpl;

    fn impl(self: *Connection) *Impl {
        return @ptrCast(@alignCast(self));
    }

    /// Destroy the handle. CONTRACT — external synchronization required: no
    /// other fiber may be inside, or subsequently enter, ANY method of this
    /// handle once deinit starts; the handle memory is freed here, so a racing
    /// call is a use-after-free (the internal lock only serializes the
    /// state-pointer swap — see `Impl.handle_lock`). Note also that deinit
    /// JOINS the connection's actor fiber (blocks until it fully unwinds), so
    /// it can take as long as a full connection teardown.
    pub fn deinit(self: *Connection) void {
        self.impl().deinit();
    }

    pub fn openStream(self: *Connection, io: std.Io) OpenStreamErrorImpl!*Stream {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        return openStreamFromState(state, io);
    }

    pub fn acceptStream(self: *Connection, io: std.Io, opts: AcceptStreamOptionsImpl) AcceptStreamErrorImpl!*Stream {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        return acceptStreamFromState(state, io, opts);
    }

    pub fn close(self: *Connection, io: std.Io, app_error_code: u64, reason: []const u8) CloseErrorImpl!void {
        const state = self.impl().liveRetained() orelse return error.AlreadyClosed;
        defer state.release();
        if (state.isClosed()) return error.AlreadyClosed;

        var reply: CloseConnectionReply = .{};
        var args: Command = .{ .close_connection = .{
            .code = app_error_code,
            .reason = undefined,
            .reason_len = @min(reason.len, commands.close_connection_reason_max),
            .reply = &reply,
        } };
        @memcpy(args.close_connection.reason[0..args.close_connection.reason_len], reason[0..args.close_connection.reason_len]);
        if (!try postCommand(state, args, io)) return error.AlreadyClosed;
        reply.event.waitUncancelable(io);
        return reply.result;
    }

    pub fn sendDatagram(self: *Connection, io: std.Io, data: []const u8) SendDatagramErrorImpl!void {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        if (state.isClosed()) return error.ConnectionClosed;

        var reply: SendDatagramReply = .{};
        if (!try postCommand(state, .{ .send_datagram = .{ .payload = data, .reply = &reply } }, io)) return error.ConnectionClosed;
        reply.event.waitUncancelable(io);
        return reply.result;
    }

    pub fn recvDatagram(self: *Connection, io: std.Io, opts: RecvDatagramOptionsImpl) RecvDatagramErrorImpl!*Datagram {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        const dgram_queue = state.datagram_queue orelse return error.DgramNotEnabled;

        const deadline = opts.timeout.toDeadline(io);
        if (opts.timeout == .none) {
            const dgram = dgram_queue.recv(io) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                error.Closed => return error.ConnectionClosed,
            };
            return dgram;
        }
        while (true) {
            const observed = dgram_queue.observe();
            var dgram: *Datagram = undefined;
            if (dgram_queue.tryRecv(io, &dgram)) return dgram;
            if (state.isClosed()) return error.ConnectionClosed;
            if (io_time.timeoutExpired(io, deadline)) return error.Timeout;
            if (deadline == .none) try dgram_queue.wait(io, observed) else try dgram_queue.waitTimeout(io, observed, deadline);
        }
    }

    pub fn maxDatagramPayload(self: *Connection) usize {
        const state = self.impl().liveRetained() orelse return 0;
        defer state.release();
        return state.max_datagram_payload.load(.acquire);
    }

    pub fn stats(self: *const Connection) ConnectionStats {
        const state = @constCast(self).impl().liveRetained() orelse return .{};
        defer state.release();
        return state.currentStats();
    }

    /// Structured reason the handshake failed, derived from the published stats
    /// snapshot. Meaningful after `waitHandshake` returns an error; on a healthy
    /// or established connection it reports `.none`/`.unknown`.
    pub fn failReason(self: *const Connection) conn_stats.HandshakeFailure {
        return conn_stats.classifyHandshakeFailure(self.stats());
    }

    /// Returns the verified libp2p public key extracted from the peer's TLS
    /// certificate. The returned value borrows memory owned by the
    /// connection; do not free its `data` slice.
    ///
    /// A `*Connection` returned by `dial()` or `accept()` has a published
    /// public key by construction (both call `waitHandshake` before
    /// returning); calling this method on such a handle always succeeds.
    pub fn remotePublicKey(self: *const Connection) keys.PublicKey {
        const state = @constCast(self).impl().liveRetained().?;
        defer state.release();
        return state.loadPublicKey().?;
    }

    /// Returns the peer's IP address. As with `remotePublicKey`, valid on any
    /// `*Connection` returned by `dial()` or `accept()`.
    pub fn remoteAddress(self: *const Connection) std.Io.net.IpAddress {
        const state = @constCast(self).impl().liveRetained().?;
        defer state.release();
        return state.loadRemoteAddr().?;
    }

    pub fn isClosed(self: *const Connection) bool {
        const state = @constCast(self).impl().liveRetained() orelse return true;
        defer state.release();
        return state.isClosed();
    }

    pub fn waitHandshake(self: *Connection, io: std.Io) stage_mod.HandshakeWaitError!void {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        return state.handshake.wait(io, &state.closed);
    }
};

pub fn createSpawned(allocator: Allocator, state: *SharedState, actor: *ConnectionActor) Allocator.Error!*Connection {
    const handle = try allocator.create(Impl);
    handle.* = .{ .allocator = allocator, .state = state, .spawned_actor = actor };
    return @ptrCast(handle);
}

fn postCommand(state: *SharedState, cmd: Command, io: std.Io) std.Io.Cancelable!bool {
    state.inbox.putOne(io, cmd) catch |err| switch (err) {
        error.Closed => return false,
        error.Canceled => return error.Canceled,
    };
    state.notifyControlCommandsReady();
    return true;
}

fn postFireAndForget(state: *SharedState, cmd: Command) void {
    const io = state.io;
    const n = state.inbox.putUncancelable(io, &.{cmd}, 0) catch {
        _ = state.control_inbox_drops.fetchAdd(1, .acq_rel);
        return;
    };
    if (n == 1) {
        state.notifyControlCommandsReady();
    } else {
        _ = state.control_inbox_drops.fetchAdd(1, .acq_rel);
    }
}

fn openStreamFromState(state: *SharedState, io: std.Io) OpenStreamErrorImpl!*Stream {
    if (state.isClosed()) return error.ConnectionClosed;

    var reply: OpenStreamReply = .{};
    if (!try postCommand(state, .{ .open_stream = &reply }, io)) return error.ConnectionClosed;
    reply.event.waitUncancelable(io);
    return reply.result;
}

fn acceptStreamFromState(state: *SharedState, io: std.Io, opts: AcceptStreamOptionsImpl) AcceptStreamErrorImpl!*Stream {
    const deadline = opts.timeout.toDeadline(io);
    while (true) {
        var buf: [1]*Stream = undefined;
        const observed = state.waitset.observe();
        const n = state.accept_queue.getUncancelable(io, &buf, 0) catch return error.ConnectionClosed;
        if (n == 1) {
            _ = state.accepted_stream_pops.fetchAdd(1, .acq_rel);
            state.notifyAcceptedStreamPop();
            return buf[0];
        }
        if (state.isClosed()) return error.ConnectionClosed;
        if (opts.timeout != .none and io_time.timeoutExpired(io, deadline)) return error.Timeout;
        // Park on the signal EPOCH (an edge), not the readiness bits. This is a
        // handle-side waiter that never clears the waitset bits — only the owning
        // actor does, via `take`, each of its loops. If we parked with the
        // level-triggered `wait` (returns whenever ANY bit is set), the actor's
        // routinely-set, not-yet-taken bits (inbound packets, control commands)
        // would make us return immediately and spin: re-check the still-empty
        // accept queue, see the connection still open, return again. That spin
        // burns the executor and can starve the actor fiber that would push a
        // stream or mark the connection closed — turning a quiet idle accept into
        // a livelock. Edge detection wakes us exactly when something we care about
        // happened (a pushed stream, or shutdown), and `isClosed()` is re-checked
        // on each wake so a connection close reliably unblocks us.
        if (opts.timeout == .none) {
            try state.waitset.waitEpoch(io, observed);
            continue;
        }
        try state.waitset.waitEpochTimeout(io, observed, deadline);
    }
}

test "timeout acceptStream wakes when a stream enters the accept queue" {
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

    const AcceptCtx = struct {
        state: *SharedState,
        io: std.Io,
        result: ?*stream_handle.Stream = null,
        err: ?anyerror = null,

        fn run(ctx: *@This()) void {
            ctx.result = acceptStreamFromState(ctx.state, ctx.io, .{
                .timeout = .{ .duration = .{
                    .raw = std.Io.Duration.fromMilliseconds(500),
                    .clock = .awake,
                } },
            }) catch |err| {
                ctx.err = err;
                return;
            };
        }
    };

    var ctx = AcceptCtx{ .state = state, .io = io };
    const accept_thread = try std.Thread.spawn(.{}, AcceptCtx.run, .{&ctx});

    var fake_storage: usize = 0;
    const fake_stream: *stream_handle.Stream = @ptrCast(@alignCast(&fake_storage));
    try std.testing.expect(state.tryPutAcceptedStream(io, fake_stream));

    accept_thread.join();
    if (ctx.err) |err| return err;
    try std.testing.expectEqual(fake_stream, ctx.result orelse return error.TestExpectedEqual);
}

test "openStream returns when actor inbox is already closed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SharedState.create(allocator, io, .{
        .inbox_capacity = 1,
        .accept_capacity = 1,
    });
    defer {
        state.release();
        state.release();
    }

    state.inbox.close(io);
    try std.testing.expectError(error.ConnectionClosed, openStreamFromState(state, io));
}
