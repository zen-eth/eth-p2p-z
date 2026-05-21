//! Commands sent from caller fibers (via `Connection` / `Stream` handles) to
//! the `ConnectionActor` over its inbox queue.
//!
//! Replies live on the caller's stack: each reply struct holds a
//! `std.Io.Event` plus a typed result slot. Callers `waitUncancelable` on the
//! event so the reply struct's lifetime is bounded by the calling function —
//! no heap allocation, no detach state machine. The actor completes queued
//! reply commands when the control inbox is shut down, so a caller parked on
//! a stack reply slot is not orphaned by teardown.
//!
//! Fire-and-forget commands (`drop_stream`, `shutdown`) carry their args
//! inline and have no reply.

const std = @import("std");
const stream_handle = @import("../stream/handle.zig");

pub const Stream = stream_handle.Stream;

pub const OpenStreamError = error{
    ConnectionClosed,
    StreamLimitReached,
} || std.mem.Allocator.Error;

pub const SendDatagramError = error{
    ConnectionClosed,
    DatagramTooLarge,
    SendQueueFull,
    DgramNotEnabled,
};

pub const CloseConnectionError = error{AlreadyClosed};

/// Reply for commands that signal completion but carry no result value
/// (close_stream, close_read_stream, close_write_stream, reset_stream,
/// close_connection, send_datagram succeeded). The caller stack-allocates,
/// the actor calls `complete` exactly once.
pub const VoidReply = struct {
    event: std.Io.Event = .unset,

    pub fn complete(self: *VoidReply, io: std.Io) void {
        self.event.set(io);
    }
};

pub const OpenStreamReply = struct {
    event: std.Io.Event = .unset,
    result: OpenStreamError!*Stream = error.ConnectionClosed,

    pub fn complete(self: *OpenStreamReply, io: std.Io, result: OpenStreamError!*Stream) void {
        self.result = result;
        self.event.set(io);
    }
};

pub const SendDatagramReply = struct {
    event: std.Io.Event = .unset,
    result: SendDatagramError!void = error.ConnectionClosed,

    pub fn complete(self: *SendDatagramReply, io: std.Io, result: SendDatagramError!void) void {
        self.result = result;
        self.event.set(io);
    }
};

pub const CloseConnectionReply = struct {
    event: std.Io.Event = .unset,
    result: CloseConnectionError!void = {},

    pub fn complete(self: *CloseConnectionReply, io: std.Io, result: CloseConnectionError!void) void {
        self.result = result;
        self.event.set(io);
    }
};

/// Inline reason buffer for `CONNECTION_CLOSE`. RFC 9000 §10.2 caps the
/// reason phrase at "small" (typical impls clamp around 256 bytes); we copy
/// into the command so the caller's slice need not outlive the call.
pub const close_connection_reason_max: usize = 256;

pub const Command = union(enum) {
    /// Open a bidirectional stream from this peer's side.
    open_stream: *OpenStreamReply,

    /// Graceful bidirectional close (FIN + STOP_SENDING(0)). Fire-and-forget:
    /// FIN flushing happens asynchronously via the actor drain.
    close_stream: struct {
        stream_id: u64,
    },
    /// Graceful read-side close (STOP_SENDING(0)).
    close_read_stream: struct {
        stream_id: u64,
        reply: *VoidReply,
    },
    /// Graceful write-side close (FIN). Reply fires *after* the FIN has
    /// actually been handed to quiche — preserves the existing
    /// `closeWrite` blocking semantics so callers can sequence "FIN sent
    /// before connection close".
    close_write_stream: struct {
        stream_id: u64,
        reply: *VoidReply,
    },
    /// Abrupt close: RESET_STREAM(code) on the write side, STOP_SENDING(code)
    /// on the read side. Stream actor entry is dropped immediately.
    reset_stream: struct {
        stream_id: u64,
        code: u64,
        reply: *VoidReply,
    },
    /// Stream handle was dropped without an explicit close. Actor maps this
    /// to RESET_STREAM(0). Fire-and-forget.
    drop_stream: u64,

    send_datagram: struct {
        /// Borrowed; valid for the duration of the actor's processing of
        /// this command (caller is parked on `reply.event` until then).
        /// Quiche copies internally on `quiche_conn_dgram_send`.
        payload: []const u8,
        reply: *SendDatagramReply,
    },

    close_connection: struct {
        code: u64,
        reason: [close_connection_reason_max]u8,
        reason_len: usize,
        reply: *CloseConnectionReply,
    },

    /// Asks the actor to drain its remaining work and exit. Fire-and-forget;
    /// caller observes via `SharedState.closed` going true.
    shutdown: void,

    pub fn completeClosed(cmd: Command, io: std.Io) void {
        switch (cmd) {
            .open_stream => |reply| reply.complete(io, error.ConnectionClosed),
            .close_stream, .drop_stream, .shutdown => {},
            .close_read_stream => |args| args.reply.complete(io),
            .close_write_stream => |args| args.reply.complete(io),
            .reset_stream => |args| args.reply.complete(io),
            .send_datagram => |args| args.reply.complete(io, error.ConnectionClosed),
            .close_connection => |args| args.reply.complete(io, error.AlreadyClosed),
        }
    }
};

test "command union size sanity" {
    // The union should fit comfortably in a queue buffer; commands are
    // pushed by-value into the inbox.
    try std.testing.expect(@sizeOf(Command) <= 320);
}

test "completeClosed wakes every reply-bearing command" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var open_reply: OpenStreamReply = .{};
    Command.completeClosed(.{ .open_stream = &open_reply }, io);
    try std.testing.expect(open_reply.event.isSet());
    try std.testing.expectError(error.ConnectionClosed, open_reply.result);

    var close_read_reply: VoidReply = .{};
    Command.completeClosed(.{ .close_read_stream = .{
        .stream_id = 1,
        .reply = &close_read_reply,
    } }, io);
    try std.testing.expect(close_read_reply.event.isSet());

    var close_write_reply: VoidReply = .{};
    Command.completeClosed(.{ .close_write_stream = .{
        .stream_id = 1,
        .reply = &close_write_reply,
    } }, io);
    try std.testing.expect(close_write_reply.event.isSet());

    var reset_reply: VoidReply = .{};
    Command.completeClosed(.{ .reset_stream = .{
        .stream_id = 1,
        .code = 0,
        .reply = &reset_reply,
    } }, io);
    try std.testing.expect(reset_reply.event.isSet());

    var datagram_reply: SendDatagramReply = .{};
    Command.completeClosed(.{ .send_datagram = .{
        .payload = "dropped",
        .reply = &datagram_reply,
    } }, io);
    try std.testing.expect(datagram_reply.event.isSet());
    try std.testing.expectError(error.ConnectionClosed, datagram_reply.result);

    var close_conn_reply: CloseConnectionReply = .{};
    Command.completeClosed(.{ .close_connection = .{
        .code = 0,
        .reason = undefined,
        .reason_len = 0,
        .reply = &close_conn_reply,
    } }, io);
    try std.testing.expect(close_conn_reply.event.isSet());
    try std.testing.expectError(error.AlreadyClosed, close_conn_reply.result);
}
