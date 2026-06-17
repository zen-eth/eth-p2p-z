//! `Record`: per-stream state held by the connection actor in its streams
//! table. Every field is touched only from the actor fiber — no
//! thread-shared resources. Holds one ref to the heap-allocated
//! `SharedState` (the handle holds the other).

const std = @import("std");
const conn_commands = @import("../connection/commands.zig");
const shared_state_mod = @import("shared_state.zig");

const Allocator = std.mem.Allocator;

pub const SharedState = shared_state_mod.SharedState;
pub const VoidReply = conn_commands.VoidReply;

const default_stream_scratch_len = 16 * 1024;

pub const Record = struct {
    shared: *SharedState,
    pending_send: []u8 = &.{},
    pending_send_off: usize = 0,
    pending_send_len: usize = 0,
    fin_sent: bool = false,
    fin_requested: bool = false,
    peer_stream_credit_reserved: bool = false,
    outbound_ready_queued: bool = false,
    /// If a `close_write_stream` command is mid-flight, the reply lives on
    /// the caller's stack and we fire it when FIN is actually sent.
    close_write_reply: ?*VoidReply = null,

    pub fn init(allocator: Allocator, shared: *SharedState, outbound_bytes: usize) Allocator.Error!Record {
        shared.retain();
        errdefer shared.release();
        const pending_len = @min(outbound_bytes, default_stream_scratch_len);
        return .{
            .shared = shared,
            .pending_send = if (pending_len > 0) try allocator.alloc(u8, pending_len) else &.{},
        };
    }

    pub fn deinit(self: *Record, allocator: Allocator) void {
        if (self.pending_send.len > 0) {
            allocator.free(self.pending_send);
            self.pending_send = &.{};
        }
        self.shared.release();
        self.* = undefined;
    }

    pub fn streamId(self: *const Record) u64 {
        return self.shared.stream_id;
    }

    pub fn pendingOutboundSlice(self: *Record) []const u8 {
        return self.pending_send[self.pending_send_off..self.pending_send_len];
    }

    pub fn advancePendingOutbound(self: *Record, n: usize) void {
        self.pending_send_off += n;
        if (self.pending_send_off >= self.pending_send_len) {
            self.pending_send_off = 0;
            self.pending_send_len = 0;
        }
    }

    /// Pull the next chunk of outbound bytes from the shared queue into the
    /// pending-send scratch buffer. Returns true if there is data to send.
    pub fn refillPendingOutbound(self: *Record, io: std.Io) bool {
        if (self.pending_send_len != self.pending_send_off) return true;
        if (self.pending_send.len == 0) return false;
        const queue = if (self.shared.outbound_queue) |*q| q else return false;
        const read_len = queue.tryGet(io, self.pending_send) catch return false;
        self.pending_send_off = 0;
        self.pending_send_len = read_len;
        return read_len > 0;
    }

    pub fn outboundIdle(self: *Record, io: std.Io) bool {
        if (self.pending_send_len != self.pending_send_off) return false;
        const queue = if (self.shared.outbound_queue) |*q| q else return true;
        return queue.used(io) == 0;
    }

    /// True when there are bytes to write to quiche or a deferred FIN to
    /// flush. Drives the connection actor's outbound-ready dispatch.
    pub fn needsOutboundDrain(self: *Record, io: std.Io) bool {
        if (self.shared.isOutboundResetByPeer()) return false;
        if (self.pendingOutboundSlice().len > 0) return true;
        if (self.shared.outbound_queue) |*q| {
            if (q.used(io) > 0) return true;
        }
        return self.fin_requested and !self.fin_sent and self.outboundIdle(io);
    }

    pub fn finReady(self: *Record, io: std.Io) bool {
        return self.fin_requested and !self.fin_sent and self.outboundIdle(io);
    }

    pub fn requestFin(self: *Record, reply: ?*VoidReply, io: std.Io) void {
        if (self.fin_sent or self.shared.isOutboundResetByPeer()) {
            if (reply) |r| r.complete(io);
            return;
        }
        if (self.close_write_reply) |existing| existing.complete(io);
        self.fin_requested = true;
        self.close_write_reply = reply;
    }

    /// Mark FIN as sent on the wire. Fires any deferred close_write reply
    /// so the caller's `closeWrite` returns once the FIN is in quiche's
    /// hands.
    pub fn markFinSent(self: *Record, io: std.Io) void {
        self.fin_sent = true;
        self.fin_requested = false;
        if (self.close_write_reply) |reply| {
            reply.complete(io);
            self.close_write_reply = null;
        }
    }

    /// Peer sent STOP_SENDING / RESET on our send side. Drop pending bytes
    /// and treat as fin-equivalent for any waiter.
    pub fn markWriteStoppedByPeer(self: *Record, io: std.Io) void {
        _ = self.releasePeerStreamCreditReservation();
        self.fin_sent = true;
        self.fin_requested = false;
        self.outbound_ready_queued = false;
        self.pending_send_off = 0;
        self.pending_send_len = 0;
        self.shared.markOutboundResetByPeer();
        if (self.close_write_reply) |reply| {
            reply.complete(io);
            self.close_write_reply = null;
        }
    }

    pub fn releasePeerStreamCreditReservation(self: *Record) bool {
        if (!self.peer_stream_credit_reserved) return false;
        self.peer_stream_credit_reserved = false;
        return true;
    }

    pub fn clearOutboundReadyQueued(self: *Record) void {
        self.outbound_ready_queued = false;
    }

    pub fn markOutboundReadyQueued(self: *Record) bool {
        if (self.outbound_ready_queued) return false;
        self.outbound_ready_queued = true;
        return true;
    }

    pub fn outboundReadyQueued(self: *const Record) bool {
        return self.outbound_ready_queued;
    }

    /// Detach the deferred close-write waiter on actor teardown so any
    /// caller blocked in `closeWrite` returns rather than waiting forever.
    pub fn detachCloseWriteWaiter(self: *Record, io: std.Io) void {
        if (self.close_write_reply) |reply| {
            reply.complete(io);
            self.close_write_reply = null;
        }
    }
};

test "requestFin defers reply until FIN is marked sent" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const conn = try @import("../connection/shared_state.zig").SharedState.create(allocator, io, .{
        .inbox_capacity = 4,
        .accept_capacity = 1,
    });
    defer {
        conn.release();
        conn.release();
    }

    const stream = try SharedState.create(allocator, io, conn, .{
        .stream_id = 0,
        .inbound_queue_bytes = 1024,
        .outbound_queue_bytes = 1024,
    });
    var record = try Record.init(allocator, stream, 1024);
    defer record.deinit(allocator);
    defer stream.release();

    var reply: VoidReply = .{};
    record.requestFin(&reply, io);
    try std.testing.expect(record.fin_requested);
    try std.testing.expect(record.finReady(io));
    try std.testing.expect(!reply.event.isSet());

    record.markFinSent(io);
    try std.testing.expect(!record.fin_requested);
    try std.testing.expect(record.fin_sent);
    try std.testing.expect(reply.event.isSet());
}
