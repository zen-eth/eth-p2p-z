const std = @import("std");
const channel = @import("../io/channel.zig");
const connection_mod = @import("../connection/mod.zig");

const Connection = connection_mod.Connection;

fn dropConn(slot: **Connection, _: std.Io) void {
    slot.*.deinit();
}

/// Bounded MPSC channel of accepted `*Connection` handed to the user via
/// `QuicEndpoint.accept`. The drop hook deinits any connection still queued
/// when the channel is discarded (close/teardown).
pub const Channel = channel.Bounded(*Connection, dropConn, null);

/// In-flight admission token. The router-side reservation system holds at most
/// `capacity` of these (counted via the accompanying `available` atomic) so
/// that handshake fibers spawned from `acceptInitial` are guaranteed a place
/// in the channel by the time they call `publish`.
pub const Permit = struct {
    channel: ?*Channel.State = null,
    available: ?*std.atomic.Value(usize) = null,

    /// Move semantics: hand the reservation to a child fiber without
    /// double-releasing on this side.
    pub fn take(p: *Permit) Permit {
        const moved = p.*;
        p.channel = null;
        p.available = null;
        return moved;
    }

    pub fn publish(p: *Permit, io: std.Io, conn: *Connection) bool {
        const ch = p.channel orelse return false;
        const available = p.available orelse return false;
        p.channel = null;
        p.available = null;
        if (ch.sender().trySend(io, conn)) return true;
        // trySend dropped `conn` via `dropConn`. Restore the reservation slot
        // so a future reserver could use it (or it stays free at teardown).
        _ = available.fetchAdd(1, .acq_rel);
        return false;
    }

    pub fn cancel(p: *Permit) void {
        const available = p.available orelse return;
        _ = available.fetchAdd(1, .acq_rel);
        p.channel = null;
        p.available = null;
    }
};

/// Pre-flight admission check. CAS-decrements `available`; on success the
/// caller owns a slot until `publish` (transfers it to a queued item) or
/// `cancel` (releases it).
pub fn tryReserve(ch: *Channel.State, available: *std.atomic.Value(usize)) ?Permit {
    // CAS-decrement-to-floor admission idiom, shared with the switch handler gate.
    if (!channel.tryDecrementToFloor(available)) return null;
    return .{ .channel = ch, .available = available };
}

test "tryReserve caps in-flight admissions at capacity" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try Channel.State.init(std.testing.allocator, io, 2, 0);
    defer state.release();
    defer state.close(io);

    var available: std.atomic.Value(usize) = .init(2);

    var p1 = tryReserve(state, &available) orelse return error.TestExpectedEqual;
    var p2 = tryReserve(state, &available) orelse return error.TestExpectedEqual;
    try std.testing.expect(tryReserve(state, &available) == null);

    p1.cancel();
    var p3 = tryReserve(state, &available) orelse return error.TestExpectedEqual;
    p2.cancel();
    p3.cancel();
    try std.testing.expectEqual(@as(usize, 2), available.load(.acquire));
}
