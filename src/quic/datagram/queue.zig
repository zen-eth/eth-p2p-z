const std = @import("std");
const channel = @import("../io/channel.zig");
const handle_pool = @import("handle_pool.zig");

const Datagram = handle_pool.Datagram;

fn dropDatagram(slot: **Datagram, _: std.Io) void {
    slot.*.release();
}

/// Bounded MPSC channel of `*Datagram` returned by the pool. The drop hook
/// returns each slot to the pool whenever the channel discards an item
/// (overflow `trySend`, `discardQueued`, or `release` of the last ref).
pub const Queue = channel.Bounded(*Datagram, dropDatagram, null);

test "datagram channel returns slots when dropped on overflow" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const pool = try handle_pool.Pool.init(allocator, io, 2, 8);
    const state = try Queue.State.init(allocator, io, 1, 0);
    defer state.release();
    defer state.close(io);

    const queued = pool.acquire(4) orelse return error.TestUnexpectedResult;
    try std.testing.expect(state.sender().trySend(io, queued));

    const dropped = pool.acquire(4) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!state.sender().trySend(io, dropped));

    const reused = pool.acquire(4) orelse return error.TestUnexpectedResult;
    reused.release();
    pool.close();
}
