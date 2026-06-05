const std = @import("std");
const channel = @import("channel.zig");

pub const Ready = packed struct(u32) {
    inbound_packets: bool = false,
    control_commands: bool = false,
    stream_inbound: bool = false,
    stream_outbound: bool = false,
    accepted_stream_pop: bool = false,
    accepted_stream_push: bool = false,
    shutdown: bool = false,
    _reserved: u25 = 0,

    pub fn any(ready: Ready) bool {
        const raw: u32 = @bitCast(ready);
        return raw != 0;
    }

    pub fn merge(lhs: Ready, rhs: Ready) Ready {
        const lhs_raw: u32 = @bitCast(lhs);
        const rhs_raw: u32 = @bitCast(rhs);
        return @bitCast(lhs_raw | rhs_raw);
    }
};

pub const WaitSet = struct {
    bits: std.atomic.Value(u32) = .init(0),
    signal: channel.Signal = .{},

    pub fn notify(ws: *WaitSet, io: std.Io, ready: Ready) void {
        const raw: u32 = @bitCast(ready);
        if (raw == 0) return;
        _ = ws.bits.fetchOr(raw, .release);
        ws.signal.notify(io);
    }

    pub fn take(ws: *WaitSet) Ready {
        return @bitCast(ws.bits.swap(0, .acq_rel));
    }

    pub fn peek(ws: *const WaitSet) Ready {
        return @bitCast(ws.bits.load(.acquire));
    }

    pub fn observe(ws: *const WaitSet) u32 {
        return ws.signal.observe();
    }

    pub fn unchanged(ws: *const WaitSet, observed: u32) bool {
        return ws.signal.unchanged(observed);
    }

    pub fn wait(ws: *WaitSet, io: std.Io, observed: u32) std.Io.Cancelable!void {
        if (ws.peek().any()) return;
        return ws.signal.wait(io, observed);
    }

    pub fn waitTimeout(ws: *WaitSet, io: std.Io, observed: u32, timeout: std.Io.Timeout) std.Io.Cancelable!void {
        if (ws.peek().any()) return;
        return ws.signal.waitTimeout(io, observed, timeout);
    }

    /// Park until the signal epoch advances past `observed` (an edge), ignoring
    /// the readiness bits entirely. For waiters that do NOT clear the bits via
    /// `take` (e.g. a connection handle blocked in acceptStream, distinct from
    /// the owning actor which takes the bits each loop). Such a waiter must not
    /// use `wait`/`waitTimeout`: those return immediately whenever ANY bit is
    /// set, and the actor routinely sets bits the waiter does not consume (e.g.
    /// inbound-packet or control-command readiness). A non-clearing waiter using
    /// the level-triggered `wait` would therefore spin — observe, find its own
    /// condition unmet, see an unrelated bit still set, return immediately, and
    /// loop — burning the executor and starving the very actor that would make
    /// its condition (a queued stream, or a closed connection) true.
    ///
    /// Edge detection on the epoch is the correct model here: `notify` advances
    /// the epoch on every relevant event (a pushed stream, shutdown), so the
    /// waiter re-checks its own condition exactly when something changed and
    /// otherwise stays parked. The caller must capture `observed` via `observe`
    /// BEFORE re-checking its condition so a notify between the check and the
    /// park cannot be lost.
    pub fn waitEpoch(ws: *WaitSet, io: std.Io, observed: u32) std.Io.Cancelable!void {
        return ws.signal.wait(io, observed);
    }

    pub fn waitEpochTimeout(ws: *WaitSet, io: std.Io, observed: u32, timeout: std.Io.Timeout) std.Io.Cancelable!void {
        return ws.signal.waitTimeout(io, observed, timeout);
    }
};

test "connection waitset coalesces typed readiness" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var waitset: WaitSet = .{};
    waitset.notify(io, .{ .inbound_packets = true });
    waitset.notify(io, .{ .control_commands = true });

    const ready = waitset.take();
    try std.testing.expect(ready.inbound_packets);
    try std.testing.expect(ready.control_commands);
    try std.testing.expect(!ready.stream_outbound);
    try std.testing.expect(!waitset.take().any());
}
