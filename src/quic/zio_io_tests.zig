//! Multi-executor integration tests for the lowest IO primitives (Layer 0).
//!
//! WHY THIS FILE EXISTS: every other test in the tree runs on `std.Io.Threaded`
//! and is single-threaded + sequential (trySend/tryRecv with no blocking
//! handoff), so the cross-executor wake / backpressure / refcount contracts that
//! make the model multi-executor-safe are NOT exercised anywhere. That gap is
//! exactly what let zio v0.11's stubbed `batchAwaitConcurrent` reach interop.
//!
//! These tests spin a REAL `zio.Runtime` with >=2 executors and put producers
//! and consumers on different executors, so they pin the Invariant-Ledger
//! contracts (see docs/quiche-refactor-plan.md) before the Layer-0 refactor
//! touches channel.zig / waitset.zig. BoringSSL-free: imports only zio + the
//! io primitives, so it compiles in seconds.
//!
//! Run: `zig build zio-io-test` (optionally `-Dfilter=...`).

const std = @import("std");
const zio = @import("zio");
const channel = @import("io/channel.zig");
const waitset_mod = @import("io/waitset.zig");

const testing = std.testing;
const Signal = channel.Signal;
const WaitSet = waitset_mod.WaitSet;
const Ready = waitset_mod.Ready;

/// Spin a multi-executor zio runtime, run `root(io, ctx)` as the root fiber on
/// the main executor, and block until it completes. Worker fibers spawned via
/// `std.Io.concurrent` land on the other executor(s), giving genuine
/// cross-executor scheduling. Propagates the root fiber's error.
fn runRoot(comptime executors: u8, comptime root: anytype, ctx: anytype) !void {
    const rt = try zio.Runtime.init(testing.allocator, .{ .executors = .exact(executors) });
    defer rt.deinit();
    var handle = try rt.spawn(root, .{ rt.io(), ctx });
    defer handle.cancel();
    return handle.join();
}

// ---------------------------------------------------------------------------
// Signal — the futex-backed epoch wake (observe-before-wait, notify wakes)
// ---------------------------------------------------------------------------

const SignalCtx = struct {
    sig: Signal = .{},
    /// Producer publishes its data here before notifying; consumer reads after waking.
    payload: std.atomic.Value(u64) = .init(0),
    /// Set by the consumer once it has observed the payload, so the test can assert it ran.
    observed_payload: std.atomic.Value(u64) = .init(0),
};

fn signalConsumer(io: std.Io, ctx: *SignalCtx) void {
    // Mirror the production observe-before-wait loop: snapshot the epoch, check
    // the condition, and only park if it is not yet satisfied. A notify that
    // races between observe() and wait() bumps the epoch, so wait() returns
    // immediately rather than blocking forever (the lost-wakeup-free contract).
    while (true) {
        const observed = ctx.sig.observe();
        const value = ctx.payload.load(.acquire);
        if (value != 0) {
            ctx.observed_payload.store(value, .release);
            return;
        }
        ctx.sig.wait(io, observed) catch return;
    }
}

test "Signal: cross-executor wake delivers published payload" {
    var ctx = SignalCtx{};
    try runRoot(2, struct {
        fn root(io: std.Io, c: *SignalCtx) !void {
            var consumer = try std.Io.concurrent(io, signalConsumer, .{ io, c });
            // Publish THEN notify — the consumer must observe the payload whether
            // it had already parked (futexWake) or not yet observed (epoch check).
            c.payload.store(0xABCD, .release);
            c.sig.notify(io);
            consumer.await(io);
        }
    }.root, &ctx);
    try testing.expectEqual(@as(u64, 0xABCD), ctx.observed_payload.load(.acquire));
}

// ---------------------------------------------------------------------------
// WaitSet — atomic typed-readiness bitset coalesced behind one futex
// ---------------------------------------------------------------------------

const WaitSetCtx = struct {
    ws: WaitSet = .{},
    saw_inbound: std.atomic.Value(bool) = .init(false),
    saw_outbound: std.atomic.Value(bool) = .init(false),
};

fn waitsetConsumer(io: std.Io, ctx: *WaitSetCtx) void {
    // Accumulate readiness bits until shutdown is signalled. Multiple notifies
    // between two take()s must coalesce (fetchOr), never be lost.
    while (true) {
        const observed = ctx.ws.observe();
        const ready = ctx.ws.take();
        if (ready.inbound_packets) ctx.saw_inbound.store(true, .release);
        if (ready.stream_outbound) ctx.saw_outbound.store(true, .release);
        if (ready.shutdown) return;
        ctx.ws.wait(io, observed) catch return;
    }
}

test "WaitSet: coalesces typed readiness across executors" {
    var ctx = WaitSetCtx{};
    try runRoot(2, struct {
        fn root(io: std.Io, c: *WaitSetCtx) !void {
            var consumer = try std.Io.concurrent(io, waitsetConsumer, .{ io, c });
            c.ws.notify(io, .{ .inbound_packets = true });
            c.ws.notify(io, .{ .stream_outbound = true });
            c.ws.notify(io, .{ .shutdown = true });
            consumer.await(io);
        }
    }.root, &ctx);
    try testing.expect(ctx.saw_inbound.load(.acquire));
    try testing.expect(ctx.saw_outbound.load(.acquire));
}

// ---------------------------------------------------------------------------
// Bounded channel — blocking slot handoff across executors
// ---------------------------------------------------------------------------

const ChanU8 = channel.Bounded(u8, null, null);

const SlotCtx = struct {
    state: *ChanU8.State = undefined,
    count: usize,
    received: std.atomic.Value(u64) = .init(0),
    sum: std.atomic.Value(u64) = .init(0),
};

fn slotConsumer(io: std.Io, ctx: *SlotCtx) void {
    var n: usize = 0;
    while (n < ctx.count) : (n += 1) {
        const v = ctx.state.receiver().recv(io) catch break;
        _ = ctx.sum.fetchAdd(v, .acq_rel);
    }
    ctx.received.store(n, .release);
}

test "Bounded channel: blocking recv wakes on cross-executor send" {
    const count: usize = 32;
    var ctx = SlotCtx{ .count = count };
    try runRoot(2, struct {
        fn root(io: std.Io, c: *SlotCtx) !void {
            // Capacity 2 << 32 items, so the sender repeatedly fills the queue and
            // blocks until the consumer (other executor) drains it.
            const state = try ChanU8.State.init(testing.allocator, io, 2, 0);
            defer state.release();
            c.state = state;
            var consumer = try std.Io.concurrent(io, slotConsumer, .{ io, c });
            var i: usize = 0;
            while (i < c.count) : (i += 1) {
                try state.sender().send(io, @intCast(i + 1));
            }
            consumer.await(io);
            state.close(io);
        }
    }.root, &ctx);
    try testing.expectEqual(@as(u64, count), ctx.received.load(.acquire));
    // sum 1..=32 = 528
    try testing.expectEqual(@as(u64, 528), ctx.sum.load(.acquire));
}

// ---------------------------------------------------------------------------
// Bounded channel — byte-budget backpressure (the `writable` Signal path)
// ---------------------------------------------------------------------------

fn slice_cost(item: *const []const u8) usize {
    return item.len;
}
const ChanSlice = channel.Bounded([]const u8, null, slice_cost);

const ByteCtx = struct {
    state: *ChanSlice.State = undefined,
    count: usize,
    received: std.atomic.Value(u64) = .init(0),
};

fn byteConsumer(io: std.Io, ctx: *ByteCtx) void {
    var n: usize = 0;
    while (n < ctx.count) : (n += 1) {
        _ = ctx.state.receiver().recv(io) catch break;
    }
    ctx.received.store(n, .release);
}

test "Bounded channel: byte backpressure blocks sender until consumer frees budget" {
    const count: usize = 16;
    var ctx = ByteCtx{ .count = count };
    try runRoot(2, struct {
        fn root(io: std.Io, c: *ByteCtx) !void {
            // byte_capacity = 4 with 3-byte items => at most one item in flight;
            // every send past the first must block on `writable` until the
            // consumer's recv -> releaseCost -> writable.notify frees the budget.
            const state = try ChanSlice.State.init(testing.allocator, io, 8, 4);
            defer state.release();
            c.state = state;
            var consumer = try std.Io.concurrent(io, byteConsumer, .{ io, c });
            var i: usize = 0;
            while (i < c.count) : (i += 1) {
                try state.sender().send(io, "abc");
            }
            consumer.await(io);
            state.close(io);
        }
    }.root, &ctx);
    try testing.expectEqual(@as(u64, count), ctx.received.load(.acquire));
}

// ---------------------------------------------------------------------------
// Bounded channel — refcount free-on-last under concurrent retain/release
// ---------------------------------------------------------------------------

const RefCtx = struct {
    state: *ChanU8.State = undefined,
    workers: usize,
};

fn refWorker(_: std.Io, ctx: *RefCtx) void {
    // Concurrent retain/release pairs stress the acq_rel refcount. The base ref
    // held by `root` keeps the state alive across all of these, so no worker can
    // observe previous==1 and free it mid-flight.
    var i: usize = 0;
    while (i < 64) : (i += 1) {
        ctx.state.retain();
        ctx.state.release();
    }
}

test "Bounded channel: refcount frees exactly once after concurrent retain/release" {
    const workers: usize = 6;
    var ctx = RefCtx{ .workers = workers };
    // testing.allocator asserts no leak (under-free) and no double-free (over-free).
    try runRoot(4, struct {
        fn root(io: std.Io, c: *RefCtx) !void {
            const state = try ChanU8.State.init(testing.allocator, io, 4, 0);
            c.state = state;
            var group: std.Io.Group = .init;
            var i: usize = 0;
            while (i < c.workers) : (i += 1) {
                try group.concurrent(io, refWorker, .{ io, c });
            }
            try group.await(io);
            // Drop the base ref last -> previous==1 -> exactly one free here.
            state.release();
        }
    }.root, &ctx);
}
