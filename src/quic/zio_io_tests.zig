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
//! contracts before the Layer-0 refactor
//! touches channel.zig / waitset.zig. BoringSSL-free: imports only zio + the
//! io primitives, so it compiles in seconds.
//!
//! Run: `zig build zio-io-test` (optionally `-Dfilter=...`).

const std = @import("std");
const zio = @import("zio");
const channel = @import("io/channel.zig");
const waitset_mod = @import("io/waitset.zig");
const semaphore = @import("io/semaphore.zig");

const testing = std.testing;
const Signal = channel.Signal;
const WaitSet = waitset_mod.WaitSet;
const Ready = waitset_mod.Ready;
const Semaphore = semaphore.Semaphore;

// Pull in semaphore.zig's own (non-blocking-path) unit test under this target too.
test {
    _ = @import("io/semaphore.zig");
}

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

// ---------------------------------------------------------------------------
// Regression: zio kqueue completion-lifecycle UAF. N fibers each blocked in a
// receiveManyTimeout on their own socket, cancelled concurrently cross-executor.
// On stock zio v0.12.0 this crashed 40/40 (poll() dereferenced a Completion freed
// by a resumed fiber via a stale kevent udata). The epoll-style fix (lalinsky/zio
// #443) keys pending completions by (ident,filter) in a loop-owned `poll_queue`
// and stops storing raw Completion pointers in udata, so stale events can't deref.
// ---------------------------------------------------------------------------
const io_time = @import("io/time.zig");
const ReproFut = std.Io.Future((std.Io.Cancelable || std.Io.ConcurrentError)!void);

fn reproCancelOne(io: std.Io, fut: *ReproFut) void {
    fut.cancel(io) catch {};
}

fn reproBareRecv(io: std.Io) (std.Io.Cancelable || std.Io.ConcurrentError)!void {
    var addr: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const socket = std.Io.net.IpAddress.bind(&addr, io, .{ .mode = .dgram }) catch return;
    defer socket.close(io);
    var data_buf: [2048]u8 = undefined;
    var ctrl_buf: [256]u8 align(8) = undefined;
    var messages = [_]std.Io.net.IncomingMessage{.{
        .from = undefined,
        .data = undefined,
        .control = &ctrl_buf,
        .flags = undefined,
    }};
    const maybe_err, _ = socket.receiveManyTimeout(io, &messages, &data_buf, .{}, .none);
    if (maybe_err) |e| switch (e) {
        error.Canceled => return error.Canceled,
        else => return,
    };
}

test "kqueue: concurrent cross-exec cancel of N recvmsg fibers is UAF-free" {
    const N = 8;
    try runRoot(2, struct {
        fn root(io: std.Io, _: void) !void {
            var futs: [N]ReproFut = undefined;
            for (&futs) |*f| f.* = try std.Io.concurrent(io, reproBareRecv, .{io});
            io_time.ms(150).sleep(io) catch {}; // let all park in recvmsg
            var group: std.Io.Group = .init;
            for (&futs) |*f| try group.concurrent(io, reproCancelOne, .{ io, f });
            group.await(io) catch {};
        }
    }.root, {});
}

// ---------------------------------------------------------------------------
// Cancellation-wakeup regressions: an outer fiber blocked in `std.Io.Select.await()`
// over `concurrent` branches (recv, idle timer, futex wait), cancelled via its
// `Future`. These confirm a fiber parked in recv/futex/Select wakes on cancel and
// tears down cleanly. (They do NOT reproduce the router's real teardown deadlock,
// which came from driving teardown off the one-shot `Future.cancel` — see the NOTE
// at the bottom and `router.closeListener`.)
// ---------------------------------------------------------------------------
const SelResult = union(enum) {
    recv: (std.Io.Cancelable || std.Io.ConcurrentError)!void,
    idle: std.Io.Cancelable!void,
};
const Sel = std.Io.Select(SelResult);

fn selIdle(io: std.Io) std.Io.Cancelable!void {
    return io_time.ms(60_000).sleep(io);
}

fn selOuterLoop(io: std.Io) (std.Io.Cancelable || std.Io.ConcurrentError)!void {
    while (true) {
        try std.Io.checkCancel(io);
        var buf: [2]SelResult = undefined;
        var sel = Sel.init(io, &buf);
        defer while (sel.cancel()) |_| {};
        try sel.concurrent(.recv, reproBareRecv, .{io});
        try sel.concurrent(.idle, selIdle, .{io});
        _ = try sel.await();
    }
}

test "select-nested recv loop: cancelling the outer fiber tears down cleanly" {
    try runRoot(2, struct {
        fn root(io: std.Io, _: void) !void {
            var fut: ReproFut = try std.Io.concurrent(io, selOuterLoop, .{io});
            io_time.ms(150).sleep(io) catch {}; // let it park in select.await over the blocked recv
            fut.cancel(io) catch {};
        }
    }.root, {});
}

// The router's SECOND select branch is `route_commands.wait` == `io.futexWait`,
// not a timer. Cancelling a futex-blocked fiber — directly, then nested in a
// Select alongside an unbounded recv (the exact router shape) — also wakes it.
const FutexFut = std.Io.Future(std.Io.Cancelable!void);

fn futexBlockForever(io: std.Io) std.Io.Cancelable!void {
    var word: std.atomic.Value(u32) = .init(0);
    return io.futexWait(u32, &word.raw, 0);
}

test "futex: cancelling a fiber parked in futexWait wakes it" {
    try runRoot(2, struct {
        fn root(io: std.Io, _: void) !void {
            var fut: FutexFut = try std.Io.concurrent(io, futexBlockForever, .{io});
            io_time.ms(150).sleep(io) catch {};
            fut.cancel(io) catch {};
        }
    }.root, {});
}

const SelFx = union(enum) {
    recv: (std.Io.Cancelable || std.Io.ConcurrentError)!void,
    futex: std.Io.Cancelable!void,
};
const SelFxT = std.Io.Select(SelFx);

fn fxWait(io: std.Io) std.Io.Cancelable!void {
    var word: std.atomic.Value(u32) = .init(0);
    return io.futexWait(u32, &word.raw, 0);
}

fn fxOuterLoop(io: std.Io) (std.Io.Cancelable || std.Io.ConcurrentError)!void {
    while (true) {
        try std.Io.checkCancel(io);
        var buf: [2]SelFx = undefined;
        var sel = SelFxT.init(io, &buf);
        defer while (sel.cancel()) |_| {};
        try sel.concurrent(.recv, reproBareRecv, .{io});
        try sel.concurrent(.futex, fxWait, .{io});
        _ = try sel.await();
    }
}

test "select recv+futex loop: cancelling the outer fiber tears down cleanly" {
    try runRoot(2, struct {
        fn root(io: std.Io, _: void) !void {
            var fut: ReproFut = try std.Io.concurrent(io, fxOuterLoop, .{io});
            io_time.ms(150).sleep(io) catch {};
            fut.cancel(io) catch {};
        }
    }.root, {});
}

// NOTE: the router's teardown deadlock was deterministic, not a scheduler race.
// `std.Io.Future.cancel` is one-shot, so cancel-based teardown of a fiber parked in
// the router's multi-arm `Select` can lose the single cancellation and re-park with
// no waker. The router therefore stops off a persistent flag + channel-close, never
// `cancel` (see `router.closeListener`); these single-cancel cases don't hit it.

// ---------------------------------------------------------------------------
// Semaphore — counting admission gate (E3). Caps concurrent permit-holders at N
// across executors via the observe-before-wait Signal; blocked acquirers wake
// on a cross-executor release.
// ---------------------------------------------------------------------------

const SemCtx = struct {
    sem: Semaphore,
    capacity: usize,
    workers: usize,
    inflight: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),
    completed: std.atomic.Value(usize) = .init(0),
};

fn semWorker(io: std.Io, ctx: *SemCtx) void {
    ctx.sem.acquire(io) catch return;
    defer ctx.sem.release(io);
    const now = ctx.inflight.fetchAdd(1, .acq_rel) + 1;
    var p = ctx.peak.load(.acquire);
    while (now > p) {
        if (ctx.peak.cmpxchgWeak(p, now, .acq_rel, .acquire)) |actual| p = actual else break;
    }
    // Hold the permit briefly so workers actually overlap and contend for slots.
    io_time.ms(2).sleep(io) catch {};
    _ = ctx.inflight.fetchSub(1, .acq_rel);
    _ = ctx.completed.fetchAdd(1, .acq_rel);
}

test "Semaphore: caps concurrent holders at N across executors" {
    var ctx = SemCtx{ .sem = Semaphore.init(3), .capacity = 3, .workers = 20 };
    try runRoot(4, struct {
        fn root(io: std.Io, c: *SemCtx) !void {
            var group: std.Io.Group = .init;
            var i: usize = 0;
            while (i < c.workers) : (i += 1) {
                try group.concurrent(io, semWorker, .{ io, c });
            }
            try group.await(io);
        }
    }.root, &ctx);
    // All admitted eventually; the gate NEVER let more than `capacity` run at once.
    try testing.expectEqual(ctx.workers, ctx.completed.load(.acquire));
    try testing.expect(ctx.peak.load(.acquire) <= ctx.capacity);
    // And it did exercise real concurrency (not a vacuous serialized run).
    try testing.expect(ctx.peak.load(.acquire) >= 2);
    // Deterministic invariant on the semaphore's OWN state (the inflight side
    // counter can lag true occupancy): every acquire was paired with exactly one
    // release, so all permits are back. A lost or doubled wake-one would surface
    // as a worker that never completed (caught above) or a permit imbalance here.
    try testing.expectEqual(ctx.capacity, ctx.sem.permits.load(.acquire));
}
