const std = @import("std");
const channel = @import("channel.zig");
const Signal = channel.Signal;

/// A counting semaphore with a cancelable, blocking `acquire` and a
/// non-blocking `release`, built on the project's epoch `Signal`
/// (observe-before-wait). It exists to bound the number of concurrent
/// stream-handler fibers (E3) and composes with the cooperative
/// single-executor-per-fiber teardown model:
///
///   - `acquire` is a CANCEL POINT, and cancellation is the PRIMARY way a
///     parked waiter is unwound at teardown (the dispatcher loop via
///     `Group.cancel`, the one-shot command path via the actor's main-future
///     cancel). `close()`/`error.Closed` is a secondary, belt-and-suspenders
///     PERSISTENT wake — used for the aggregate gate at `Switch.deinit`, where
///     no fiber is actually parked by then.
///   - `release` never blocks or allocates, so it is safe to run from a
///     handler's cleanup `defer` even while that handler is being canceled.
///
/// `permits` is the number of currently-available slots; it never goes
/// negative (acquire only decrements when > 0). Multi-producer / multi-consumer
/// safe: state is a single atomic + the futex-backed `Signal`.
///
/// Deliberately NOT `std.Io.Semaphore`: that one is `Mutex`+`Condition` based,
/// while this project's cross-fiber comms invariant restricts blocking
/// coordination to lock-free `std.Io.Queue`/atomics and the futex-backed
/// `Signal`. This gate follows that invariant (and adds `close()`, which
/// `std.Io.Semaphore` lacks).
pub const Semaphore = struct {
    permits: std.atomic.Value(usize),
    signal: Signal = .{},
    closed: std.atomic.Value(bool) = .init(false),

    pub const AcquireError = error{Closed} || std.Io.Cancelable;

    pub fn init(initial_permits: usize) Semaphore {
        return .{ .permits = .init(initial_permits) };
    }

    /// Take one permit, blocking (cancelably) until one is free or the
    /// semaphore is closed. The `observe()` snapshot is taken BEFORE the
    /// closed-check and claim attempt, so a `release`/`close` that races in
    /// after them bumps the epoch and makes `wait` return immediately rather
    /// than sleeping forever (the lost-wakeup-free contract).
    pub fn acquire(s: *Semaphore, io: std.Io) AcquireError!void {
        while (true) {
            const observed = s.signal.observe();
            if (s.closed.load(.acquire)) return error.Closed;
            if (s.tryClaim()) return;
            try s.signal.wait(io, observed);
        }
    }

    fn tryClaim(s: *Semaphore) bool {
        return channel.tryDecrementToFloor(&s.permits);
    }

    /// Return one permit and wake a single waiter. Non-blocking; safe during
    /// cancel unwinding. Wakes exactly one (release frees exactly one permit);
    /// `close` below wakes all.
    pub fn release(s: *Semaphore, io: std.Io) void {
        _ = s.permits.fetchAdd(1, .release);
        s.signal.notifyOne(io);
    }

    /// Wake all waiters with `error.Closed` (teardown). Idempotent.
    pub fn close(s: *Semaphore, io: std.Io) void {
        s.closed.store(true, .release);
        s.signal.notify(io);
    }
};

test "Semaphore: claim up to capacity, release, and close (non-blocking paths)" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var sem = Semaphore.init(2);
    // permits available → acquire returns immediately (no block).
    try sem.acquire(io); // 2 -> 1
    try sem.acquire(io); // 1 -> 0
    // exhausted: no permits left, so a blocking acquire would now park.
    try std.testing.expectEqual(@as(usize, 0), sem.permits.load(.acquire));
    sem.release(io); // 0 -> 1
    try std.testing.expectEqual(@as(usize, 1), sem.permits.load(.acquire));
    try sem.acquire(io); // 1 -> 0: claims the released permit without blocking.
    try std.testing.expectEqual(@as(usize, 0), sem.permits.load(.acquire));
    // once closed, acquire returns Closed without blocking.
    sem.close(io);
    try std.testing.expectError(error.Closed, sem.acquire(io));
}
