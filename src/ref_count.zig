//! Generic atomically reference-counted box: shared ownership of one heap value,
//! no weak refs, no cycle handling. The atomic counter lets the box be shared
//! across fibers/threads; the wrapper (and its `T`) is freed exactly once, on the
//! 1→0 transition. Ordering rationale lives in `AtomicRc` and the per-method docs.

const std = @import("std");

/// Intrusive atomic refcount: embed as a field of an owning struct that calls
/// `retain`/`release` and does its own at-zero cleanup + free. Count starts at 1;
/// `release*` returns `true` exactly once, on the 1→0 transition.
///
/// Two ordering flavors — pick ONE per owner and use it consistently:
///
///   * `retain` / `release` — lighter: `.monotonic` bump, `.release` decrement +
///     acquire fence on the 1→0 transition.
///
///   * `retainChecked` / `releaseChecked` — `.acq_rel` both directions, asserting
///     the prior count was positive to catch retain-after-free / over-release in
///     debug builds. Strictly stronger, so the at-zero reader still observes every
///     prior holder's writes.
pub const AtomicRc = struct {
    n: std.atomic.Value(usize) = .init(1),

    /// Start the count at `initial` instead of the default 1, for an owner that
    /// is minted with several intended holders at once.
    pub fn initCount(initial: usize) AtomicRc {
        return .{ .n = .init(initial) };
    }

    /// Add one holder (lighter pattern). Unordered: a new holder becomes reachable
    /// to other threads only through a synchronizing edge that already carries the
    /// happens-before, so the bump needs no ordering.
    pub fn retain(self: *AtomicRc) void {
        _ = self.n.fetchAdd(1, .monotonic);
    }

    /// Give up one holder (lighter pattern). Returns `true` iff this was the LAST
    /// reference, the caller's signal to run cleanup + free. The `.release`
    /// decrement plus `.acquire` fence on the 1→0 transition establishes
    /// happens-before with every prior holder's writes before reclamation.
    pub fn release(self: *AtomicRc) bool {
        if (self.n.fetchSub(1, .release) != 1) return false;
        _ = self.n.load(.acquire); // acquire fence on the 1→0 transition
        return true;
    }

    /// Add one holder (QUIC lifecycle pattern): `.acq_rel`, asserting the prior
    /// count was positive to catch a retain-after-free in debug builds.
    pub fn retainChecked(self: *AtomicRc) void {
        const previous = self.n.fetchAdd(1, .acq_rel);
        std.debug.assert(previous > 0);
    }

    /// Give up one holder (QUIC lifecycle pattern): `.acq_rel`, asserting the prior
    /// count was positive to catch an over-release in debug builds. Returns `true`
    /// iff this was the LAST reference; the caller then runs cleanup + free.
    pub fn releaseChecked(self: *AtomicRc) bool {
        const previous = self.n.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        return previous == 1;
    }

    /// Current count (best-effort, unordered) — for assertions/diagnostics, not
    /// a synchronization primitive.
    pub fn count(self: *const AtomicRc) usize {
        return self.n.load(.monotonic);
    }
};

/// An atomically reference-counted, heap-allocated `T`.
pub fn RefCount(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        refs: std.atomic.Value(usize),
        value: T,

        const Self = @This();

        /// Heap-allocate the wrapper with `value` and an initial reference count
        /// of 1 (the caller owns that one reference). On the final `release` the
        /// allocator frees the wrapper (after deinit-ing `value`).
        pub fn init(allocator: std.mem.Allocator, value: T) std.mem.Allocator.Error!*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .refs = .init(1),
                .value = value,
            };
            return self;
        }

        /// Add one holder's reference; returns `self` for chaining. Unordered: a
        /// freshly retained box becomes reachable to other threads only through a
        /// synchronizing edge that already carries the happens-before.
        pub fn retain(self: *Self) *Self {
            _ = self.refs.fetchAdd(1, .monotonic);
            return self;
        }

        /// Give up one holder's reference. On the 1→0 transition, deinits the inner
        /// value and frees the wrapper. The `.release` decrement plus `.acquire`
        /// fence establishes happens-before with every prior holder's writes before
        /// reclamation.
        pub fn release(self: *Self) void {
            if (self.refs.fetchSub(1, .release) != 1) return;
            _ = self.refs.load(.acquire);
            const allocator = self.allocator;
            deinitValue(&self.value, allocator);
            allocator.destroy(self);
        }

        /// Pointer to the wrapped value (stable while any reference is held).
        pub fn get(self: *Self) *T {
            return &self.value;
        }

        /// Comptime-dispatch the inner deinit: `deinit(allocator)` if `T.deinit`
        /// takes an allocator as its second parameter, `deinit()` if it takes only
        /// self, nothing if `T` has no `deinit`.
        fn deinitValue(value: *T, allocator: std.mem.Allocator) void {
            if (!@hasDecl(T, "deinit")) return;
            const params = @typeInfo(@TypeOf(T.deinit)).@"fn".params;
            if (params.len >= 2) {
                value.deinit(allocator);
            } else {
                value.deinit();
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "RefCount(T) without deinit: init then release frees once" {
    const allocator = std.testing.allocator;
    const Boxed = struct { n: u32 };
    const rc = try RefCount(Boxed).init(allocator, .{ .n = 7 });
    try std.testing.expectEqual(@as(usize, 1), rc.refs.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 7), rc.get().n);
    rc.release(); // refs 1→0, wrapper freed; testing.allocator confirms no leak.
}

test "RefCount(T) retain/release balance: freed only on the last release" {
    const allocator = std.testing.allocator;
    const Boxed = struct { n: u32 };
    const rc = try RefCount(Boxed).init(allocator, .{ .n = 1 });
    _ = rc.retain();
    _ = rc.retain();
    try std.testing.expectEqual(@as(usize, 3), rc.refs.load(.monotonic));
    rc.release();
    rc.release();
    // Still one holder: the value is live (no premature free / use-after-free).
    try std.testing.expectEqual(@as(usize, 1), rc.refs.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), rc.get().n);
    rc.release(); // final release frees the wrapper; testing.allocator checks it.
}

test "RefCount(T) calls a deinit(allocator) exactly once at refs==0" {
    const allocator = std.testing.allocator;
    // A T that owns a heap slice; its deinit frees it with the allocator passed
    // through by RefCount. A missed call leaks the slice; a double call is a
    // double-free — testing.allocator catches both.
    const Owner = struct {
        buf: []u8,
        fn deinit(self: *@This(), a: std.mem.Allocator) void {
            a.free(self.buf);
        }
    };
    const buf = try allocator.alloc(u8, 8);
    const rc = try RefCount(Owner).init(allocator, .{ .buf = buf });
    _ = rc.retain();
    rc.release(); // not the last reference: deinit must NOT run yet.
    try std.testing.expectEqual(@as(usize, 1), rc.refs.load(.monotonic));
    rc.release(); // last reference: deinit(allocator) runs exactly once.
}

test "RefCount(T) calls a no-allocator deinit() exactly once at refs==0" {
    const allocator = std.testing.allocator;
    // A T whose deinit takes only self. RefCount must pick the zero-arg form.
    const Counter = struct {
        flag: *bool,
        fn deinit(self: *@This()) void {
            self.flag.* = true;
        }
    };
    var called = false;
    const rc = try RefCount(Counter).init(allocator, .{ .flag = &called });
    rc.release();
    try std.testing.expect(called);
}

test "AtomicRc starts at 1; release returns was-last exactly on 1->0" {
    var rc: AtomicRc = .{};
    try std.testing.expectEqual(@as(usize, 1), rc.count());
    // The sole reference is the last one.
    try std.testing.expect(rc.release());
}

test "AtomicRc retain/release balance: was-last only on the final release" {
    var rc: AtomicRc = .{};
    rc.retain();
    rc.retain();
    try std.testing.expectEqual(@as(usize, 3), rc.count());
    try std.testing.expect(!rc.release()); // 3->2
    try std.testing.expect(!rc.release()); // 2->1
    try std.testing.expectEqual(@as(usize, 1), rc.count());
    try std.testing.expect(rc.release()); // 1->0, last
}

test "AtomicRc initCount mints several holders" {
    var rc: AtomicRc = .initCount(2);
    try std.testing.expectEqual(@as(usize, 2), rc.count());
    try std.testing.expect(!rc.release()); // 2->1
    try std.testing.expect(rc.release()); // 1->0, last
}

test "AtomicRc checked variant: same balance and was-last semantics" {
    var rc: AtomicRc = .initCount(2);
    rc.retainChecked();
    try std.testing.expectEqual(@as(usize, 3), rc.count());
    try std.testing.expect(!rc.releaseChecked()); // 3->2
    try std.testing.expect(!rc.releaseChecked()); // 2->1
    try std.testing.expect(rc.releaseChecked()); // 1->0, last
}
