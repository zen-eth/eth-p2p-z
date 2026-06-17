//! A small, generic, atomically reference-counted box: shared ownership of one
//! heap value, no weak references and no cycle handling.
//!
//! `RefCount(T)` heap-allocates a wrapper holding a `T` and an atomic counter.
//! `init` mints it with one reference; each holder `retain`s to add a reference
//! and `release`s to drop one. The wrapper (and its `T`) is freed exactly once,
//! on the 1→0 transition. The counter is atomic so the box can be shared across
//! fibers/threads; for the ordering rationale see `AtomicRc` and the per-method
//! docs.
//!
//! The inner `T.deinit` (if present) is dispatched at comptime: a `deinit` that
//! takes an allocator is called as `value.deinit(allocator)`, one that takes only
//! self as `value.deinit()`, and a `T` with no `deinit` skips the call entirely.

const std = @import("std");

/// Intrusive atomic refcount: embed as a field of an owning struct; the owner
/// calls `retain`/`release` and performs its own at-zero cleanup + free. The
/// counter mechanics live here so every owner shares one audited implementation;
/// the struct layout, allocation, pointer types, and call sites stay the owner's.
///
/// Two ordering flavors — pick ONE per owner and use it consistently:
///
///   * `retain` / `release` — the lighter pattern: `.monotonic` bump, `.release`
///     decrement + acquire fence on the 1→0 transition.
///
///   * `retainChecked` / `releaseChecked` — both directions use `.acq_rel` and
///     assert the prior count was positive, catching a retain-after-free or an
///     over-release in debug builds on UAF-sensitive primitives. `.acq_rel` is
///     strictly stronger than the lighter pattern, so the at-zero reader still
///     observes every prior holder's writes.
///
/// Either way the count starts at 1 and `release*` returns `true` exactly once,
/// on the 1→0 transition — the caller's signal to run cleanup and free.
pub const AtomicRc = struct {
    n: std.atomic.Value(usize) = .init(1),

    /// Start the count at `initial` instead of the default 1, for an owner that
    /// is minted with several intended holders at once.
    pub fn initCount(initial: usize) AtomicRc {
        return .{ .n = .init(initial) };
    }

    /// Add one holder (lighter pattern). Unordered: a new holder becomes
    /// reachable to other threads only through a synchronizing edge that carries
    /// the happens-before, so the bump needs no ordering.
    pub fn retain(self: *AtomicRc) void {
        _ = self.n.fetchAdd(1, .monotonic);
    }

    /// Give up one holder (lighter pattern). Returns `true` iff this was the LAST
    /// reference; the caller must then run its cleanup + free. The decrement is
    /// `.release` and the freeing thread issues an `.acquire` fence on the 1→0
    /// transition, establishing happens-before with every prior holder's writes.
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

    /// Give up one holder (QUIC lifecycle pattern): `.acq_rel`, asserting the
    /// prior count was positive to catch an over-release in debug builds.
    /// Returns `true` iff this was the LAST reference; the caller then runs its
    /// own cleanup + free.
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

        /// Add one holder's reference and return `self` for convenient chaining.
        /// Cheap and unordered: a freshly retained box only becomes reachable to
        /// other threads through a synchronizing edge that carries the
        /// happens-before, so the bump itself needs no ordering.
        pub fn retain(self: *Self) *Self {
            _ = self.refs.fetchAdd(1, .monotonic);
            return self;
        }

        /// Give up one holder's reference. On the 1→0 transition this deinits the
        /// inner value and frees the wrapper. The decrement is `.release` and the
        /// freeing thread issues an `.acquire` fence first, establishing
        /// happens-before with every prior holder's writes before reclamation.
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
