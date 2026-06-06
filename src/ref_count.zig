//! A small, generic, atomically reference-counted box, modeled on a standard
//! atomic refcount (no weak references, no cycle handling — just shared
//! ownership of one heap value).
//!
//! `RefCount(T)` heap-allocates a wrapper holding a `T` and an atomic counter.
//! `init` mints it with one reference; each holder calls `retain` to add a
//! reference and `release` to drop one. The wrapper (and its `T`) is freed
//! exactly once, on the reference's 1→0 transition. The counter is atomic so the
//! box can be shared across fibers/threads — matching the `OutboundFrame`
//! refcount in this codebase, whose ordering this primitive copies:
//!   - `retain` bumps with `.monotonic`: a new holder only becomes reachable to
//!     another thread through some synchronizing edge (a mutex/queue/signal), so
//!     the bump itself needs no ordering.
//!   - `release` decrements with `.release`; the holder that observes the 1→0
//!     transition issues an `.acquire` fence before freeing, establishing
//!     happens-before with every prior holder's writes so the memory is safely
//!     reclaimed.
//!
//! The inner `T.deinit` (if present) is dispatched at comptime: a `deinit` that
//! takes an allocator is called as `value.deinit(allocator)`, one that takes only
//! self as `value.deinit()`, and a `T` with no `deinit` skips the call entirely.

const std = @import("std");

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
