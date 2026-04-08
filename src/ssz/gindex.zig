//! # SSZ Generalized Index Arithmetic
//!
//! A generalized index (gindex) is an integer that identifies a node in an
//! SSZ Merkle binary tree. The root is gindex 1. The left child of node N is
//! 2*N, the right child is 2*N+1. Leaves are at the greatest depth.
//!
//! This module provides comptime helpers for computing gindexes from SSZ type
//! layouts, enabling zero-cost field access in the typed view layer.
//!
//! ## SSZ Gindex Layout
//!
//! For a Container with N fields:
//!   - Pad = nextPow2(N)   (next power of two ≥ N)
//!   - Depth = log2(Pad)   (levels from container root to field leaf)
//!   - Field i gindex (relative to container root = 1): Pad + i
//!   - Field i absolute gindex: container_base * Pad + i
//!
//! For a List[T, limit]:
//!   - Data root gindex  = base * 2
//!   - Length mixin gindex = base * 2 + 1
//!   - Element i data gindex = data_root * nextPow2(limit) + i
//!
//! For a Vector[T, n]:
//!   - Element i gindex = base * nextPow2(n) + i

const std = @import("std");

/// A generalized Merkle tree index. Root = 1; left(n)=2n; right(n)=2n+1.
pub const Gindex = u64;

/// Returns the smallest power of two that is ≥ x.
/// nextPow2(0) = 1, nextPow2(1) = 1, nextPow2(2) = 2, nextPow2(3) = 4, …
pub fn nextPow2(x: u64) u64 {
    if (x <= 1) return 1;
    var v: u64 = x - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

/// Returns floor(log2(x)) for x > 0.
pub fn log2Floor(x: u64) u6 {
    std.debug.assert(x > 0);
    return @intCast(63 - @clz(x));
}

/// Returns ceil(log2(x)) for x > 0. ceil_log2(1) = 0.
pub fn log2Ceil(x: u64) u6 {
    if (x <= 1) return 0;
    return log2Floor(x - 1) + 1;
}

/// Computes the absolute gindex of field `field_idx` within a container
/// whose root is at absolute gindex `base` and has `num_fields` fields.
///
///   field_gindex = base * nextPow2(num_fields) + field_idx
///
/// This is a comptime expression when both `base` and `field_idx` are
/// comptime: the multiply becomes a compile-time constant.
pub inline fn containerFieldGindex(base: Gindex, comptime num_fields: u64, comptime field_idx: u64) Gindex {
    const pad = comptime nextPow2(num_fields);
    return base * pad + field_idx;
}

/// Computes the gindex of element `index` within a List or Vector whose
/// data root is at `data_gindex` and whose outer limit (capacity) is `limit`.
///
///   element_gindex = data_gindex * nextPow2(limit) + index
pub inline fn listElementGindex(data_gindex: Gindex, comptime limit: u64, index: u64) Gindex {
    const pad = comptime nextPow2(limit);
    return data_gindex * pad + index;
}

/// The left child of gindex `g`.
pub inline fn leftChild(g: Gindex) Gindex {
    return g * 2;
}

/// The right child of gindex `g`.
pub inline fn rightChild(g: Gindex) Gindex {
    return g * 2 + 1;
}

/// The parent of gindex `g` (root has no parent; do not call for g == 1).
pub inline fn parentOf(g: Gindex) Gindex {
    return g >> 1;
}

/// Returns the depth of gindex `g` from the root (root has depth 0).
pub inline fn depthOf(g: Gindex) u6 {
    std.debug.assert(g > 0);
    return log2Floor(g);
}

// ─── Tests ────────────────────────────────────────────────────────────

test "nextPow2" {
    const testing = std.testing;
    try testing.expectEqual(@as(u64, 1), nextPow2(0));
    try testing.expectEqual(@as(u64, 1), nextPow2(1));
    try testing.expectEqual(@as(u64, 2), nextPow2(2));
    try testing.expectEqual(@as(u64, 4), nextPow2(3));
    try testing.expectEqual(@as(u64, 4), nextPow2(4));
    try testing.expectEqual(@as(u64, 8), nextPow2(5));
    try testing.expectEqual(@as(u64, 32), nextPow2(28));
    try testing.expectEqual(@as(u64, 1 << 40), nextPow2(1 << 40));
}

test "log2Floor and log2Ceil" {
    const testing = std.testing;
    try testing.expectEqual(@as(u6, 0), log2Floor(1));
    try testing.expectEqual(@as(u6, 1), log2Floor(2));
    try testing.expectEqual(@as(u6, 1), log2Floor(3));
    try testing.expectEqual(@as(u6, 2), log2Floor(4));
    try testing.expectEqual(@as(u6, 5), log2Floor(32));
    try testing.expectEqual(@as(u6, 0), log2Ceil(1));
    try testing.expectEqual(@as(u6, 1), log2Ceil(2));
    try testing.expectEqual(@as(u6, 2), log2Ceil(3));
    try testing.expectEqual(@as(u6, 2), log2Ceil(4));
    try testing.expectEqual(@as(u6, 5), log2Ceil(28));
    try testing.expectEqual(@as(u6, 5), log2Ceil(32));
}

test "containerFieldGindex matches beacon state spec" {
    // BeaconState has 28 fields → pad = 32
    // Root at gindex 1 → field i at 32 + i
    const testing = std.testing;
    try testing.expectEqual(@as(Gindex, 34), containerFieldGindex(1, 28, 2)); // slot
    try testing.expectEqual(@as(Gindex, 43), containerFieldGindex(1, 28, 11)); // validators
    try testing.expectEqual(@as(Gindex, 44), containerFieldGindex(1, 28, 12)); // balances
}

test "listElementGindex" {
    const testing = std.testing;
    // Validator 0 data gindex from validators list (base 43, data at 86, limit 2^40)
    const data_g: Gindex = 86;
    const limit: u64 = 1 << 40;
    try testing.expectEqual(data_g * (1 << 40) + 0, listElementGindex(data_g, limit, 0));
    try testing.expectEqual(data_g * (1 << 40) + 7, listElementGindex(data_g, limit, 7));
}
