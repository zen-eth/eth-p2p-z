//! # Incremental SHA-256 Merkle Tree Root
//!
//! Computes the SSZ `hash_tree_root` of a Merkle subtree stored in a
//! `MerkleBackend`, visiting only the dirty path (O(dirty × depth) instead
//! of O(total_leaves)).
//!
//! ## Algorithm
//!
//! ```
//! fn incrementalHashTreeRoot(backend, gindex, depth):
//!   if not dirty(gindex):
//!     return cached_hash(gindex) OR zero_hash(depth)
//!   if depth == 0:                      // leaf
//!     hash = cached_hash(gindex) OR chunk(gindex) OR zero_chunk
//!     setHash(gindex, hash); return hash
//!   left  = incrementalHashTreeRoot(backend, gindex*2,   depth-1)
//!   right = incrementalHashTreeRoot(backend, gindex*2+1, depth-1)
//!   hash  = SHA256(left || right)
//!   setHash(gindex, hash); return hash
//! ```
//!
//! ## Zero hashes
//!
//! Empty (never-written) subtrees return the precomputed "zero hash" for
//! their depth:
//!   zero_hash[0] = 0x00…00  (32 bytes)
//!   zero_hash[d] = SHA256(zero_hash[d-1] || zero_hash[d-1])
//!
//! ## Usage notes
//!
//! The caller MUST pass the correct `depth` for the subtree rooted at
//! `gindex`.  For non-uniform trees (e.g. SSZ lists whose data tree and
//! length mixin have different depths), the caller is responsible for
//! splitting the computation and mixing results with `sha256Pair`.

const std = @import("std");
const testing = std.testing;
const Sha256 = std.crypto.hash.sha2.Sha256;

const merkle_backend = @import("../state/merkle_backend.zig");
pub const MerkleBackend = merkle_backend.MerkleBackend;
pub const Hash = merkle_backend.Hash;
pub const Gindex = merkle_backend.Gindex;

// ─── Precomputed zero hashes ─────────────────────────────────────────

/// Maximum depth supported by `zeroHash`. Sufficient for any realistic SSZ tree
/// (e.g. validators list at depth 43, BeaconState container at depth 5).
pub const MAX_ZERO_DEPTH: usize = 64;

/// Cache of precomputed zero hashes. Filled lazily on first call.
var zero_hash_cache: [MAX_ZERO_DEPTH + 1]Hash = undefined;
var zero_hash_cache_ready = false;

/// Return the SSZ zero hash for a subtree of `depth` levels.
///   zeroHash(0) = 0x00…00 (the zero leaf)
///   zeroHash(d) = SHA256(zeroHash(d-1) || zeroHash(d-1))
pub fn zeroHash(depth: usize) Hash {
    if (!zero_hash_cache_ready) buildZeroHashCache();
    return zero_hash_cache[@min(depth, MAX_ZERO_DEPTH)];
}

fn buildZeroHashCache() void {
    zero_hash_cache[0] = std.mem.zeroes(Hash);
    var d: usize = 1;
    while (d <= MAX_ZERO_DEPTH) : (d += 1) {
        zero_hash_cache[d] = sha256Pair(zero_hash_cache[d - 1], zero_hash_cache[d - 1]);
    }
    zero_hash_cache_ready = true;
}

// ─── SHA-256 helpers ─────────────────────────────────────────────────

/// SHA-256 of a 64-byte buffer: the standard SSZ internal-node hash.
pub fn sha256Of64(data: *const [64]u8) Hash {
    var hasher = Sha256.init(.{});
    hasher.update(data);
    return hasher.finalResult();
}

/// SHA-256(left_32 || right_32).  The canonical SSZ Merkle combine step.
pub fn sha256Pair(left: Hash, right: Hash) Hash {
    var buf: [64]u8 = undefined;
    @memcpy(buf[0..32], &left);
    @memcpy(buf[32..64], &right);
    return sha256Of64(&buf);
}

// ─── Incremental hash tree root ──────────────────────────────────────

/// Compute (or retrieve from cache) the hash of the Merkle subtree rooted at
/// `gindex` with `depth` levels below it.
///
/// Complexity: O(dirty_leaves × depth) — only dirty paths are visited.
///
/// After the call, `backend.getHash(gindex)` returns the computed root and
/// `backend.isDirty(gindex)` is false.
pub fn incrementalHashTreeRoot(backend: *MerkleBackend, gindex: Gindex, depth: usize) !Hash {
    // Fast path: subtree is clean.
    if (!backend.isDirty(gindex)) {
        if (backend.getHash(gindex)) |h| return h;
        // Subtree was never touched → return the appropriate zero hash.
        return zeroHash(depth);
    }

    // Leaf: hash = chunk value (or zero if never written).
    if (depth == 0) {
        // A previously-computed hash (from a deeper call) takes precedence.
        if (backend.getHash(gindex)) |h| return h;
        const chunk = backend.getChunk(gindex) orelse std.mem.zeroes(Hash);
        // For leaves, the chunk IS the hash (SSZ leaf = 32-byte padded value).
        try backend.setHash(gindex, chunk);
        return chunk;
    }

    // Internal node: recurse into children and combine.
    const left = try incrementalHashTreeRoot(backend, gindex * 2, depth - 1);
    const right = try incrementalHashTreeRoot(backend, gindex * 2 + 1, depth - 1);
    const result = sha256Pair(left, right);
    try backend.setHash(gindex, result);
    return result;
}

/// Hash a list subtree that has a data tree and a length mixin at different
/// depths.  This is the standard SSZ List mixing step:
///
///   hash_tree_root(list) = SHA256(
///     incrementalHashTreeRoot(data_gindex, data_depth),
///     length_chunk_as_hash
///   )
///
/// The length chunk is read directly (not recursed into) because it occupies
/// a single 32-byte leaf regardless of the list limit.
pub fn listHashTreeRoot(
    backend: *MerkleBackend,
    list_base_gindex: Gindex,
    data_depth: usize,
) !Hash {
    const data_gindex = list_base_gindex * 2;
    const length_gindex = list_base_gindex * 2 + 1;

    const data_hash = try incrementalHashTreeRoot(backend, data_gindex, data_depth);

    // Length mixin: the chunk stored at length_gindex holds the list length
    // as a little-endian uint256 (first 8 bytes suffice for any practical length).
    const length_chunk = backend.getChunk(length_gindex) orelse std.mem.zeroes(Hash);

    return sha256Pair(data_hash, length_chunk);
}

// ─── Tests ────────────────────────────────────────────────────────────

test "zeroHash consistency" {
    const h0 = zeroHash(0);
    try testing.expectEqual(std.mem.zeroes(Hash), h0);

    const h1 = zeroHash(1);
    const expected = sha256Pair(h0, h0);
    try testing.expectEqual(expected, h1);

    const h2 = zeroHash(2);
    try testing.expectEqual(sha256Pair(h1, h1), h2);
}

test "incrementalHashTreeRoot - single leaf" {
    const allocator = testing.allocator;
    const merkle_backend_mod = @import("../state/merkle_backend.zig");
    var backend = try merkle_backend_mod.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    // Write slot value (u64 = 42) at gindex 1 (the only leaf, depth 0).
    var chunk: merkle_backend_mod.Chunk = std.mem.zeroes(merkle_backend_mod.Chunk);
    std.mem.writeInt(u64, chunk[0..8], 42, .little);
    try backend.putChunk(1, chunk);

    const root = try incrementalHashTreeRoot(&backend, 1, 0);
    // Leaf hash = chunk value.
    try testing.expectEqual(chunk, root);
}

test "incrementalHashTreeRoot - two leaves" {
    const allocator = testing.allocator;
    const merkle_backend_mod = @import("../state/merkle_backend.zig");
    var backend = try merkle_backend_mod.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var left_chunk: merkle_backend_mod.Chunk = std.mem.zeroes(merkle_backend_mod.Chunk);
    left_chunk[0] = 0xAA;
    var right_chunk: merkle_backend_mod.Chunk = std.mem.zeroes(merkle_backend_mod.Chunk);
    right_chunk[0] = 0xBB;

    try backend.putChunk(2, left_chunk); // left child of root
    try backend.putChunk(3, right_chunk); // right child of root

    const root = try incrementalHashTreeRoot(&backend, 1, 1);
    const expected = sha256Pair(left_chunk, right_chunk);
    try testing.expectEqual(expected, root);
}

test "incrementalHashTreeRoot - empty subtree returns zero hash" {
    const allocator = testing.allocator;
    const merkle_backend_mod = @import("../state/merkle_backend.zig");
    var backend = try merkle_backend_mod.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    // Only write left child; right is empty.
    var left_chunk: merkle_backend_mod.Chunk = std.mem.zeroes(merkle_backend_mod.Chunk);
    left_chunk[0] = 0x01;
    try backend.putChunk(2, left_chunk);

    const root = try incrementalHashTreeRoot(&backend, 1, 1);
    const expected = sha256Pair(left_chunk, zeroHash(0));
    try testing.expectEqual(expected, root);
}

test "incrementalHashTreeRoot - second call uses cache (no recompute)" {
    const allocator = testing.allocator;
    const merkle_backend_mod = @import("../state/merkle_backend.zig");
    var backend = try merkle_backend_mod.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var chunk: merkle_backend_mod.Chunk = std.mem.zeroes(merkle_backend_mod.Chunk);
    chunk[0] = 0xCC;
    try backend.putChunk(1, chunk);

    const root1 = try incrementalHashTreeRoot(&backend, 1, 0);
    // After first call, the node is clean.
    try testing.expect(!backend.isDirty(1));

    const root2 = try incrementalHashTreeRoot(&backend, 1, 0);
    try testing.expectEqual(root1, root2);
}
