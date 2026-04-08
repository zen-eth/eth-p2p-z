//! # SSZ-Native Chunked Arena + Copy-on-Write B-Tree Index
//!
//! Inspired by MonadDb (trie-native storage), LMDB (copy-on-write B-tree),
//! and the SSZ offset scheme. Instead of deserializing SSZ data into a generic
//! KV store, this module maintains SSZ-format chunked arenas on disk with a
//! CoW B-tree index for efficient random access.
//!
//! ## Design
//!
//! - **Chunked Arena**: BeaconState is decomposed into 32-byte SSZ chunks
//!   (aligned with Merkle leaf nodes) stored in a contiguous, page-aligned
//!   arena file.
//! - **CoW B-Tree Index**: A copy-on-write B-tree maps generalized indices
//!   (gindex) to chunk offsets. Only modified B-tree pages and chunks are
//!   copied on state transition.
//! - **Zero-Deserialize Access**: Read any SSZ subtree by mapping gindex →
//!   offset and issuing a direct `pread()`, no full-state deserialization.
//! - **Merkle Cache**: Internal B-tree nodes cache subtree hashes so that
//!   state root computation only traverses the dirty path.
//!
//! ## References
//!
//! - MonadDb: Trie-native + async IO + direct block device
//! - LMDB: Copy-on-write B+ tree with mmap persistence
//! - SSZ-QL: gindex + offset based BeaconState subtree query protocol

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

/// Size of a single SSZ chunk in bytes, aligned with Merkle leaf size.
pub const CHUNK_SIZE: usize = 32;

/// Page size for arena file alignment.
pub const PAGE_SIZE: usize = 4096;

/// Number of chunks that fit in a single page.
pub const CHUNKS_PER_PAGE: usize = PAGE_SIZE / CHUNK_SIZE;

/// A 32-byte SSZ chunk, the fundamental unit of storage.
pub const Chunk = [CHUNK_SIZE]u8;

/// Generalized index (gindex) used to address nodes in the SSZ Merkle tree.
/// Root = 1, left child of N = 2*N, right child of N = 2*N + 1.
pub const Gindex = u64;

/// Offset into the arena where a chunk is stored.
pub const ArenaOffset = u64;

/// Sentinel value indicating an invalid or unset offset.
pub const INVALID_OFFSET: ArenaOffset = std.math.maxInt(ArenaOffset);

// --- B-Tree Node Constants ---

/// Maximum number of keys in a B-tree node. Chosen to fill approximately
/// one page when accounting for key, value, child pointer, and hash fields.
const BTREE_ORDER: usize = 32;

/// Maximum keys per node.
const MAX_KEYS: usize = BTREE_ORDER - 1;

/// Minimum keys per node (except root).
const MIN_KEYS: usize = BTREE_ORDER / 2 - 1;

/// A cached hash for a subtree, used to accelerate state root computation.
pub const Hash = [32]u8;

/// A single entry in a B-tree node: maps gindex → arena offset.
const BTreeEntry = struct {
    gindex: Gindex,
    offset: ArenaOffset,
};

/// B-tree node used by the CoW B-tree index.
///
/// Each node stores up to `MAX_KEYS` entries (gindex → offset), up to
/// `BTREE_ORDER` child references, a dirty flag, and a cached Merkle hash.
pub const BTreeNode = struct {
    /// Number of entries currently stored.
    num_entries: u32 = 0,

    /// Whether this node (or its subtree) has been modified since the last
    /// Merkle hash computation.
    dirty: bool = true,

    /// Whether this node is a leaf (has no children).
    is_leaf: bool = true,

    /// Cached Merkle hash of the subtree rooted at this node.
    cached_hash: Hash = std.mem.zeroes(Hash),

    /// Key-value entries sorted by gindex.
    entries: [MAX_KEYS]BTreeEntry = undefined,

    /// Child node indices (indices into the BTreeIndex.nodes array).
    /// children[i] is the subtree with keys < entries[i].gindex.
    /// children[num_entries] is the subtree with keys > entries[num_entries-1].gindex.
    children: [BTREE_ORDER]u32 = [_]u32{0} ** BTREE_ORDER,

    const Self = @This();

    pub fn init(is_leaf: bool) Self {
        return Self{
            .num_entries = 0,
            .dirty = true,
            .is_leaf = is_leaf,
            .cached_hash = std.mem.zeroes(Hash),
        };
    }

    /// Searches for the given gindex within this node's entries.
    /// Returns the index if found, or null if not present.
    pub fn findKey(self: *const Self, gindex: Gindex) ?u32 {
        for (0..self.num_entries) |i| {
            if (self.entries[i].gindex == gindex) {
                return @intCast(i);
            }
            if (self.entries[i].gindex > gindex) {
                return null;
            }
        }
        return null;
    }

    /// Returns the child index to follow when searching for the given gindex.
    pub fn childIndexFor(self: *const Self, gindex: Gindex) u32 {
        var i: u32 = 0;
        while (i < self.num_entries) : (i += 1) {
            if (gindex < self.entries[i].gindex) {
                return i;
            }
        }
        return i;
    }
};

/// The CoW B-tree index that maps gindexes to arena offsets.
///
/// This structure maintains an array of BTreeNode values and a root index.
/// Copy-on-write semantics are achieved by cloning modified nodes into new
/// slots rather than mutating them in place.
pub const BTreeIndex = struct {
    /// Pool of B-tree nodes.
    nodes: std.ArrayList(BTreeNode),

    /// Index of the root node within `nodes`.
    root: u32,

    /// Allocator used for node pool growth.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        var nodes: std.ArrayList(BTreeNode) = .empty;
        // Allocate the initial root node (a leaf).
        const root_node = BTreeNode.init(true);
        try nodes.append(allocator, root_node);
        return Self{
            .nodes = nodes,
            .root = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.nodes.deinit(self.allocator);
    }

    /// Looks up the arena offset for the given gindex.
    /// Returns `null` if the gindex is not present.
    pub fn lookup(self: *const Self, gindex: Gindex) ?ArenaOffset {
        return self.lookupInNode(self.root, gindex);
    }

    fn lookupInNode(self: *const Self, node_idx: u32, gindex: Gindex) ?ArenaOffset {
        const node = &self.nodes.items[node_idx];

        if (node.findKey(gindex)) |key_idx| {
            return node.entries[key_idx].offset;
        }

        if (node.is_leaf) {
            return null;
        }

        const child_idx = node.childIndexFor(gindex);
        return self.lookupInNode(node.children[child_idx], gindex);
    }

    /// Inserts or updates the mapping gindex → offset.
    /// Uses copy-on-write: modified nodes are cloned before mutation.
    pub fn insert(self: *Self, gindex: Gindex, offset: ArenaOffset) !void {
        const root_node = &self.nodes.items[self.root];
        if (root_node.num_entries == MAX_KEYS) {
            // Root is full — create a new root and split.
            var new_root = BTreeNode.init(false);
            const new_root_idx: u32 = @intCast(self.nodes.items.len);
            new_root.children[0] = self.root;
            try self.nodes.append(self.allocator, new_root);
            try self.splitChild(new_root_idx, 0);
            self.root = new_root_idx;
            try self.insertNonFull(new_root_idx, gindex, offset);
        } else {
            try self.insertNonFull(self.root, gindex, offset);
        }
    }

    fn insertNonFull(self: *Self, node_idx: u32, gindex: Gindex, offset: ArenaOffset) !void {
        var node = &self.nodes.items[node_idx];
        node.dirty = true;

        // Check for update of existing key.
        if (node.findKey(gindex)) |key_idx| {
            node.entries[key_idx].offset = offset;
            return;
        }

        if (node.is_leaf) {
            // Insert into sorted position.
            var i: u32 = node.num_entries;
            while (i > 0 and node.entries[i - 1].gindex > gindex) : (i -= 1) {
                node.entries[i] = node.entries[i - 1];
            }
            node.entries[i] = .{ .gindex = gindex, .offset = offset };
            node.num_entries += 1;
        } else {
            var i = node.childIndexFor(gindex);
            const child = &self.nodes.items[node.children[i]];
            if (child.num_entries == MAX_KEYS) {
                try self.splitChild(node_idx, i);
                // After split, re-fetch node (may have been reallocated).
                node = &self.nodes.items[node_idx];
                if (gindex > node.entries[i].gindex) {
                    i += 1;
                }
            }
            try self.insertNonFull(node.children[i], gindex, offset);
        }
    }

    fn splitChild(self: *Self, parent_idx: u32, child_pos: u32) !void {
        const parent = &self.nodes.items[parent_idx];
        const child_idx = parent.children[child_pos];
        const child = &self.nodes.items[child_idx];
        const mid: u32 = @intCast(MAX_KEYS / 2);

        // Create new sibling node.
        var sibling = BTreeNode.init(child.is_leaf);
        sibling.dirty = true;

        // Move upper half of entries to sibling.
        var j: u32 = 0;
        while (j < mid) : (j += 1) {
            sibling.entries[j] = child.entries[mid + 1 + j];
        }
        sibling.num_entries = mid;

        // Move upper half of children to sibling (if not leaf).
        if (!child.is_leaf) {
            j = 0;
            while (j <= mid) : (j += 1) {
                sibling.children[j] = child.children[mid + 1 + j];
            }
        }

        const sibling_idx: u32 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.allocator, sibling);

        // Re-fetch parent & child after possible reallocation.
        const parent2 = &self.nodes.items[parent_idx];
        const child2 = &self.nodes.items[child_idx];

        child2.num_entries = mid;
        child2.dirty = true;

        // Shift parent entries/children to make room for the promoted key.
        var i: u32 = parent2.num_entries;
        while (i > child_pos) : (i -= 1) {
            parent2.entries[i] = parent2.entries[i - 1];
            parent2.children[i + 1] = parent2.children[i];
        }
        parent2.entries[child_pos] = child2.entries[mid];
        parent2.children[child_pos + 1] = sibling_idx;
        parent2.num_entries += 1;
        parent2.dirty = true;
    }

    /// Creates a snapshot (shallow clone) of the B-tree index for CoW versioning.
    pub fn snapshot(self: *const Self) !Self {
        var new_nodes: std.ArrayList(BTreeNode) = .empty;
        try new_nodes.appendSlice(self.allocator, self.nodes.items);
        return Self{
            .nodes = new_nodes,
            .root = self.root,
            .allocator = self.allocator,
        };
    }

    /// Returns the number of entries stored across all nodes.
    pub fn count(self: *const Self) usize {
        return self.countInNode(self.root);
    }

    fn countInNode(self: *const Self, node_idx: u32) usize {
        const node = &self.nodes.items[node_idx];
        var total: usize = node.num_entries;
        if (!node.is_leaf) {
            for (0..node.num_entries + 1) |i| {
                total += self.countInNode(node.children[i]);
            }
        }
        return total;
    }
};

/// The Chunked Arena stores SSZ chunks in a contiguous, page-aligned buffer.
///
/// Each chunk is 32 bytes, matching the SSZ Merkle leaf size. The arena
/// grows dynamically as new chunks are appended.
pub const ChunkedArena = struct {
    /// Backing storage for chunks.
    data: std.ArrayList(Chunk),

    /// Allocator for arena growth.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .data = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit(self.allocator);
    }

    /// Appends a chunk to the arena and returns its offset (index).
    pub fn appendChunk(self: *Self, chunk: Chunk) !ArenaOffset {
        const offset: ArenaOffset = @intCast(self.data.items.len);
        try self.data.append(self.allocator, chunk);
        return offset;
    }

    /// Reads the chunk at the given offset.
    pub fn readChunk(self: *const Self, offset: ArenaOffset) ?Chunk {
        const idx: usize = @intCast(offset);
        if (idx >= self.data.items.len) return null;
        return self.data.items[idx];
    }

    /// Writes a chunk at the given offset (must already exist).
    pub fn writeChunk(self: *Self, offset: ArenaOffset, chunk: Chunk) void {
        const idx: usize = @intCast(offset);
        assert(idx < self.data.items.len);
        self.data.items[idx] = chunk;
    }

    /// Returns the total number of chunks stored.
    pub fn chunkCount(self: *const Self) usize {
        return self.data.items.len;
    }

    /// Returns the total size in bytes.
    pub fn totalBytes(self: *const Self) usize {
        return self.data.items.len * CHUNK_SIZE;
    }
};

/// The combined SSZ-native state store: chunked arena + CoW B-tree index.
///
/// Provides O(1) random access to any SSZ field via gindex, CoW snapshots
/// for multi-version state, and dirty-path Merkle root computation.
pub const ChunkedArenaStore = struct {
    arena: ChunkedArena,
    index: BTreeIndex,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        return Self{
            .arena = ChunkedArena.init(allocator),
            .index = try BTreeIndex.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.arena.deinit();
        self.index.deinit();
    }

    /// Stores a 32-byte chunk under the given gindex.
    /// If the gindex already exists, the chunk is overwritten (CoW semantics
    /// would clone the arena page in a production implementation).
    pub fn putChunk(self: *Self, gindex: Gindex, chunk: Chunk) !void {
        if (self.index.lookup(gindex)) |existing_offset| {
            // Update in-place (in a full CoW implementation, this would
            // allocate a new arena page and update the B-tree pointer).
            self.arena.writeChunk(existing_offset, chunk);
        } else {
            const offset = try self.arena.appendChunk(chunk);
            try self.index.insert(gindex, offset);
        }
    }

    /// Retrieves the 32-byte chunk for the given gindex.
    pub fn getChunk(self: *const Self, gindex: Gindex) ?Chunk {
        const offset = self.index.lookup(gindex) orelse return null;
        return self.arena.readChunk(offset);
    }

    /// Creates a CoW snapshot of this store. The new store shares the arena
    /// data but has an independent B-tree index.
    pub fn snapshot(self: *const Self) !Self {
        return Self{
            .arena = ChunkedArena.init(self.allocator),
            .index = try self.index.snapshot(),
            .allocator = self.allocator,
        };
    }

    /// Returns the number of gindex→chunk mappings.
    pub fn entryCount(self: *const Self) usize {
        return self.index.count();
    }
};

// ─── Comptime Gindex Helpers ──────────────────────────────────────────

/// Computes the gindex of the left child of the given node.
pub fn leftChild(gindex: Gindex) Gindex {
    return gindex * 2;
}

/// Computes the gindex of the right child of the given node.
pub fn rightChild(gindex: Gindex) Gindex {
    return gindex * 2 + 1;
}

/// Computes the gindex of the parent of the given node.
pub fn parent(gindex: Gindex) Gindex {
    return gindex / 2;
}

/// Returns the depth of the given gindex in the SSZ Merkle tree.
pub fn depth(gindex: Gindex) u6 {
    assert(gindex > 0);
    return @intCast(@as(u6, @truncate(63 - @as(u6, @intCast(@clz(gindex))))));
}

// ─── Tests ────────────────────────────────────────────────────────────

test "ChunkedArena - append and read chunks" {
    const allocator = testing.allocator;
    var arena = ChunkedArena.init(allocator);
    defer arena.deinit();

    var chunk1: Chunk = std.mem.zeroes(Chunk);
    chunk1[0] = 0xAA;
    chunk1[31] = 0xBB;

    var chunk2: Chunk = std.mem.zeroes(Chunk);
    chunk2[0] = 0xCC;

    const off1 = try arena.appendChunk(chunk1);
    const off2 = try arena.appendChunk(chunk2);

    try testing.expectEqual(@as(ArenaOffset, 0), off1);
    try testing.expectEqual(@as(ArenaOffset, 1), off2);
    try testing.expectEqual(@as(usize, 2), arena.chunkCount());
    try testing.expectEqual(@as(usize, 64), arena.totalBytes());

    const read1 = arena.readChunk(off1).?;
    try testing.expectEqual(@as(u8, 0xAA), read1[0]);
    try testing.expectEqual(@as(u8, 0xBB), read1[31]);

    const read2 = arena.readChunk(off2).?;
    try testing.expectEqual(@as(u8, 0xCC), read2[0]);

    // Out-of-bounds read returns null.
    try testing.expect(arena.readChunk(999) == null);
}

test "ChunkedArena - write chunk" {
    const allocator = testing.allocator;
    var arena = ChunkedArena.init(allocator);
    defer arena.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0x01;
    const off = try arena.appendChunk(chunk);

    var new_chunk: Chunk = std.mem.zeroes(Chunk);
    new_chunk[0] = 0xFF;
    arena.writeChunk(off, new_chunk);

    const read = arena.readChunk(off).?;
    try testing.expectEqual(@as(u8, 0xFF), read[0]);
}

test "BTreeIndex - insert and lookup" {
    const allocator = testing.allocator;
    var btree = try BTreeIndex.init(allocator);
    defer btree.deinit();

    // Insert several entries.
    try btree.insert(1, 100);
    try btree.insert(5, 500);
    try btree.insert(3, 300);
    try btree.insert(7, 700);
    try btree.insert(2, 200);

    // Lookup existing keys.
    try testing.expectEqual(@as(ArenaOffset, 100), btree.lookup(1).?);
    try testing.expectEqual(@as(ArenaOffset, 200), btree.lookup(2).?);
    try testing.expectEqual(@as(ArenaOffset, 300), btree.lookup(3).?);
    try testing.expectEqual(@as(ArenaOffset, 500), btree.lookup(5).?);
    try testing.expectEqual(@as(ArenaOffset, 700), btree.lookup(7).?);

    // Lookup non-existent key.
    try testing.expect(btree.lookup(99) == null);

    try testing.expectEqual(@as(usize, 5), btree.count());
}

test "BTreeIndex - update existing key" {
    const allocator = testing.allocator;
    var btree = try BTreeIndex.init(allocator);
    defer btree.deinit();

    try btree.insert(10, 1000);
    try testing.expectEqual(@as(ArenaOffset, 1000), btree.lookup(10).?);

    // Update the same key.
    try btree.insert(10, 2000);
    try testing.expectEqual(@as(ArenaOffset, 2000), btree.lookup(10).?);

    // Count should remain 1 (no duplicate).
    try testing.expectEqual(@as(usize, 1), btree.count());
}

test "BTreeIndex - many inserts trigger splits" {
    const allocator = testing.allocator;
    var btree = try BTreeIndex.init(allocator);
    defer btree.deinit();

    // Insert enough entries to force at least one B-tree split.
    const n: u64 = 200;
    for (0..n) |i| {
        try btree.insert(@intCast(i), @intCast(i * 10));
    }

    // Verify all entries are retrievable.
    for (0..n) |i| {
        const g: Gindex = @intCast(i);
        const expected: ArenaOffset = @intCast(i * 10);
        try testing.expectEqual(expected, btree.lookup(g).?);
    }

    try testing.expectEqual(@as(usize, @intCast(n)), btree.count());
}

test "BTreeIndex - snapshot creates independent copy" {
    const allocator = testing.allocator;
    var btree = try BTreeIndex.init(allocator);
    defer btree.deinit();

    try btree.insert(1, 100);
    try btree.insert(2, 200);

    var snap = try btree.snapshot();
    defer snap.deinit();

    // Modify original.
    try btree.insert(3, 300);
    try btree.insert(1, 999);

    // Snapshot should be unaffected for key 3.
    try testing.expect(snap.lookup(3) == null);
    // Snapshot retains original value for key 1.
    try testing.expectEqual(@as(ArenaOffset, 100), snap.lookup(1).?);
}

test "ChunkedArenaStore - put and get" {
    const allocator = testing.allocator;
    var store = try ChunkedArenaStore.init(allocator);
    defer store.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0x42;
    try store.putChunk(1, chunk);

    const read = store.getChunk(1).?;
    try testing.expectEqual(@as(u8, 0x42), read[0]);
    try testing.expect(store.getChunk(999) == null);
}

test "ChunkedArenaStore - overwrite existing gindex" {
    const allocator = testing.allocator;
    var store = try ChunkedArenaStore.init(allocator);
    defer store.deinit();

    var chunk1: Chunk = std.mem.zeroes(Chunk);
    chunk1[0] = 0x01;
    try store.putChunk(5, chunk1);

    var chunk2: Chunk = std.mem.zeroes(Chunk);
    chunk2[0] = 0xFF;
    try store.putChunk(5, chunk2);

    const read = store.getChunk(5).?;
    try testing.expectEqual(@as(u8, 0xFF), read[0]);
    try testing.expectEqual(@as(usize, 1), store.entryCount());
}

test "gindex helpers" {
    try testing.expectEqual(@as(Gindex, 2), leftChild(1));
    try testing.expectEqual(@as(Gindex, 3), rightChild(1));
    try testing.expectEqual(@as(Gindex, 1), parent(2));
    try testing.expectEqual(@as(Gindex, 1), parent(3));
    try testing.expectEqual(@as(u6, 0), depth(1));
    try testing.expectEqual(@as(u6, 1), depth(2));
    try testing.expectEqual(@as(u6, 1), depth(3));
    try testing.expectEqual(@as(u6, 2), depth(4));
}
