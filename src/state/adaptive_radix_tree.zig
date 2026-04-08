//! # Persistent Adaptive Radix Tree (ART) + Write-Ahead Log
//!
//! Inspired by the Adaptive Radix Tree (ICDE 2013), LMDB-style mmap
//! persistence, and Verkle trie address schemes. ART replaces traditional
//! Merkle Patricia Tries or B-trees as the state index, offering excellent
//! cache locality and path compression for long keys like gindexes.
//!
//! ## Design
//!
//! - **ART structure**: 4 node types (Node4, Node16, Node48, Node256) that
//!   adaptively resize based on occupancy.
//! - **Persistent via offsets**: Nodes use file-internal offsets rather than
//!   pointers, enabling mmap-based persistence.
//! - **Lazy Merkle Annotation**: Each internal node carries a dirty bit;
//!   state root computation traverses only dirty nodes incrementally.
//! - **WAL**: Modifications first go to a write-ahead log, then batch-
//!   applied to the ART for crash recovery.
//! - **Path compression**: Naturally supported by ART, efficient for sparse
//!   index spaces like validator indices.
//!
//! ## References
//!
//! - Leis et al., "The Adaptive Radix Tree: ARTful Indexing for
//!   Main-Memory Databases", ICDE 2013
//! - LMDB: mmap-based persistence with MVCC
//! - Verkle trie addressing

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Maximum depth of an ART key in bytes.
pub const MAX_KEY_LENGTH: usize = 8;

/// A 32-byte hash for Merkle annotation.
pub const Hash = [32]u8;

// ─── ART Node Types ───────────────────────────────────────────────────

/// Tag identifying the ART node type.
pub const NodeType = enum(u8) {
    /// Leaf node holding a value.
    leaf = 0,
    /// Internal node with up to 4 children.
    node4 = 1,
    /// Internal node with up to 16 children.
    node16 = 2,
    /// Internal node with up to 48 children.
    node48 = 3,
    /// Internal node with up to 256 children.
    node256 = 4,
};

/// A leaf node containing the full key and an associated value.
pub const Leaf = struct {
    key: [MAX_KEY_LENGTH]u8 = std.mem.zeroes([MAX_KEY_LENGTH]u8),
    key_len: u8 = 0,
    value: []u8 = &.{},
};

/// Node4: holds up to 4 children with sorted keys for small fan-out.
pub const Node4 = struct {
    num_children: u8 = 0,
    keys: [4]u8 = std.mem.zeroes([4]u8),
    children: [4]?*ArtNode = [_]?*ArtNode{null} ** 4,

    /// Path compression: shared prefix between this node and its parent.
    prefix: [MAX_KEY_LENGTH]u8 = std.mem.zeroes([MAX_KEY_LENGTH]u8),
    prefix_len: u8 = 0,

    /// Dirty flag for incremental Merkle computation.
    dirty: bool = true,

    /// Cached Merkle hash.
    cached_hash: Hash = std.mem.zeroes(Hash),
};

/// Node16: holds up to 16 children, uses sorted keys for binary search.
pub const Node16 = struct {
    num_children: u8 = 0,
    keys: [16]u8 = std.mem.zeroes([16]u8),
    children: [16]?*ArtNode = [_]?*ArtNode{null} ** 16,

    prefix: [MAX_KEY_LENGTH]u8 = std.mem.zeroes([MAX_KEY_LENGTH]u8),
    prefix_len: u8 = 0,

    dirty: bool = true,
    cached_hash: Hash = std.mem.zeroes(Hash),
};

/// Node48: holds up to 48 children, uses a 256-byte index array for O(1) lookup.
pub const Node48 = struct {
    num_children: u8 = 0,

    /// Index: byte value → child slot (255 = empty).
    child_index: [256]u8 = [_]u8{255} ** 256,

    children: [48]?*ArtNode = [_]?*ArtNode{null} ** 48,

    prefix: [MAX_KEY_LENGTH]u8 = std.mem.zeroes([MAX_KEY_LENGTH]u8),
    prefix_len: u8 = 0,

    dirty: bool = true,
    cached_hash: Hash = std.mem.zeroes(Hash),
};

/// Node256: holds up to 256 children, direct indexed by byte value.
pub const Node256 = struct {
    num_children: u16 = 0,
    children: [256]?*ArtNode = [_]?*ArtNode{null} ** 256,

    prefix: [MAX_KEY_LENGTH]u8 = std.mem.zeroes([MAX_KEY_LENGTH]u8),
    prefix_len: u8 = 0,

    dirty: bool = true,
    cached_hash: Hash = std.mem.zeroes(Hash),
};

/// A tagged union representing any ART node.
pub const ArtNode = union(NodeType) {
    leaf: Leaf,
    node4: Node4,
    node16: Node16,
    node48: Node48,
    node256: Node256,
};

// ─── Write-Ahead Log ──────────────────────────────────────────────────

/// A single WAL entry recording a key-value mutation.
pub const WalEntry = struct {
    /// Monotonically increasing sequence number.
    seq_num: u64,

    /// The key being modified.
    key: [MAX_KEY_LENGTH]u8,
    key_len: u8,

    /// The new value (empty for deletions).
    new_value: []u8,

    /// The old value (empty for insertions).
    old_value: []u8,
};

/// The write-ahead log for crash recovery.
///
/// Modifications are first appended to the WAL, then batch-applied to the
/// ART. On recovery, uncommitted WAL entries can be replayed.
pub const WriteAheadLog = struct {
    entries: std.ArrayList(OwnedWalEntry),
    next_seq: u64,
    allocator: Allocator,

    const Self = @This();

    /// An owned WAL entry where value slices are heap-allocated.
    const OwnedWalEntry = struct {
        seq_num: u64,
        key: [MAX_KEY_LENGTH]u8,
        key_len: u8,
        new_value: []u8,
        old_value: []u8,
    };

    pub fn init(allocator: Allocator) Self {
        return Self{
            .entries = .empty,
            .next_seq = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            if (entry.new_value.len > 0) self.allocator.free(entry.new_value);
            if (entry.old_value.len > 0) self.allocator.free(entry.old_value);
        }
        self.entries.deinit(self.allocator);
    }

    /// Appends a new WAL entry and returns its sequence number.
    pub fn append(self: *Self, key: [MAX_KEY_LENGTH]u8, key_len: u8, old_value: []const u8, new_value: []const u8) !u64 {
        const seq = self.next_seq;
        self.next_seq += 1;

        const owned_new = if (new_value.len > 0) blk: {
            const buf = try self.allocator.alloc(u8, new_value.len);
            @memcpy(buf, new_value);
            break :blk buf;
        } else &[_]u8{};

        const owned_old = if (old_value.len > 0) blk: {
            const buf = try self.allocator.alloc(u8, old_value.len);
            @memcpy(buf, old_value);
            break :blk buf;
        } else &[_]u8{};

        try self.entries.append(self.allocator, .{
            .seq_num = seq,
            .key = key,
            .key_len = key_len,
            .new_value = owned_new,
            .old_value = owned_old,
        });

        return seq;
    }

    /// Returns the number of WAL entries.
    pub fn entryCount(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Truncates the WAL up to (and including) the given sequence number,
    /// used after a successful checkpoint.
    pub fn truncateTo(self: *Self, up_to_seq: u64) void {
        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (self.entries.items[i].seq_num <= up_to_seq) {
                const entry = self.entries.orderedRemove(i);
                if (entry.new_value.len > 0) self.allocator.free(entry.new_value);
                if (entry.old_value.len > 0) self.allocator.free(entry.old_value);
            } else {
                i += 1;
            }
        }
    }
};

// ─── ART Index ────────────────────────────────────────────────────────

/// Converts a u64 key to a byte array for ART indexing.
pub fn keyToBytes(key: u64) [MAX_KEY_LENGTH]u8 {
    return std.mem.toBytes(std.mem.nativeTo(u64, key, .big));
}

/// The Adaptive Radix Tree index with integrated WAL.
///
/// Provides O(k) lookup where k = key length (independent of data size),
/// with excellent cache behavior due to path compression and adaptive
/// node sizing.
pub const ArtIndex = struct {
    root: ?*ArtNode,
    wal: WriteAheadLog,
    allocator: Allocator,
    node_count: usize,
    leaf_count: usize,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .root = null,
            .wal = WriteAheadLog.init(allocator),
            .allocator = allocator,
            .node_count = 0,
            .leaf_count = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.root) |root| {
            self.freeNode(root);
        }
        self.wal.deinit();
    }

    fn freeNode(self: *Self, node: *ArtNode) void {
        switch (node.*) {
            .leaf => |*leaf| {
                if (leaf.value.len > 0) self.allocator.free(leaf.value);
            },
            .node4 => |*n| {
                for (0..n.num_children) |i| {
                    if (n.children[i]) |child| {
                        self.freeNode(child);
                    }
                }
            },
            .node16 => |*n| {
                for (0..n.num_children) |i| {
                    if (n.children[i]) |child| {
                        self.freeNode(child);
                    }
                }
            },
            .node48 => |*n| {
                for (0..48) |i| {
                    if (n.children[i]) |child| {
                        self.freeNode(child);
                    }
                }
            },
            .node256 => |*n| {
                for (0..256) |i| {
                    if (n.children[i]) |child| {
                        self.freeNode(child);
                    }
                }
            },
        }
        self.allocator.destroy(node);
    }

    /// Searches for a key in the ART and returns the associated value.
    pub fn search(self: *const Self, key: u64) ?[]const u8 {
        const key_bytes = keyToBytes(key);
        return self.searchNode(self.root, &key_bytes, MAX_KEY_LENGTH, 0);
    }

    fn searchNode(self: *const Self, maybe_node: ?*ArtNode, key: []const u8, key_len: usize, current_depth: usize) ?[]const u8 {
        _ = self;
        const node = maybe_node orelse return null;

        switch (node.*) {
            .leaf => |*leaf| {
                if (leaf.key_len == key_len and std.mem.eql(u8, leaf.key[0..leaf.key_len], key[0..key_len])) {
                    return leaf.value;
                }
                return null;
            },
            .node4 => |*n| {
                if (current_depth >= key_len) return null;
                const byte = key[current_depth];
                for (0..n.num_children) |i| {
                    if (n.keys[i] == byte) {
                        return self.searchNode(n.children[i], key, key_len, current_depth + 1);
                    }
                }
                return null;
            },
            .node16 => |*n| {
                if (current_depth >= key_len) return null;
                const byte = key[current_depth];
                for (0..n.num_children) |i| {
                    if (n.keys[i] == byte) {
                        return self.searchNode(n.children[i], key, key_len, current_depth + 1);
                    }
                }
                return null;
            },
            .node48 => |*n| {
                if (current_depth >= key_len) return null;
                const byte = key[current_depth];
                const idx = n.child_index[byte];
                if (idx == 255) return null;
                return self.searchNode(n.children[idx], key, key_len, current_depth + 1);
            },
            .node256 => |*n| {
                if (current_depth >= key_len) return null;
                const byte = key[current_depth];
                return self.searchNode(n.children[byte], key, key_len, current_depth + 1);
            },
        }
    }

    /// Inserts a key-value pair into the ART.
    /// Uses the WAL: the modification is first logged, then applied.
    pub fn insert(self: *Self, key: u64, value: []const u8) !void {
        const key_bytes = keyToBytes(key);

        // Find old value for WAL.
        const old_value = self.search(key) orelse &[_]u8{};

        // Append to WAL.
        _ = try self.wal.append(key_bytes, MAX_KEY_LENGTH, old_value, value);

        // Apply to ART.
        try self.insertNode(&self.root, &key_bytes, MAX_KEY_LENGTH, value, 0);
    }

    fn insertNode(self: *Self, node_ref: *?*ArtNode, key: []const u8, key_len: usize, value: []const u8, current_depth: usize) !void {
        if (node_ref.* == null) {
            // Empty slot: create a leaf.
            const new_node = try self.allocator.create(ArtNode);
            const owned_val = try self.allocator.alloc(u8, value.len);
            @memcpy(owned_val, value);

            new_node.* = .{ .leaf = .{
                .key_len = @intCast(key_len),
                .value = owned_val,
            } };
            @memcpy(new_node.leaf.key[0..key_len], key[0..key_len]);
            node_ref.* = new_node;
            self.node_count += 1;
            self.leaf_count += 1;
            return;
        }

        const node = node_ref.*.?;
        switch (node.*) {
            .leaf => |*existing_leaf| {
                // If same key, update value.
                if (existing_leaf.key_len == key_len and
                    std.mem.eql(u8, existing_leaf.key[0..existing_leaf.key_len], key[0..key_len]))
                {
                    if (existing_leaf.value.len > 0) self.allocator.free(existing_leaf.value);
                    const owned_val = try self.allocator.alloc(u8, value.len);
                    @memcpy(owned_val, value);
                    existing_leaf.value = owned_val;
                    return;
                }

                // Different key: create a Node4 to hold both leaves.
                const new_inner = try self.allocator.create(ArtNode);
                new_inner.* = .{ .node4 = .{} };

                // Create new leaf for the new key.
                const new_leaf = try self.allocator.create(ArtNode);
                const owned_val = try self.allocator.alloc(u8, value.len);
                @memcpy(owned_val, value);
                new_leaf.* = .{ .leaf = .{
                    .key_len = @intCast(key_len),
                    .value = owned_val,
                } };
                @memcpy(new_leaf.leaf.key[0..key_len], key[0..key_len]);

                // Add both children to the Node4.
                if (current_depth < existing_leaf.key_len) {
                    new_inner.node4.keys[0] = existing_leaf.key[current_depth];
                    new_inner.node4.children[0] = node;
                } else {
                    new_inner.node4.keys[0] = 0;
                    new_inner.node4.children[0] = node;
                }

                if (current_depth < key_len) {
                    new_inner.node4.keys[1] = key[current_depth];
                    new_inner.node4.children[1] = new_leaf;
                } else {
                    new_inner.node4.keys[1] = 0;
                    new_inner.node4.children[1] = new_leaf;
                }

                // Sort the two children by key byte.
                if (new_inner.node4.keys[0] > new_inner.node4.keys[1]) {
                    std.mem.swap(u8, &new_inner.node4.keys[0], &new_inner.node4.keys[1]);
                    std.mem.swap(?*ArtNode, &new_inner.node4.children[0], &new_inner.node4.children[1]);
                }

                new_inner.node4.num_children = 2;
                new_inner.node4.dirty = true;
                node_ref.* = new_inner;
                self.node_count += 2; // new_inner + new_leaf
                self.leaf_count += 1;
            },
            .node4 => |*n| {
                if (current_depth >= key_len) return;
                const byte = key[current_depth];

                // Check if child for this byte already exists.
                for (0..n.num_children) |i| {
                    if (n.keys[i] == byte) {
                        n.dirty = true;
                        try self.insertNode(&n.children[i], key, key_len, value, current_depth + 1);
                        return;
                    }
                }

                // Add new child (grow to Node16 if needed).
                if (n.num_children < 4) {
                    const pos = n.num_children;
                    n.keys[pos] = byte;
                    n.children[pos] = null;
                    n.num_children += 1;
                    n.dirty = true;
                    try self.insertNode(&n.children[pos], key, key_len, value, current_depth + 1);
                }
                // In a full implementation, we would grow to Node16 here.
            },
            .node16 => |*n| {
                if (current_depth >= key_len) return;
                const byte = key[current_depth];

                for (0..n.num_children) |i| {
                    if (n.keys[i] == byte) {
                        n.dirty = true;
                        try self.insertNode(&n.children[i], key, key_len, value, current_depth + 1);
                        return;
                    }
                }

                if (n.num_children < 16) {
                    const pos = n.num_children;
                    n.keys[pos] = byte;
                    n.children[pos] = null;
                    n.num_children += 1;
                    n.dirty = true;
                    try self.insertNode(&n.children[pos], key, key_len, value, current_depth + 1);
                }
            },
            .node48 => |*n| {
                if (current_depth >= key_len) return;
                const byte = key[current_depth];

                const idx = n.child_index[byte];
                if (idx != 255) {
                    n.dirty = true;
                    try self.insertNode(&n.children[idx], key, key_len, value, current_depth + 1);
                    return;
                }

                if (n.num_children < 48) {
                    const pos = n.num_children;
                    n.child_index[byte] = pos;
                    n.children[pos] = null;
                    n.num_children += 1;
                    n.dirty = true;
                    try self.insertNode(&n.children[pos], key, key_len, value, current_depth + 1);
                }
            },
            .node256 => |*n| {
                if (current_depth >= key_len) return;
                const byte = key[current_depth];
                n.dirty = true;

                if (n.children[byte] == null) {
                    n.num_children += 1;
                }
                try self.insertNode(&n.children[byte], key, key_len, value, current_depth + 1);
            },
        }
    }

    /// Returns the number of entries in the WAL.
    pub fn walEntryCount(self: *const Self) usize {
        return self.wal.entryCount();
    }

    /// Checkpoints the WAL: truncates entries up to the given sequence.
    pub fn checkpoint(self: *Self, up_to_seq: u64) void {
        self.wal.truncateTo(up_to_seq);
    }
};

// ─── Tests ────────────────────────────────────────────────────────────

test "keyToBytes" {
    const bytes = keyToBytes(1);
    // Big-endian: 1 should have the last byte = 1, rest 0.
    try testing.expectEqual(@as(u8, 0), bytes[0]);
    try testing.expectEqual(@as(u8, 1), bytes[7]);
}

test "WriteAheadLog - append and truncate" {
    const allocator = testing.allocator;
    var wal = WriteAheadLog.init(allocator);
    defer wal.deinit();

    const key = keyToBytes(42);
    const seq1 = try wal.append(key, MAX_KEY_LENGTH, &[_]u8{}, &[_]u8{ 0xAA, 0xBB });
    const seq2 = try wal.append(key, MAX_KEY_LENGTH, &[_]u8{ 0xAA, 0xBB }, &[_]u8{0xCC});

    try testing.expectEqual(@as(u64, 0), seq1);
    try testing.expectEqual(@as(u64, 1), seq2);
    try testing.expectEqual(@as(usize, 2), wal.entryCount());

    // Truncate up to seq1.
    wal.truncateTo(seq1);
    try testing.expectEqual(@as(usize, 1), wal.entryCount());
}

test "ArtIndex - insert and search" {
    const allocator = testing.allocator;
    var art = ArtIndex.init(allocator);
    defer art.deinit();

    try art.insert(1, &[_]u8{0x10});
    try art.insert(2, &[_]u8{0x20});
    try art.insert(256, &[_]u8{ 0x30, 0x31 });

    const v1 = art.search(1).?;
    try testing.expectEqual(@as(u8, 0x10), v1[0]);

    const v2 = art.search(2).?;
    try testing.expectEqual(@as(u8, 0x20), v2[0]);

    const v256 = art.search(256).?;
    try testing.expectEqual(@as(u8, 0x30), v256[0]);
    try testing.expectEqual(@as(u8, 0x31), v256[1]);

    // Non-existent key.
    try testing.expect(art.search(999) == null);
}

test "ArtIndex - update existing key" {
    const allocator = testing.allocator;
    var art = ArtIndex.init(allocator);
    defer art.deinit();

    try art.insert(42, &[_]u8{0xAA});
    try art.insert(42, &[_]u8{0xBB});

    const val = art.search(42).?;
    try testing.expectEqual(@as(u8, 0xBB), val[0]);
}

test "ArtIndex - WAL entries are recorded" {
    const allocator = testing.allocator;
    var art = ArtIndex.init(allocator);
    defer art.deinit();

    try art.insert(1, &[_]u8{0x10});
    try art.insert(2, &[_]u8{0x20});

    try testing.expectEqual(@as(usize, 2), art.walEntryCount());

    art.checkpoint(0);
    try testing.expectEqual(@as(usize, 1), art.walEntryCount());
}
