//! # Hybrid Epoch-Partitioned Storage with Structural Sharing
//!
//! Inspired by persistent data structures (Clojure/Haskell), Portal Network
//! DHT partitioning, and Git's packfile format. State is partitioned by
//! epoch into immutable "state packs" that share structure via delta encoding.
//!
//! ## Design
//!
//! - **Epoch State Pack**: Each finalized epoch produces an immutable pack file
//!   containing SSZ chunks, an object table, and delta-encoded references to
//!   previous packs.
//! - **Delta Encoding**: Unchanged data between adjacent epochs is represented
//!   as COPY commands referencing a base epoch's pack.
//! - **Active State (Hot)**: The current, un-finalized state is kept in a
//!   mutable in-memory structure. On finalization, it freezes into a pack.
//! - **Pack Index**: A global index maps (epoch, gindex) → pack_file:offset
//!   for efficient lookups across all packs.
//! - **GC & Compaction**: Old packs can be merged ("mega packs") similar to
//!   `git gc --aggressive`.
//!
//! ## References
//!
//! - Git packfile format: delta-encoded object storage
//! - Clojure/Haskell persistent data structures: structural sharing
//! - Portal Network State Spec: DHT-partitioned state

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Generalized index.
pub const Gindex = u64;

/// Epoch number.
pub const Epoch = u64;

// ─── Delta Encoding ───────────────────────────────────────────────────

/// A delta instruction: either copy from a base pack or insert new data.
pub const DeltaOp = union(enum) {
    /// Copy `length` bytes from `base_epoch`'s pack at `offset`.
    copy: struct {
        base_epoch: Epoch,
        offset: u64,
        length: u32,
    },
    /// Insert raw data inline.
    insert: struct {
        data: []u8,
    },
};

// ─── Pack Object ──────────────────────────────────────────────────────

/// Type of object stored in a pack.
pub const ObjectType = enum(u8) {
    /// Raw SSZ chunk data (32 bytes).
    raw_chunk = 0,
    /// Delta-encoded object referencing a base pack.
    delta = 1,
    /// Aggregated data (e.g., list of balances).
    aggregate = 2,
};

/// An entry in the pack's object table.
pub const PackObjectEntry = struct {
    /// The gindex this object corresponds to.
    gindex: Gindex,

    /// Offset within the pack's data section.
    offset: u64,

    /// Size of the object in bytes.
    size: u32,

    /// Whether this is raw data or a delta reference.
    obj_type: ObjectType,

    /// If delta-encoded, the base epoch to resolve against.
    base_epoch: ?Epoch,
};

// ─── Epoch State Pack ─────────────────────────────────────────────────

/// An immutable pack containing the state for a single finalized epoch.
///
/// Pack layout:
/// ```
/// [Header: epoch, state_root, num_objects]
/// [Object Table: gindex → (offset, size, type, base_ref?)]
/// [Object Data: raw or delta-encoded chunks]
/// ```
pub const EpochPack = struct {
    /// The epoch this pack covers.
    epoch: Epoch,

    /// The state root at this epoch.
    state_root: [32]u8,

    /// Object table entries.
    objects: std.ArrayList(PackObjectEntry),

    /// Raw data section.
    data: std.ArrayList(u8),

    /// Allocator.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, epoch: Epoch) Self {
        return Self{
            .epoch = epoch,
            .state_root = std.mem.zeroes([32]u8),
            .objects = std.ArrayList(PackObjectEntry).init(allocator),
            .data = std.ArrayList(u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.objects.deinit();
        self.data.deinit();
    }

    /// Adds a raw chunk to the pack.
    pub fn addRawChunk(self: *Self, gindex: Gindex, chunk_data: []const u8) !void {
        const offset: u64 = @intCast(self.data.items.len);
        try self.data.appendSlice(chunk_data);

        try self.objects.append(.{
            .gindex = gindex,
            .offset = offset,
            .size = @intCast(chunk_data.len),
            .obj_type = .raw_chunk,
            .base_epoch = null,
        });
    }

    /// Adds a delta-encoded reference to a base epoch's pack.
    pub fn addDelta(self: *Self, gindex: Gindex, base_epoch: Epoch, base_offset: u64, length: u32) !void {
        // Store the delta instruction as metadata (no data section bytes needed).
        try self.objects.append(.{
            .gindex = gindex,
            .offset = base_offset,
            .size = length,
            .obj_type = .delta,
            .base_epoch = base_epoch,
        });
    }

    /// Reads raw object data for a given gindex (only for raw_chunk type).
    pub fn readRaw(self: *const Self, gindex: Gindex) ?[]const u8 {
        for (self.objects.items) |obj| {
            if (obj.gindex == gindex and obj.obj_type == .raw_chunk) {
                const start: usize = @intCast(obj.offset);
                const end: usize = start + obj.size;
                if (end <= self.data.items.len) {
                    return self.data.items[start..end];
                }
            }
        }
        return null;
    }

    /// Returns the number of objects in this pack.
    pub fn objectCount(self: *const Self) usize {
        return self.objects.items.len;
    }

    /// Returns the total data size.
    pub fn dataSize(self: *const Self) usize {
        return self.data.items.len;
    }

    /// Counts the number of delta-encoded objects.
    pub fn deltaCount(self: *const Self) usize {
        var count: usize = 0;
        for (self.objects.items) |obj| {
            if (obj.obj_type == .delta) count += 1;
        }
        return count;
    }
};

// ─── Active State (Hot) ───────────────────────────────────────────────

/// The mutable in-memory state for the current (un-finalized) epoch.
///
/// Once finalized, this state is frozen into an EpochPack.
pub const ActiveState = struct {
    /// Current epoch being accumulated.
    current_epoch: Epoch,

    /// Mutable key-value map: gindex → value bytes.
    data: std.AutoHashMap(Gindex, []u8),

    /// Set of gindexes modified since the last finalization.
    dirty_set: std.AutoHashMap(Gindex, void),

    /// Allocator.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, epoch: Epoch) Self {
        return Self{
            .current_epoch = epoch,
            .data = std.AutoHashMap(Gindex, []u8).init(allocator),
            .dirty_set = std.AutoHashMap(Gindex, void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.data.valueIterator();
        while (it.next()) |value| {
            self.allocator.free(value.*);
        }
        self.data.deinit();
        self.dirty_set.deinit();
    }

    /// Gets the current value for a gindex.
    pub fn get(self: *const Self, gindex: Gindex) ?[]const u8 {
        return self.data.get(gindex);
    }

    /// Sets a value for a gindex, marking it as dirty.
    pub fn put(self: *Self, gindex: Gindex, value: []const u8) !void {
        const owned = try self.allocator.alloc(u8, value.len);
        @memcpy(owned, value);

        if (self.data.fetchPut(gindex, owned)) |old_entry| {
            self.allocator.free(old_entry.value);
        }

        try self.dirty_set.put(gindex, {});
    }

    /// Returns the number of dirty gindexes.
    pub fn dirtyCount(self: *const Self) usize {
        return self.dirty_set.count();
    }

    /// Returns the total number of entries.
    pub fn entryCount(self: *const Self) usize {
        return self.data.count();
    }

    /// Freezes this active state into an immutable EpochPack.
    /// The previous epoch's pack is used for delta encoding unchanged data.
    pub fn freeze(self: *Self, prev_pack: ?*const EpochPack) !EpochPack {
        var pack = EpochPack.init(self.allocator, self.current_epoch);

        var it = self.data.iterator();
        while (it.next()) |entry| {
            const gindex = entry.key_ptr.*;
            const value = entry.value_ptr.*;

            if (self.dirty_set.contains(gindex)) {
                // Modified: store as raw chunk.
                try pack.addRawChunk(gindex, value);
            } else if (prev_pack) |prev| {
                // Unchanged: delta-encode against previous pack.
                // Find the object in the previous pack.
                for (prev.objects.items) |obj| {
                    if (obj.gindex == gindex) {
                        try pack.addDelta(gindex, prev.epoch, obj.offset, obj.size);
                        break;
                    }
                }
            } else {
                // No previous pack: store as raw.
                try pack.addRawChunk(gindex, value);
            }
        }

        return pack;
    }

    /// Clears the dirty set (called after finalization).
    pub fn clearDirty(self: *Self) void {
        self.dirty_set.clearRetainingCapacity();
    }
};

// ─── Pack Index ───────────────────────────────────────────────────────

/// An index entry mapping (epoch, gindex) to a pack location.
const PackIndexKey = struct {
    epoch: Epoch,
    gindex: Gindex,
};

/// Global index for looking up objects across all packs.
pub const PackIndex = struct {
    /// Maps (epoch, gindex) → pack index in the packs array.
    index: std.AutoHashMap(PackIndexKey, usize),

    /// All stored epoch packs.
    packs: std.ArrayList(EpochPack),

    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .index = std.AutoHashMap(PackIndexKey, usize).init(allocator),
            .packs = std.ArrayList(EpochPack).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.index.deinit();
        for (self.packs.items) |*pack| {
            pack.deinit();
        }
        self.packs.deinit();
    }

    /// Adds a pack to the index, indexing all its objects.
    pub fn addPack(self: *Self, pack: EpochPack) !void {
        const pack_idx = self.packs.items.len;
        for (pack.objects.items) |obj| {
            try self.index.put(.{ .epoch = pack.epoch, .gindex = obj.gindex }, pack_idx);
        }
        try self.packs.append(pack);
    }

    /// Looks up raw data for a gindex at a specific epoch.
    /// Follows delta chains to resolve the actual data.
    pub fn resolve(self: *const Self, epoch: Epoch, gindex: Gindex) ?[]const u8 {
        return self.resolveWithDepth(epoch, gindex, 0);
    }

    fn resolveWithDepth(self: *const Self, epoch: Epoch, gindex: Gindex, current_depth: usize) ?[]const u8 {
        // Limit depth to prevent infinite loops.
        if (current_depth > 100) return null;

        const pack_idx = self.index.get(.{ .epoch = epoch, .gindex = gindex }) orelse return null;
        const pack = &self.packs.items[pack_idx];

        for (pack.objects.items) |obj| {
            if (obj.gindex == gindex) {
                switch (obj.obj_type) {
                    .raw_chunk => {
                        const start: usize = @intCast(obj.offset);
                        const end: usize = start + obj.size;
                        if (end <= pack.data.items.len) {
                            return pack.data.items[start..end];
                        }
                    },
                    .delta => {
                        // Resolve from base epoch.
                        if (obj.base_epoch) |base_epoch| {
                            return self.resolveWithDepth(base_epoch, gindex, current_depth + 1);
                        }
                    },
                    .aggregate => {},
                }
                break;
            }
        }
        return null;
    }

    /// Returns the total number of packs.
    pub fn packCount(self: *const Self) usize {
        return self.packs.items.len;
    }

    /// Returns the total number of indexed objects.
    pub fn totalObjects(self: *const Self) usize {
        return self.index.count();
    }

    /// Removes packs older than the given epoch (garbage collection).
    pub fn gc(self: *Self, min_epoch: Epoch) void {
        // Remove index entries for old epochs.
        var keys_to_remove = std.ArrayList(PackIndexKey).init(self.allocator);
        defer keys_to_remove.deinit();

        var it = self.index.keyIterator();
        while (it.next()) |key| {
            if (key.epoch < min_epoch) {
                keys_to_remove.append(key.*) catch continue;
            }
        }

        for (keys_to_remove.items) |key| {
            _ = self.index.remove(key);
        }
    }
};

// ─── Epoch Storage (Combined) ─────────────────────────────────────────

/// The combined epoch-partitioned storage system.
///
/// Manages an active (mutable) state and a history of frozen epoch packs.
/// State transitions occur in the active state; finalization freezes the
/// active state into an immutable pack with delta encoding.
pub const EpochStorage = struct {
    /// The mutable active state for the current epoch.
    active: ActiveState,

    /// Global pack index for historical state access.
    pack_index: PackIndex,

    /// Allocator.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .active = ActiveState.init(allocator, 0),
            .pack_index = PackIndex.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.active.deinit();
        self.pack_index.deinit();
    }

    /// Gets the current value for a gindex from the active state.
    pub fn get(self: *const Self, gindex: Gindex) ?[]const u8 {
        return self.active.get(gindex);
    }

    /// Sets a value in the active state.
    pub fn put(self: *Self, gindex: Gindex, value: []const u8) !void {
        try self.active.put(gindex, value);
    }

    /// Finalizes the current epoch, freezing the active state into a pack.
    pub fn finalizeEpoch(self: *Self) !void {
        const prev_pack: ?*const EpochPack = if (self.pack_index.packs.items.len > 0)
            &self.pack_index.packs.items[self.pack_index.packs.items.len - 1]
        else
            null;

        const pack = try self.active.freeze(prev_pack);
        try self.pack_index.addPack(pack);

        self.active.clearDirty();
        self.active.current_epoch += 1;
    }

    /// Resolves a gindex at a historical epoch.
    pub fn getHistorical(self: *const Self, epoch: Epoch, gindex: Gindex) ?[]const u8 {
        return self.pack_index.resolve(epoch, gindex);
    }

    /// Returns the current epoch number.
    pub fn currentEpoch(self: *const Self) Epoch {
        return self.active.current_epoch;
    }

    /// Returns the total number of finalized packs.
    pub fn packCount(self: *const Self) usize {
        return self.pack_index.packCount();
    }
};

// ─── Tests ────────────────────────────────────────────────────────────

test "EpochPack - add raw chunks" {
    const allocator = testing.allocator;
    var pack = EpochPack.init(allocator, 0);
    defer pack.deinit();

    try pack.addRawChunk(1, &[_]u8{ 0xAA, 0xBB });
    try pack.addRawChunk(2, &[_]u8{0xCC});

    try testing.expectEqual(@as(usize, 2), pack.objectCount());
    try testing.expectEqual(@as(usize, 3), pack.dataSize());
    try testing.expectEqual(@as(usize, 0), pack.deltaCount());

    const data = pack.readRaw(1).?;
    try testing.expectEqual(@as(u8, 0xAA), data[0]);
    try testing.expectEqual(@as(u8, 0xBB), data[1]);
}

test "EpochPack - add delta" {
    const allocator = testing.allocator;
    var pack = EpochPack.init(allocator, 1);
    defer pack.deinit();

    try pack.addDelta(1, 0, 0, 32);
    try testing.expectEqual(@as(usize, 1), pack.objectCount());
    try testing.expectEqual(@as(usize, 1), pack.deltaCount());
    try testing.expectEqual(@as(usize, 0), pack.dataSize());
}

test "ActiveState - put, get, freeze" {
    const allocator = testing.allocator;
    var active = ActiveState.init(allocator, 0);
    defer active.deinit();

    try active.put(1, &[_]u8{0xAA});
    try active.put(2, &[_]u8{0xBB});

    try testing.expectEqual(@as(u8, 0xAA), active.get(1).?[0]);
    try testing.expectEqual(@as(usize, 2), active.dirtyCount());

    var pack = try active.freeze(null);
    defer pack.deinit();

    try testing.expectEqual(@as(Epoch, 0), pack.epoch);
    try testing.expectEqual(@as(usize, 2), pack.objectCount());
    try testing.expectEqual(@as(usize, 0), pack.deltaCount());
}

test "ActiveState - delta encoding on freeze" {
    const allocator = testing.allocator;

    // Create initial state and freeze to epoch 0 pack.
    var active = ActiveState.init(allocator, 0);
    try active.put(1, &[_]u8{0xAA});
    try active.put(2, &[_]u8{0xBB});

    var pack0 = try active.freeze(null);
    defer pack0.deinit();

    // Advance to epoch 1, modify only gindex 1.
    active.clearDirty();
    active.current_epoch = 1;
    try active.put(1, &[_]u8{0xFF}); // Modified.
    // gindex 2 is unchanged.

    var pack1 = try active.freeze(&pack0);
    defer pack1.deinit();

    // pack1 should have 1 raw (gindex 1) + 1 delta (gindex 2).
    try testing.expectEqual(@as(usize, 2), pack1.objectCount());
    try testing.expectEqual(@as(usize, 1), pack1.deltaCount());

    active.deinit();
}

test "PackIndex - add and resolve" {
    const allocator = testing.allocator;
    var index = PackIndex.init(allocator);
    defer index.deinit();

    var pack = EpochPack.init(allocator, 0);
    try pack.addRawChunk(1, &[_]u8{0xAA});
    try pack.addRawChunk(2, &[_]u8{0xBB});

    try index.addPack(pack);

    try testing.expectEqual(@as(usize, 1), index.packCount());

    const v1 = index.resolve(0, 1).?;
    try testing.expectEqual(@as(u8, 0xAA), v1[0]);

    const v2 = index.resolve(0, 2).?;
    try testing.expectEqual(@as(u8, 0xBB), v2[0]);

    try testing.expect(index.resolve(0, 99) == null);
    try testing.expect(index.resolve(99, 1) == null);
}

test "PackIndex - resolve delta chain" {
    const allocator = testing.allocator;
    var index = PackIndex.init(allocator);
    defer index.deinit();

    // Epoch 0: raw data for gindex 1.
    var pack0 = EpochPack.init(allocator, 0);
    try pack0.addRawChunk(1, &[_]u8{0xAA});
    try index.addPack(pack0);

    // Epoch 1: delta referencing epoch 0 for gindex 1.
    var pack1 = EpochPack.init(allocator, 1);
    try pack1.addDelta(1, 0, 0, 1);
    try index.addPack(pack1);

    // Resolving epoch 1, gindex 1 should follow the delta to epoch 0.
    const val = index.resolve(1, 1).?;
    try testing.expectEqual(@as(u8, 0xAA), val[0]);
}

test "EpochStorage - basic workflow" {
    const allocator = testing.allocator;
    var storage = EpochStorage.init(allocator);
    defer storage.deinit();

    // Put data in active state.
    try storage.put(1, &[_]u8{0x10});
    try storage.put(2, &[_]u8{0x20});

    // Finalize epoch 0.
    try storage.finalizeEpoch();
    try testing.expectEqual(@as(Epoch, 1), storage.currentEpoch());
    try testing.expectEqual(@as(usize, 1), storage.packCount());

    // Historical access.
    const h1 = storage.getHistorical(0, 1).?;
    try testing.expectEqual(@as(u8, 0x10), h1[0]);
}

test "EpochStorage - multiple epochs" {
    const allocator = testing.allocator;
    var storage = EpochStorage.init(allocator);
    defer storage.deinit();

    try storage.put(1, &[_]u8{0xAA});
    try storage.finalizeEpoch();

    try storage.put(1, &[_]u8{0xBB}); // Modify gindex 1.
    try storage.finalizeEpoch();

    try testing.expectEqual(@as(Epoch, 2), storage.currentEpoch());
    try testing.expectEqual(@as(usize, 2), storage.packCount());

    // Epoch 0 should have the original value.
    const e0 = storage.getHistorical(0, 1).?;
    try testing.expectEqual(@as(u8, 0xAA), e0[0]);

    // Epoch 1 should have the updated value.
    const e1 = storage.getHistorical(1, 1).?;
    try testing.expectEqual(@as(u8, 0xBB), e1[0]);
}
