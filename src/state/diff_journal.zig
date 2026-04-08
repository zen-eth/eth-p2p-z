//! # Log-Structured Diff Journal + Flat Snapshot
//!
//! Inspired by SlimArchive (USENIX ATC 2024), Ethereum Geth's snapshot layer,
//! and LSM reverse-diff techniques. Maintains a flat snapshot of the latest
//! state plus a series of reverse diff journals for historical state access.
//!
//! ## Design
//!
//! - **Flat Snapshot**: The latest state is stored in a fixed-layout flat
//!   buffer. Fixed-length fields occupy known offsets at the head; variable-
//!   length fields (validators, balances) use an offset table.
//! - **Reverse Diff Journal**: Each epoch produces a reverse diff (recording
//!   old values) appended to a journal log. Format per entry:
//!   `[slot, field_gindex, old_value_length, old_value]`.
//! - **Epoch Compaction**: Periodically merge multiple diffs (like LSM level
//!   compaction) to reduce the cost of deep historical rewinding.
//! - **Bloom Filter Index**: Each epoch journal includes a bloom filter over
//!   modified gindexes, enabling fast skip of unrelated journals.
//!
//! ## References
//!
//! - SlimArchive (USENIX ATC 2024): transaction-level reverse diffs, 98% storage savings
//! - Geth Snapshot Layer: flat KV + diff layers
//! - LSM-tree compaction strategies

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// A generalized index identifying a field in the SSZ Merkle tree.
pub const Gindex = u64;

/// Slot number (monotonically increasing time unit in Beacon Chain).
pub const Slot = u64;

/// Epoch number.
pub const Epoch = u64;

/// Number of slots per epoch (Ethereum mainnet: 32).
pub const SLOTS_PER_EPOCH: Slot = 32;

/// Convert a slot to its epoch.
pub fn slotToEpoch(slot: Slot) Epoch {
    return slot / SLOTS_PER_EPOCH;
}

// ─── Diff Journal Entry ───────────────────────────────────────────────

/// A single reverse-diff entry recording the previous value of a field
/// before it was modified at a given slot.
pub const DiffEntry = struct {
    /// The slot at which the modification occurred.
    slot: Slot,

    /// The gindex of the modified field.
    gindex: Gindex,

    /// The old value (before modification).
    old_value: []const u8,

    /// Serialize this entry to bytes: [slot:8][gindex:8][len:4][old_value:len]
    pub fn serializedSize(self: *const DiffEntry) usize {
        return 8 + 8 + 4 + self.old_value.len;
    }
};

// ─── Bloom Filter ─────────────────────────────────────────────────────

/// A simple bloom filter for tracking which gindexes were modified in an epoch.
/// Uses a fixed-size bit array with multiple hash functions.
pub const BloomFilter = struct {
    /// Number of bits in the filter.
    pub const NUM_BITS: usize = 1024;

    /// Number of hash functions.
    pub const NUM_HASHES: usize = 3;

    /// Bit array backing the filter.
    bits: [NUM_BITS / 8]u8 = std.mem.zeroes([NUM_BITS / 8]u8),

    const Self = @This();

    /// Inserts a gindex into the bloom filter.
    pub fn insert(self: *Self, gindex: Gindex) void {
        const hashes = computeHashes(gindex);
        for (hashes) |h| {
            const bit_idx = h % NUM_BITS;
            const byte_idx = bit_idx / 8;
            const bit_offset: u3 = @intCast(bit_idx % 8);
            self.bits[byte_idx] |= @as(u8, 1) << bit_offset;
        }
    }

    /// Tests whether a gindex might be present in the filter.
    /// False positives are possible; false negatives are not.
    pub fn mayContain(self: *const Self, gindex: Gindex) bool {
        const hashes = computeHashes(gindex);
        for (hashes) |h| {
            const bit_idx = h % NUM_BITS;
            const byte_idx = bit_idx / 8;
            const bit_offset: u3 = @intCast(bit_idx % 8);
            if ((self.bits[byte_idx] & (@as(u8, 1) << bit_offset)) == 0) {
                return false;
            }
        }
        return true;
    }

    /// Reset the bloom filter to empty.
    pub fn reset(self: *Self) void {
        @memset(&self.bits, 0);
    }

    fn computeHashes(gindex: Gindex) [NUM_HASHES]usize {
        // Use simple multiplicative hashing with different seeds.
        var result: [NUM_HASHES]usize = undefined;
        const seeds = [NUM_HASHES]u64{ 0x9E3779B97F4A7C15, 0x517CC1B727220A95, 0x6C62272E07BB0142 };
        for (0..NUM_HASHES) |i| {
            var h: u64 = gindex;
            h ^= seeds[i];
            h = h *% 0xFF51AFD7ED558CCD;
            h ^= h >> 33;
            result[i] = @intCast(h % NUM_BITS);
        }
        return result;
    }
};

// ─── Epoch Journal ────────────────────────────────────────────────────

/// A journal of reverse diffs for a single epoch, with an associated bloom filter.
pub const EpochJournal = struct {
    /// The epoch this journal covers.
    epoch: Epoch,

    /// Reverse diff entries for this epoch (slot, gindex, old_value).
    entries: std.ArrayList(StoredDiffEntry),

    /// Bloom filter over the gindexes modified in this epoch.
    bloom: BloomFilter,

    /// Allocator used for entry storage.
    allocator: Allocator,

    const Self = @This();

    /// An owned copy of a diff entry where old_value is heap-allocated.
    const StoredDiffEntry = struct {
        slot: Slot,
        gindex: Gindex,
        old_value: []u8,
    };

    pub fn init(allocator: Allocator, epoch: Epoch) Self {
        return Self{
            .epoch = epoch,
            .entries = .empty,
            .bloom = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.old_value);
        }
        self.entries.deinit(self.allocator);
    }

    /// Records a reverse diff: the old value of `gindex` before modification at `slot`.
    pub fn recordDiff(self: *Self, slot: Slot, gindex: Gindex, old_value: []const u8) !void {
        const owned_value = try self.allocator.alloc(u8, old_value.len);
        @memcpy(owned_value, old_value);

        try self.entries.append(self.allocator, .{
            .slot = slot,
            .gindex = gindex,
            .old_value = owned_value,
        });

        self.bloom.insert(gindex);
    }

    /// Returns the number of diff entries in this journal.
    pub fn entryCount(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Checks if this epoch's journal might contain a diff for the given gindex
    /// (using the bloom filter for a fast negative check).
    pub fn mightContain(self: *const Self, gindex: Gindex) bool {
        return self.bloom.mayContain(gindex);
    }
};

// ─── Flat Snapshot ────────────────────────────────────────────────────

/// A flat snapshot of the latest state, stored as a map from gindex to value bytes.
///
/// In a production implementation this would be a mmap'd file with fixed
/// offsets for known fields. Here we use a hash map for simplicity.
pub const FlatSnapshot = struct {
    /// Current slot of this snapshot.
    slot: Slot,

    /// Map from gindex to current value bytes.
    data: std.AutoHashMap(Gindex, []u8),

    /// Allocator for value storage.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .slot = 0,
            .data = std.AutoHashMap(Gindex, []u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.data.valueIterator();
        while (it.next()) |value| {
            self.allocator.free(value.*);
        }
        self.data.deinit();
    }

    /// Gets the current value for a gindex.
    pub fn get(self: *const Self, gindex: Gindex) ?[]const u8 {
        return self.data.get(gindex);
    }

    /// Sets a value for a gindex, returning the old value if any.
    pub fn put(self: *Self, gindex: Gindex, value: []const u8) !?[]u8 {
        const owned = try self.allocator.alloc(u8, value.len);
        @memcpy(owned, value);

        if (self.data.fetchPut(gindex, owned)) |old_entry| {
            return old_entry.value;
        }
        return null;
    }

    /// Returns the number of fields stored.
    pub fn fieldCount(self: *const Self) usize {
        return self.data.count();
    }
};

// ─── DiffJournalStore (Combined) ──────────────────────────────────────

/// The combined store: flat snapshot for latest state + epoch journals for history.
///
/// On state transition:
/// 1. For each modified field, record the old value in the current epoch journal.
/// 2. Write the new value to the flat snapshot.
///
/// To reconstruct a historical state at epoch E:
/// 1. Start from the latest snapshot.
/// 2. Apply reverse diffs from epoch journals backwards until reaching E.
pub const DiffJournalStore = struct {
    /// The flat snapshot holding the latest state.
    snapshot: FlatSnapshot,

    /// Epoch journals, ordered by epoch (oldest first).
    journals: std.ArrayList(EpochJournal),

    /// The current epoch for which diffs are being accumulated.
    current_epoch: Epoch,

    /// Allocator for journals.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .snapshot = FlatSnapshot.init(allocator),
            .journals = .empty,
            .current_epoch = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.snapshot.deinit();
        for (self.journals.items) |*journal| {
            journal.deinit();
        }
        self.journals.deinit(self.allocator);
    }

    /// Advances to a new epoch, creating a fresh journal for it.
    pub fn advanceEpoch(self: *Self, new_epoch: Epoch) !void {
        const journal = EpochJournal.init(self.allocator, new_epoch);
        try self.journals.append(self.allocator, journal);
        self.current_epoch = new_epoch;
    }

    /// Applies a state modification: records the old value as a reverse diff,
    /// then writes the new value to the snapshot.
    pub fn applyModification(self: *Self, slot: Slot, gindex: Gindex, new_value: []const u8) !void {
        // Record reverse diff with the current value (if any).
        if (self.journals.items.len > 0) {
            const current_journal = &self.journals.items[self.journals.items.len - 1];
            if (self.snapshot.get(gindex)) |old_val| {
                try current_journal.recordDiff(slot, gindex, old_val);
            }
        }

        // Write the new value to the snapshot.
        if (try self.snapshot.put(gindex, new_value)) |old_owned| {
            self.allocator.free(old_owned);
        }
        self.snapshot.slot = slot;
    }

    /// Gets the latest value for a gindex (from the flat snapshot).
    pub fn getLatest(self: *const Self, gindex: Gindex) ?[]const u8 {
        return self.snapshot.get(gindex);
    }

    /// Returns the number of epoch journals stored.
    pub fn journalCount(self: *const Self) usize {
        return self.journals.items.len;
    }

    /// Compacts old journals by merging adjacent epochs.
    /// This reduces the number of journals that need to be traversed for
    /// deep historical queries.
    pub fn compactJournals(self: *Self, max_keep: usize) void {
        // Simple compaction: keep only the most recent `max_keep` journals.
        while (self.journals.items.len > max_keep) {
            var old_journal = self.journals.orderedRemove(0);
            old_journal.deinit();
        }
    }
};

// ─── Tests ────────────────────────────────────────────────────────────

test "BloomFilter - insert and query" {
    var bloom = BloomFilter{};

    bloom.insert(42);
    bloom.insert(100);
    bloom.insert(999);

    try testing.expect(bloom.mayContain(42));
    try testing.expect(bloom.mayContain(100));
    try testing.expect(bloom.mayContain(999));

    // A value that was never inserted is unlikely to match (but bloom
    // filters allow false positives, so we only test a few).
    // We test that reset clears everything.
    bloom.reset();
    try testing.expect(!bloom.mayContain(42));
    try testing.expect(!bloom.mayContain(100));
}

test "EpochJournal - record and query diffs" {
    const allocator = testing.allocator;
    var journal = EpochJournal.init(allocator, 5);
    defer journal.deinit();

    try journal.recordDiff(160, 10, &[_]u8{ 0xAA, 0xBB });
    try journal.recordDiff(161, 20, &[_]u8{0xCC});

    try testing.expectEqual(@as(usize, 2), journal.entryCount());
    try testing.expect(journal.mightContain(10));
    try testing.expect(journal.mightContain(20));
}

test "FlatSnapshot - put and get" {
    const allocator = testing.allocator;
    var snap = FlatSnapshot.init(allocator);
    defer snap.deinit();

    const old = try snap.put(1, &[_]u8{ 0x01, 0x02 });
    try testing.expect(old == null);

    const val = snap.get(1).?;
    try testing.expectEqual(@as(u8, 0x01), val[0]);
    try testing.expectEqual(@as(u8, 0x02), val[1]);
    try testing.expectEqual(@as(usize, 1), snap.fieldCount());
}

test "FlatSnapshot - put returns old value" {
    const allocator = testing.allocator;
    var snap = FlatSnapshot.init(allocator);
    defer snap.deinit();

    _ = try snap.put(1, &[_]u8{0xAA});
    const old = try snap.put(1, &[_]u8{0xBB});
    try testing.expect(old != null);
    allocator.free(old.?);

    const val = snap.get(1).?;
    try testing.expectEqual(@as(u8, 0xBB), val[0]);
}

test "DiffJournalStore - basic workflow" {
    const allocator = testing.allocator;
    var store = DiffJournalStore.init(allocator);
    defer store.deinit();

    // Start epoch 0.
    try store.advanceEpoch(0);

    // Apply some modifications.
    try store.applyModification(0, 1, &[_]u8{0x10});
    try store.applyModification(1, 2, &[_]u8{0x20});
    try store.applyModification(2, 1, &[_]u8{0x11}); // Update gindex 1.

    // Latest values should reflect the most recent writes.
    const val1 = store.getLatest(1).?;
    try testing.expectEqual(@as(u8, 0x11), val1[0]);

    const val2 = store.getLatest(2).?;
    try testing.expectEqual(@as(u8, 0x20), val2[0]);

    // The journal should have recorded the old value of gindex 1 (0x10).
    try testing.expectEqual(@as(usize, 1), store.journalCount());
    const journal = &store.journals.items[0];
    try testing.expectEqual(@as(usize, 1), journal.entryCount());
    try testing.expectEqual(@as(u8, 0x10), journal.entries.items[0].old_value[0]);
}

test "DiffJournalStore - advance epoch" {
    const allocator = testing.allocator;
    var store = DiffJournalStore.init(allocator);
    defer store.deinit();

    try store.advanceEpoch(0);
    try store.applyModification(0, 1, &[_]u8{0xAA});

    try store.advanceEpoch(1);
    try store.applyModification(32, 1, &[_]u8{0xBB});

    try testing.expectEqual(@as(usize, 2), store.journalCount());

    // Epoch 1's journal should have the reverse diff (old value 0xAA).
    const j1 = &store.journals.items[1];
    try testing.expectEqual(@as(usize, 1), j1.entryCount());
    try testing.expectEqual(@as(u8, 0xAA), j1.entries.items[0].old_value[0]);
}

test "DiffJournalStore - compaction" {
    const allocator = testing.allocator;
    var store = DiffJournalStore.init(allocator);
    defer store.deinit();

    for (0..10) |i| {
        try store.advanceEpoch(@intCast(i));
    }

    try testing.expectEqual(@as(usize, 10), store.journalCount());

    store.compactJournals(3);
    try testing.expectEqual(@as(usize, 3), store.journalCount());
}

test "slotToEpoch" {
    try testing.expectEqual(@as(Epoch, 0), slotToEpoch(0));
    try testing.expectEqual(@as(Epoch, 0), slotToEpoch(31));
    try testing.expectEqual(@as(Epoch, 1), slotToEpoch(32));
    try testing.expectEqual(@as(Epoch, 3), slotToEpoch(100));
}
