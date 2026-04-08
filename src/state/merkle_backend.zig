//! # MerkleBackend — Unified Storage Interface
//!
//! A vtable-based interface that allows the typed view layer to operate over
//! any of the five storage backends without knowing which one is in use.
//! All hash caching and dirty tracking live in this struct, reusing the
//! backends' existing data without duplication.
//!
//! ## Adapter namespaces
//!
//! Each of the five backends provides a companion `Adapter` namespace with a
//! `create(allocator) !MerkleBackend` function that allocates an instance of
//! the backend and wraps it in a `MerkleBackend`.  All adapter instances own
//! their backing store and free it on `deinit()`.
//!
//! ## Dirty tracking
//!
//! `putChunk` marks the written gindex dirty and propagates the dirty flag up
//! the ancestor chain to the root.  `setHash` clears the dirty flag for one
//! gindex (leaf or subtree) and stores its hash.  `isDirty` checks membership
//! in the dirty set.  `getHash` reads the cache.

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

pub const Gindex = u64;

/// A 32-byte SSZ chunk (Merkle leaf) or SHA-256 hash.
pub const Chunk = [32]u8;
pub const Hash = [32]u8;

// ─── MerkleBackend ────────────────────────────────────────────────────

/// Unified Merkle-addressed storage interface.
///
/// Holds a type-erased pointer to a concrete backend plus:
///   - `hash_cache`: cached internal-node hashes (cleared when dirty)
///   - `dirty`: set of gindexes whose subtree has unflushed mutations
pub const MerkleBackend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    hash_cache: std.AutoHashMap(Gindex, Hash),
    dirty: std.AutoHashMap(Gindex, void),
    allocator: Allocator,

    const Self = @This();

    pub const VTable = struct {
        /// Read the 32-byte chunk at `gindex`. Returns null when absent.
        getChunk: *const fn (ptr: *anyopaque, gindex: Gindex) ?Chunk,
        /// Write `data` at `gindex`. Must be idempotent on repeated calls.
        putChunk: *const fn (ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void,
        /// Flush all pending writes to durable storage.
        commit: *const fn (ptr: *anyopaque) anyerror!void,
        /// Free the backend-owned memory. Called by `MerkleBackend.deinit`.
        deinit: *const fn (ptr: *anyopaque) void,
    };

    /// Wraps an existing backend pointer.  Does NOT take ownership of `ptr`;
    /// the caller must ensure `ptr` lives at least as long as this struct.
    /// Use the adapter `create` helpers for an owning wrapper.
    pub fn init(allocator: Allocator, ptr: *anyopaque, vtable: *const VTable) Self {
        return .{
            .ptr = ptr,
            .vtable = vtable,
            .hash_cache = std.AutoHashMap(Gindex, Hash).init(allocator),
            .dirty = std.AutoHashMap(Gindex, void).init(allocator),
            .allocator = allocator,
        };
    }

    /// Release hash cache, dirty set, and the underlying backend.
    pub fn deinit(self: *Self) void {
        self.hash_cache.deinit();
        self.dirty.deinit();
        self.vtable.deinit(self.ptr);
    }

    // ── Read ────────────────────────────────────────────────────────

    /// Read the 32-byte chunk stored at `gindex`.
    pub fn getChunk(self: *const Self, gindex: Gindex) ?Chunk {
        return self.vtable.getChunk(self.ptr, gindex);
    }

    /// True when the subtree at `gindex` has been mutated since the last
    /// `setHash` call for that gindex.
    pub fn isDirty(self: *const Self, gindex: Gindex) bool {
        return self.dirty.contains(gindex);
    }

    /// Return the cached Merkle hash for `gindex`, or null if not yet computed
    /// or if the node is dirty.
    pub fn getHash(self: *const Self, gindex: Gindex) ?Hash {
        return self.hash_cache.get(gindex);
    }

    // ── Write ───────────────────────────────────────────────────────

    /// Write a 32-byte chunk at `gindex`, mark it and all ancestors dirty,
    /// and invalidate their cached hashes.
    pub fn putChunk(self: *Self, gindex: Gindex, data: Chunk) !void {
        try self.vtable.putChunk(self.ptr, gindex, data);
        try self.markDirtyPath(gindex);
    }

    /// Store the precomputed hash for the subtree at `gindex` and clear its
    /// dirty flag (but NOT its ancestors').
    pub fn setHash(self: *Self, gindex: Gindex, hash: Hash) !void {
        try self.hash_cache.put(gindex, hash);
        _ = self.dirty.remove(gindex);
    }

    /// Flush all pending writes to the underlying backend.
    pub fn commit(self: *Self) !void {
        try self.vtable.commit(self.ptr);
    }

    // ── Internal helpers ─────────────────────────────────────────────

    fn markDirtyPath(self: *Self, gindex: Gindex) !void {
        var g = gindex;
        while (true) {
            try self.dirty.put(g, {});
            _ = self.hash_cache.remove(g);
            if (g <= 1) break;
            g >>= 1;
        }
    }
};

// ─── Adapter: ChunkedArenaStore ──────────────────────────────────────

const chunked_arena_mod = @import("chunked_arena.zig");

/// Adapter that wraps `ChunkedArenaStore` as a `MerkleBackend`.
pub const ChunkedArenaAdapter = struct {
    store: chunked_arena_mod.ChunkedArenaStore,
    allocator: Allocator,

    const Self = @This();

    const vtable_instance = MerkleBackend.VTable{
        .getChunk = getChunkFn,
        .putChunk = putChunkFn,
        .commit = commitFn,
        .deinit = deinitFn,
    };

    fn getChunkFn(ptr: *anyopaque, gindex: Gindex) ?Chunk {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.store.getChunk(gindex);
    }

    fn putChunkFn(ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.store.putChunk(gindex, data);
    }

    fn commitFn(ptr: *anyopaque) anyerror!void {
        _ = ptr; // ChunkedArenaStore is in-memory; no flush needed.
    }

    fn deinitFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.store.deinit();
        self.allocator.destroy(self);
    }

    /// Allocate a new `ChunkedArenaStore` and return it wrapped in a `MerkleBackend`.
    pub fn create(allocator: Allocator) !MerkleBackend {
        const adapter = try allocator.create(Self);
        adapter.* = .{
            .store = try chunked_arena_mod.ChunkedArenaStore.init(allocator),
            .allocator = allocator,
        };
        return MerkleBackend.init(allocator, adapter, &vtable_instance);
    }
};

// ─── Adapter: DiffJournalStore ───────────────────────────────────────

const diff_journal_mod = @import("diff_journal.zig");

/// Adapter that wraps `DiffJournalStore` as a `MerkleBackend`.
///
/// Chunks are stored as 32-byte slices via `FlatSnapshot`.  The reverse-diff
/// journal is written on each `putChunk` (slot 0 is used as a placeholder
/// since the adapter is slot-agnostic).
pub const DiffJournalAdapter = struct {
    store: diff_journal_mod.DiffJournalStore,
    allocator: Allocator,

    const Self = @This();

    const vtable_instance = MerkleBackend.VTable{
        .getChunk = getChunkFn,
        .putChunk = putChunkFn,
        .commit = commitFn,
        .deinit = deinitFn,
    };

    fn getChunkFn(ptr: *anyopaque, gindex: Gindex) ?Chunk {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const bytes = self.store.getLatest(gindex) orelse return null;
        if (bytes.len < 32) return null;
        var chunk: Chunk = undefined;
        @memcpy(&chunk, bytes[0..32]);
        return chunk;
    }

    fn putChunkFn(ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        // Slot 0 used as a generic placeholder; callers may advance epochs separately.
        return self.store.applyModification(0, gindex, &data);
    }

    fn commitFn(ptr: *anyopaque) anyerror!void {
        _ = ptr; // In-memory; no IO flush needed.
    }

    fn deinitFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.store.deinit();
        self.allocator.destroy(self);
    }

    pub fn create(allocator: Allocator) !MerkleBackend {
        const adapter = try allocator.create(Self);
        adapter.* = .{
            .store = diff_journal_mod.DiffJournalStore.init(allocator),
            .allocator = allocator,
        };
        return MerkleBackend.init(allocator, adapter, &vtable_instance);
    }
};

// ─── Adapter: ColumnStore ────────────────────────────────────────────

const column_store_mod = @import("column_store.zig");

/// Adapter that wraps `ColumnStore` as a `MerkleBackend`.
///
/// Because the ColumnStore organises data by semantic column rather than
/// gindex, this adapter maintains a parallel `gindex → Chunk` HashMap as the
/// chunk store.  High-level column methods (readValidator, readBalance, etc.)
/// remain available directly on `store`.
pub const ColumnStoreAdapter = struct {
    store: column_store_mod.ColumnStore,
    chunks: std.AutoHashMap(Gindex, Chunk),
    allocator: Allocator,

    const Self = @This();

    const vtable_instance = MerkleBackend.VTable{
        .getChunk = getChunkFn,
        .putChunk = putChunkFn,
        .commit = commitFn,
        .deinit = deinitFn,
    };

    fn getChunkFn(ptr: *anyopaque, gindex: Gindex) ?Chunk {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.chunks.get(gindex);
    }

    fn putChunkFn(ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.chunks.put(gindex, data);
    }

    fn commitFn(ptr: *anyopaque) anyerror!void {
        _ = ptr; // In-memory simulation; no IO flush.
    }

    fn deinitFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.store.deinit();
        self.chunks.deinit();
        self.allocator.destroy(self);
    }

    pub fn create(allocator: Allocator) !MerkleBackend {
        const adapter = try allocator.create(Self);
        adapter.* = .{
            .store = column_store_mod.ColumnStore.init(allocator),
            .chunks = std.AutoHashMap(Gindex, Chunk).init(allocator),
            .allocator = allocator,
        };
        return MerkleBackend.init(allocator, adapter, &vtable_instance);
    }
};

// ─── Adapter: ArtIndex ───────────────────────────────────────────────

const art_mod = @import("adaptive_radix_tree.zig");

/// Adapter that wraps `ArtIndex` as a `MerkleBackend`.
///
/// ART stores variable-length byte slices; chunks are written as 32-byte
/// slices and copied out on read.
pub const ArtAdapter = struct {
    art: art_mod.ArtIndex,
    allocator: Allocator,

    const Self = @This();

    const vtable_instance = MerkleBackend.VTable{
        .getChunk = getChunkFn,
        .putChunk = putChunkFn,
        .commit = commitFn,
        .deinit = deinitFn,
    };

    fn getChunkFn(ptr: *anyopaque, gindex: Gindex) ?Chunk {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const bytes = self.art.search(gindex) orelse return null;
        if (bytes.len < 32) return null;
        var chunk: Chunk = undefined;
        @memcpy(&chunk, bytes[0..32]);
        return chunk;
    }

    fn putChunkFn(ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.art.insert(gindex, &data);
    }

    fn commitFn(ptr: *anyopaque) anyerror!void {
        _ = ptr; // WAL is written on each insert; no separate flush needed.
    }

    fn deinitFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.art.deinit();
        self.allocator.destroy(self);
    }

    pub fn create(allocator: Allocator) !MerkleBackend {
        const adapter = try allocator.create(Self);
        adapter.* = .{
            .art = art_mod.ArtIndex.init(allocator),
            .allocator = allocator,
        };
        return MerkleBackend.init(allocator, adapter, &vtable_instance);
    }
};

// ─── Adapter: EpochStorage ───────────────────────────────────────────

const epoch_storage_mod = @import("epoch_storage.zig");

/// Adapter that wraps `EpochStorage` as a `MerkleBackend`.
///
/// Chunks are stored in the EpochStorage's `ActiveState` as 32-byte slices.
pub const EpochStorageAdapter = struct {
    store: epoch_storage_mod.EpochStorage,
    allocator: Allocator,

    const Self = @This();

    const vtable_instance = MerkleBackend.VTable{
        .getChunk = getChunkFn,
        .putChunk = putChunkFn,
        .commit = commitFn,
        .deinit = deinitFn,
    };

    fn getChunkFn(ptr: *anyopaque, gindex: Gindex) ?Chunk {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const bytes = self.store.get(gindex) orelse return null;
        if (bytes.len < 32) return null;
        var chunk: Chunk = undefined;
        @memcpy(&chunk, bytes[0..32]);
        return chunk;
    }

    fn putChunkFn(ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.store.put(gindex, &data);
    }

    fn commitFn(ptr: *anyopaque) anyerror!void {
        _ = ptr; // Finalization happens externally; no-op here.
    }

    fn deinitFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.store.deinit();
        self.allocator.destroy(self);
    }

    pub fn create(allocator: Allocator) !MerkleBackend {
        const adapter = try allocator.create(Self);
        adapter.* = .{
            .store = epoch_storage_mod.EpochStorage.init(allocator),
            .allocator = allocator,
        };
        return MerkleBackend.init(allocator, adapter, &vtable_instance);
    }
};

// ─── Tests ────────────────────────────────────────────────────────────

test "MerkleBackend via ChunkedArenaAdapter - put and get" {
    const allocator = testing.allocator;
    var backend = try ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0xAB;
    chunk[31] = 0xCD;
    try backend.putChunk(34, chunk);

    const got = backend.getChunk(34).?;
    try testing.expectEqual(@as(u8, 0xAB), got[0]);
    try testing.expectEqual(@as(u8, 0xCD), got[31]);
}

test "MerkleBackend dirty tracking propagates to ancestors" {
    const allocator = testing.allocator;
    var backend = try ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    try backend.putChunk(34, chunk); // gindex 34

    // 34 → 17 → 8 → 4 → 2 → 1 should all be dirty
    try testing.expect(backend.isDirty(34));
    try testing.expect(backend.isDirty(17));
    try testing.expect(backend.isDirty(8));
    try testing.expect(backend.isDirty(4));
    try testing.expect(backend.isDirty(2));
    try testing.expect(backend.isDirty(1));
    try testing.expect(!backend.isDirty(3)); // sibling — not dirty
}

test "MerkleBackend setHash clears dirty and caches hash" {
    const allocator = testing.allocator;
    var backend = try ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    try backend.putChunk(43, chunk);
    try testing.expect(backend.isDirty(43));
    try testing.expect(backend.getHash(43) == null);

    var hash: Hash = std.mem.zeroes(Hash);
    hash[0] = 0xFF;
    try backend.setHash(43, hash);

    try testing.expect(!backend.isDirty(43)); // dirty cleared
    const cached = backend.getHash(43).?;
    try testing.expectEqual(@as(u8, 0xFF), cached[0]);
}

test "MerkleBackend via DiffJournalAdapter - put and get" {
    const allocator = testing.allocator;
    var backend = try DiffJournalAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0x42;
    try backend.putChunk(100, chunk);

    const got = backend.getChunk(100).?;
    try testing.expectEqual(@as(u8, 0x42), got[0]);
}

test "MerkleBackend via ColumnStoreAdapter - put and get" {
    const allocator = testing.allocator;
    var backend = try ColumnStoreAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[5] = 0x55;
    try backend.putChunk(200, chunk);

    const got = backend.getChunk(200).?;
    try testing.expectEqual(@as(u8, 0x55), got[5]);
}

test "MerkleBackend via ArtAdapter - put and get" {
    const allocator = testing.allocator;
    var backend = try ArtAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0x11;
    try backend.putChunk(50, chunk);

    const got = backend.getChunk(50).?;
    try testing.expectEqual(@as(u8, 0x11), got[0]);
}

test "MerkleBackend via EpochStorageAdapter - put and get" {
    const allocator = testing.allocator;
    var backend = try EpochStorageAdapter.create(allocator);
    defer backend.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0x77;
    try backend.putChunk(77, chunk);

    const got = backend.getChunk(77).?;
    try testing.expectEqual(@as(u8, 0x77), got[0]);
}
