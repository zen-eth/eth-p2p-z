//! # MutationBuffer — Write-Buffering Layer for Batch State Transitions
//!
//! State transitions write ~2000 chunks per slot.  Instead of hitting the
//! backend on every write, `MutationBuffer` accumulates all mutations in an
//! in-memory `HashMap` and flushes them to the underlying `MerkleBackend` in
//! a single batch, minimising vtable dispatch and enabling IO coalescing
//! (important for io_uring and WAL-based backends).
//!
//! ## Usage
//!
//! ```zig
//! var buf = MutationBuffer.init(allocator, &backend);
//! defer buf.deinit();
//!
//! try buf.putChunk(SLOT_GINDEX, slot_chunk);    // buffered
//! try buf.putChunk(BALANCE_GINDEX, bal_chunk);  // buffered
//! try buf.flush();                              // one batched write
//! ```
//!
//! `MutationBuffer` also implements the `MerkleBackend.VTable` interface so it
//! can itself be wrapped in a `MerkleBackend` and used transparently by the
//! typed view layer.

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

const merkle_backend = @import("merkle_backend.zig");
pub const MerkleBackend = merkle_backend.MerkleBackend;
pub const Chunk = merkle_backend.Chunk;
pub const Hash = merkle_backend.Hash;
pub const Gindex = merkle_backend.Gindex;

/// Write-buffering layer that sits in front of any `MerkleBackend`.
pub const MutationBuffer = struct {
    /// Pending writes, not yet flushed to the underlying backend.
    pending: std.AutoHashMap(Gindex, Chunk),
    /// Gindexes (and ancestors) that have been modified since the last flush.
    /// Propagation matches the semantics of `MerkleBackend.putChunk`.
    dirty_set: std.AutoHashMap(Gindex, void),
    /// Underlying backend to flush into.
    backend: *MerkleBackend,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, backend: *MerkleBackend) Self {
        return .{
            .pending = std.AutoHashMap(Gindex, Chunk).init(allocator),
            .dirty_set = std.AutoHashMap(Gindex, void).init(allocator),
            .backend = backend,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.pending.deinit();
        self.dirty_set.deinit();
    }

    // ── Read (write-through cache) ───────────────────────────────────

    /// Read a chunk.  Returns the buffered value if available, otherwise
    /// falls back to the underlying backend.
    pub fn getChunk(self: *const Self, gindex: Gindex) ?Chunk {
        if (self.pending.get(gindex)) |c| return c;
        return self.backend.getChunk(gindex);
    }

    // ── Write (buffered) ─────────────────────────────────────────────

    /// Buffer a chunk write.  The dirty flag is propagated up the ancestor
    /// chain so that `hashTreeRoot` can determine which paths to recompute.
    pub fn putChunk(self: *Self, gindex: Gindex, data: Chunk) !void {
        try self.pending.put(gindex, data);
        try self.markDirtyPath(gindex);
    }

    /// Flush all buffered mutations to the underlying backend in a single
    /// batch, then clear the buffer.
    ///
    /// Groups writes sequentially; backends that support batch IO (io_uring,
    /// WAL) can further coalesce inside their `putChunk` implementation.
    pub fn flush(self: *Self) !void {
        var it = self.pending.iterator();
        while (it.next()) |entry| {
            try self.backend.putChunk(entry.key_ptr.*, entry.value_ptr.*);
        }
        try self.backend.commit();
        self.pending.clearRetainingCapacity();
        self.dirty_set.clearRetainingCapacity();
    }

    /// Number of pending (unflushed) mutations.
    pub fn pendingCount(self: *const Self) usize {
        return self.pending.count();
    }

    /// True if `gindex` has a pending write that hasn't been flushed.
    pub fn hasPending(self: *const Self, gindex: Gindex) bool {
        return self.pending.contains(gindex);
    }

    // ── MerkleBackend vtable wrapper ─────────────────────────────────

    const vtable_instance = MerkleBackend.VTable{
        .getChunk = getChunkVtable,
        .putChunk = putChunkVtable,
        .commit = commitVtable,
        .deinit = deinitVtable,
    };

    fn getChunkVtable(ptr: *anyopaque, gindex: Gindex) ?Chunk {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.getChunk(gindex);
    }

    fn putChunkVtable(ptr: *anyopaque, gindex: Gindex, data: Chunk) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.putChunk(gindex, data);
    }

    fn commitVtable(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.flush();
    }

    fn deinitVtable(ptr: *anyopaque) void {
        // MutationBuffer manages its own lifetime via deinit().
        // The MerkleBackend wrapper returned by asMerkleBackend() borrows the
        // buffer and must NOT free it; the caller must call buf.deinit()
        // directly.
        _ = ptr;
    }

    /// Wrap this `MutationBuffer` in a `MerkleBackend` so the typed view layer
    /// can use it transparently.
    ///
    /// The returned `MerkleBackend` borrows `self`; `self` must outlive it.
    /// Call `flush()` on the original buffer (not the wrapper) to commit.
    pub fn asMerkleBackend(self: *Self) MerkleBackend {
        return MerkleBackend.init(self.allocator, self, &vtable_instance);
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn markDirtyPath(self: *Self, gindex: Gindex) !void {
        var g = gindex;
        while (true) {
            try self.dirty_set.put(g, {});
            if (g <= 1) break;
            g >>= 1;
        }
    }
};

// ─── Tests ────────────────────────────────────────────────────────────

test "MutationBuffer - buffering without flushing" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var buf = MutationBuffer.init(allocator, &backend);
    defer buf.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0xAB;
    try buf.putChunk(34, chunk);

    // Buffer has it, backend does not yet.
    try testing.expectEqual(@as(usize, 1), buf.pendingCount());
    try testing.expect(buf.hasPending(34));
    try testing.expect(backend.getChunk(34) == null);

    // Read through buffer returns the buffered value.
    const got = buf.getChunk(34).?;
    try testing.expectEqual(@as(u8, 0xAB), got[0]);
}

test "MutationBuffer - flush writes to backend" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var buf = MutationBuffer.init(allocator, &backend);
    defer buf.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0xCD;
    try buf.putChunk(44, chunk);
    try testing.expectEqual(@as(usize, 1), buf.pendingCount());

    try buf.flush();
    try testing.expectEqual(@as(usize, 0), buf.pendingCount());

    // Backend should now have the chunk.
    const got = backend.getChunk(44).?;
    try testing.expectEqual(@as(u8, 0xCD), got[0]);
}

test "MutationBuffer - read falls back to backend after flush" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    // Pre-populate backend with a chunk.
    var existing: Chunk = std.mem.zeroes(Chunk);
    existing[0] = 0x99;
    try backend.putChunk(100, existing);

    var buf = MutationBuffer.init(allocator, &backend);
    defer buf.deinit();

    // Buffer has nothing at gindex 100, so it reads from backend.
    const got = buf.getChunk(100).?;
    try testing.expectEqual(@as(u8, 0x99), got[0]);
}

test "MutationBuffer - dirty propagation to ancestors" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var buf = MutationBuffer.init(allocator, &backend);
    defer buf.deinit();

    var chunk: Chunk = std.mem.zeroes(Chunk);
    try buf.putChunk(34, chunk); // gindex 34 → ancestors 17, 8, 4, 2, 1

    try testing.expect(buf.dirty_set.contains(34));
    try testing.expect(buf.dirty_set.contains(17));
    try testing.expect(buf.dirty_set.contains(8));
    try testing.expect(buf.dirty_set.contains(4));
    try testing.expect(buf.dirty_set.contains(2));
    try testing.expect(buf.dirty_set.contains(1));
    try testing.expect(!buf.dirty_set.contains(3)); // sibling, not dirty
}

test "MutationBuffer - batch multiple writes" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var buf = MutationBuffer.init(allocator, &backend);
    defer buf.deinit();

    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        std.mem.writeInt(u64, chunk[0..8], i, .little);
        try buf.putChunk(1000 + i, chunk);
    }
    try testing.expectEqual(@as(usize, 100), buf.pendingCount());

    try buf.flush();
    try testing.expectEqual(@as(usize, 0), buf.pendingCount());

    // Spot-check a few values in the backend.
    const g50 = backend.getChunk(1050).?;
    try testing.expectEqual(@as(u64, 50), std.mem.readInt(u64, g50[0..8], .little));
}

test "MutationBuffer - asMerkleBackend wrapper" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    var buf = MutationBuffer.init(allocator, &backend);
    defer buf.deinit();

    // Use the buffer through the MerkleBackend interface.
    var wrapped = buf.asMerkleBackend();
    var chunk: Chunk = std.mem.zeroes(Chunk);
    chunk[0] = 0x77;
    try wrapped.putChunk(55, chunk);

    // Data is in buf.pending, not yet in backend.
    const from_buf = buf.getChunk(55).?;
    try testing.expectEqual(@as(u8, 0x77), from_buf[0]);
}
