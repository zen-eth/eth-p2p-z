//! # io_uring Direct-IO Column Store
//!
//! Inspired by TigerBeetle (Zig LSM Forest), Nethermind Flat DB (column
//! separation), and io_uring async IO research. BeaconState is organized
//! into semantic column families, each backed by an independent flat file
//! with io_uring + O_DIRECT batch IO.
//!
//! ## Design
//!
//! - **Column families**: meta, validators, balances, randao_mixes,
//!   slashings, participation, committees, merkle_cache.
//! - **io_uring batch IO**: When multiple columns are needed (e.g., state
//!   transition reads validators + balances + committees), submit multiple
//!   io_uring SQEs to the same ring for kernel-level parallelism.
//! - **O_DIRECT + aligned buffers**: Bypass page cache for predictable
//!   latency, using `std.mem.alignedAlloc` for 4096-byte alignment.
//! - **Registered buffers**: Pre-register IO buffers with io_uring to
//!   reduce per-operation overhead.
//!
//! ## References
//!
//! - TigerBeetle: Zig-native LSM Forest + JIT compaction
//! - Nethermind Flat DB: Column-separated state storage
//! - io_uring in DBMS (arXiv 2025): async IO benefits in databases
//! - libxev: io_uring backend already used by eth-p2p-z

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Alignment required for O_DIRECT IO (typically 4096 for modern SSDs).
pub const DIRECT_IO_ALIGNMENT: usize = 4096;

/// Maximum size of a single IO operation.
pub const MAX_IO_SIZE: usize = 64 * 1024; // 64 KB

// ─── Column Family Definitions ────────────────────────────────────────

/// Identifies the semantic column families that make up BeaconState.
pub const ColumnFamily = enum(u8) {
    /// Fixed-length metadata: slot, genesis_time, fork, latest_block_header, etc.
    /// Stored in a single 4KB page.
    meta = 0,

    /// Validator records: fixed 121 bytes per validator.
    /// Supports index-based O(1) access.
    validators = 1,

    /// Effective balances: uint64 array, one per validator.
    balances = 2,

    /// RANDAO mixes: ring buffer of 32-byte values.
    randao_mixes = 3,

    /// Slashing data: ring buffer.
    slashings = 4,

    /// Epoch participation flags: bitfield per validator per epoch.
    participation = 5,

    /// Committee assignments.
    committees = 6,

    /// Merkle tree intermediate node cache.
    merkle_cache = 7,

    pub fn name(self: ColumnFamily) []const u8 {
        return switch (self) {
            .meta => "meta",
            .validators => "validators",
            .balances => "balances",
            .randao_mixes => "randao_mixes",
            .slashings => "slashings",
            .participation => "participation",
            .committees => "committees",
            .merkle_cache => "merkle_cache",
        };
    }
};

/// All column families as a comptime-known array.
pub const ALL_COLUMNS: []const ColumnFamily = &[_]ColumnFamily{
    .meta,
    .validators,
    .balances,
    .randao_mixes,
    .slashings,
    .participation,
    .committees,
    .merkle_cache,
};

// ─── Column Metadata ──────────────────────────────────────────────────

/// Size of a single validator record in the validators column.
pub const VALIDATOR_RECORD_SIZE: usize = 121;

/// Size of a balance entry (uint64).
pub const BALANCE_ENTRY_SIZE: usize = 8;

/// Size of a RANDAO mix (32-byte hash).
pub const RANDAO_MIX_SIZE: usize = 32;

/// Number of RANDAO mixes kept (epochs per historical vector).
pub const EPOCHS_PER_HISTORICAL_VECTOR: usize = 65536;

/// Size of the metadata page.
pub const META_PAGE_SIZE: usize = DIRECT_IO_ALIGNMENT; // 4KB

// ─── IO Request ───────────────────────────────────────────────────────

/// Represents a single IO request to be submitted to the io_uring ring.
pub const IoRequest = struct {
    /// Which column this request targets.
    column: ColumnFamily,

    /// Offset within the column file.
    offset: u64,

    /// Length of the IO operation.
    length: u32,

    /// Whether this is a read or write.
    op: IoOp,

    /// Buffer for the data (aligned to DIRECT_IO_ALIGNMENT).
    buffer: []align(DIRECT_IO_ALIGNMENT) u8,

    pub const IoOp = enum { read, write };
};

/// A batch of IO requests to be submitted together.
pub const IoBatch = struct {
    requests: std.ArrayList(IoRequest),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .requests = std.ArrayList(IoRequest).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.requests.deinit();
    }

    /// Adds a read request for the specified column, offset, and length.
    pub fn addRead(self: *Self, column: ColumnFamily, offset: u64, length: u32, buffer: []align(DIRECT_IO_ALIGNMENT) u8) !void {
        try self.requests.append(.{
            .column = column,
            .offset = offset,
            .length = length,
            .op = .read,
            .buffer = buffer,
        });
    }

    /// Adds a write request for the specified column, offset, and length.
    pub fn addWrite(self: *Self, column: ColumnFamily, offset: u64, length: u32, buffer: []align(DIRECT_IO_ALIGNMENT) u8) !void {
        try self.requests.append(.{
            .column = column,
            .offset = offset,
            .length = length,
            .op = .write,
            .buffer = buffer,
        });
    }

    /// Returns the number of requests in this batch.
    pub fn count(self: *const Self) usize {
        return self.requests.items.len;
    }

    /// Clears all requests.
    pub fn reset(self: *Self) void {
        self.requests.clearRetainingCapacity();
    }
};

// ─── Column Store (In-Memory Simulation) ──────────────────────────────

/// Per-column in-memory storage buffer simulating a flat file.
///
/// In production, each column would be a separate file opened with O_DIRECT
/// and accessed through io_uring. This in-memory implementation allows
/// testing the column-store logic without requiring io_uring kernel support.
pub const ColumnBuffer = struct {
    /// The column family this buffer represents.
    family: ColumnFamily,

    /// In-memory data backing the column.
    data: std.ArrayList(u8),

    /// Allocator for buffer growth.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, family: ColumnFamily) Self {
        return Self{
            .family = family,
            .data = std.ArrayList(u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit();
    }

    /// Ensures the buffer has at least `min_size` bytes.
    pub fn ensureSize(self: *Self, min_size: usize) !void {
        if (self.data.items.len < min_size) {
            const extra = min_size - self.data.items.len;
            try self.data.appendNTimes(0, extra);
        }
    }

    /// Writes data at the given offset, growing the buffer if needed.
    pub fn writeAt(self: *Self, offset: usize, data: []const u8) !void {
        const end = offset + data.len;
        try self.ensureSize(end);
        @memcpy(self.data.items[offset..end], data);
    }

    /// Reads data at the given offset. Returns null if out of bounds.
    pub fn readAt(self: *const Self, offset: usize, length: usize) ?[]const u8 {
        const end = offset + length;
        if (end > self.data.items.len) return null;
        return self.data.items[offset..end];
    }

    /// Returns the total size of the column in bytes.
    pub fn size(self: *const Self) usize {
        return self.data.items.len;
    }
};

/// The column store manages all column families for the BeaconState.
///
/// Provides methods to:
/// - Read/write individual validator records, balances, and metadata.
/// - Create batched IO requests for multi-column operations.
/// - Simulate io_uring batch execution in-memory.
pub const ColumnStore = struct {
    /// Column buffers indexed by ColumnFamily enum value.
    columns: [ALL_COLUMNS.len]ColumnBuffer,

    /// Allocator for column buffers and IO batches.
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        var columns: [ALL_COLUMNS.len]ColumnBuffer = undefined;
        for (ALL_COLUMNS, 0..) |col, i| {
            columns[i] = ColumnBuffer.init(allocator, col);
        }
        return Self{
            .columns = columns,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        for (&self.columns) |*col| {
            col.deinit();
        }
    }

    /// Returns the column buffer for the given family.
    pub fn getColumn(self: *Self, family: ColumnFamily) *ColumnBuffer {
        return &self.columns[@intFromEnum(family)];
    }

    /// Returns the column buffer for the given family (const version).
    pub fn getColumnConst(self: *const Self, family: ColumnFamily) *const ColumnBuffer {
        return &self.columns[@intFromEnum(family)];
    }

    // ── High-level accessor methods ──────────────────────────────────

    /// Writes a validator record at the given index.
    pub fn writeValidator(self: *Self, index: u32, data: []const u8) !void {
        const offset = @as(usize, index) * VALIDATOR_RECORD_SIZE;
        const col = self.getColumn(.validators);
        const write_len = @min(data.len, VALIDATOR_RECORD_SIZE);
        try col.writeAt(offset, data[0..write_len]);
    }

    /// Reads a validator record at the given index.
    pub fn readValidator(self: *const Self, index: u32) ?[]const u8 {
        const offset = @as(usize, index) * VALIDATOR_RECORD_SIZE;
        return self.getColumnConst(.validators).readAt(offset, VALIDATOR_RECORD_SIZE);
    }

    /// Writes a balance for the given validator index.
    pub fn writeBalance(self: *Self, validator_index: u32, balance: u64) !void {
        const offset = @as(usize, validator_index) * BALANCE_ENTRY_SIZE;
        const bytes = std.mem.toBytes(balance);
        const col = self.getColumn(.balances);
        try col.writeAt(offset, &bytes);
    }

    /// Reads a balance for the given validator index.
    pub fn readBalance(self: *const Self, validator_index: u32) ?u64 {
        const offset = @as(usize, validator_index) * BALANCE_ENTRY_SIZE;
        const data = self.getColumnConst(.balances).readAt(offset, BALANCE_ENTRY_SIZE) orelse return null;
        return std.mem.readInt(u64, data[0..8], .little);
    }

    /// Writes metadata to the meta column (first 4KB page).
    pub fn writeMeta(self: *Self, data: []const u8) !void {
        const col = self.getColumn(.meta);
        const write_len = @min(data.len, META_PAGE_SIZE);
        try col.writeAt(0, data[0..write_len]);
    }

    /// Reads metadata from the meta column.
    pub fn readMeta(self: *const Self, length: usize) ?[]const u8 {
        return self.getColumnConst(.meta).readAt(0, length);
    }

    /// Writes a RANDAO mix at the given epoch index (ring buffer).
    pub fn writeRandaoMix(self: *Self, epoch_index: usize, mix: [RANDAO_MIX_SIZE]u8) !void {
        const ring_idx = epoch_index % EPOCHS_PER_HISTORICAL_VECTOR;
        const offset = ring_idx * RANDAO_MIX_SIZE;
        const col = self.getColumn(.randao_mixes);
        try col.writeAt(offset, &mix);
    }

    /// Reads a RANDAO mix at the given epoch index.
    pub fn readRandaoMix(self: *const Self, epoch_index: usize) ?[RANDAO_MIX_SIZE]u8 {
        const ring_idx = epoch_index % EPOCHS_PER_HISTORICAL_VECTOR;
        const offset = ring_idx * RANDAO_MIX_SIZE;
        const data = self.getColumnConst(.randao_mixes).readAt(offset, RANDAO_MIX_SIZE) orelse return null;
        var result: [RANDAO_MIX_SIZE]u8 = undefined;
        @memcpy(&result, data);
        return result;
    }

    // ── Batch IO simulation ──────────────────────────────────────────

    /// Creates a new IO batch for multi-column operations.
    pub fn createBatch(self: *const Self) IoBatch {
        return IoBatch.init(self.allocator);
    }

    /// Executes a batch of IO requests against the in-memory column buffers.
    /// In production, this would submit io_uring SQEs and await completions.
    pub fn executeBatch(self: *Self, batch: *const IoBatch) !void {
        for (batch.requests.items) |req| {
            const col = self.getColumn(req.column);
            switch (req.op) {
                .read => {
                    const data = col.data.items;
                    const end = @as(usize, @intCast(req.offset)) + req.length;
                    if (end <= data.len) {
                        const src = data[@intCast(req.offset)..end];
                        @memcpy(req.buffer[0..src.len], src);
                    }
                },
                .write => {
                    try col.writeAt(@intCast(req.offset), req.buffer[0..req.length]);
                },
            }
        }
    }

    /// Returns the total storage size across all columns.
    pub fn totalSize(self: *const Self) usize {
        var total: usize = 0;
        for (&self.columns) |*col| {
            total += col.size();
        }
        return total;
    }
};

// ─── Aligned Buffer Helpers ───────────────────────────────────────────

/// Allocates a buffer aligned to DIRECT_IO_ALIGNMENT.
/// Caller owns the returned slice and must free it with `freeAlignedBuffer`.
pub fn allocAlignedBuffer(allocator: Allocator, size: usize) ![]align(DIRECT_IO_ALIGNMENT) u8 {
    return allocator.alignedAlloc(u8, DIRECT_IO_ALIGNMENT, size);
}

/// Frees a buffer allocated by `allocAlignedBuffer`.
pub fn freeAlignedBuffer(allocator: Allocator, buf: []align(DIRECT_IO_ALIGNMENT) u8) void {
    allocator.free(buf);
}

// ─── Tests ────────────────────────────────────────────────────────────

test "ColumnBuffer - write and read" {
    const allocator = testing.allocator;
    var col = ColumnBuffer.init(allocator, .validators);
    defer col.deinit();

    try col.writeAt(0, &[_]u8{ 0xAA, 0xBB, 0xCC });
    try col.writeAt(10, &[_]u8{ 0xDD, 0xEE });

    const r1 = col.readAt(0, 3).?;
    try testing.expectEqual(@as(u8, 0xAA), r1[0]);
    try testing.expectEqual(@as(u8, 0xBB), r1[1]);
    try testing.expectEqual(@as(u8, 0xCC), r1[2]);

    const r2 = col.readAt(10, 2).?;
    try testing.expectEqual(@as(u8, 0xDD), r2[0]);

    // Out of bounds.
    try testing.expect(col.readAt(100, 1) == null);
}

test "ColumnStore - validators" {
    const allocator = testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    // Write a validator record at index 0.
    var validator_data: [VALIDATOR_RECORD_SIZE]u8 = std.mem.zeroes([VALIDATOR_RECORD_SIZE]u8);
    validator_data[0] = 0x01; // pubkey start
    validator_data[48] = 0x02; // withdrawal credentials start
    try store.writeValidator(0, &validator_data);

    const read = store.readValidator(0).?;
    try testing.expectEqual(@as(u8, 0x01), read[0]);
    try testing.expectEqual(@as(u8, 0x02), read[48]);

    // Non-existent validator.
    try testing.expect(store.readValidator(999) == null);
}

test "ColumnStore - balances" {
    const allocator = testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.writeBalance(0, 32_000_000_000);
    try store.writeBalance(1, 31_500_000_000);

    try testing.expectEqual(@as(u64, 32_000_000_000), store.readBalance(0).?);
    try testing.expectEqual(@as(u64, 31_500_000_000), store.readBalance(1).?);
    try testing.expect(store.readBalance(999) == null);
}

test "ColumnStore - meta" {
    const allocator = testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    var meta: [64]u8 = std.mem.zeroes([64]u8);
    // Slot at offset 0 (little-endian u64).
    std.mem.writeInt(u64, meta[0..8], 12345, .little);
    try store.writeMeta(&meta);

    const read = store.readMeta(64).?;
    const slot = std.mem.readInt(u64, read[0..8], .little);
    try testing.expectEqual(@as(u64, 12345), slot);
}

test "ColumnStore - randao mixes" {
    const allocator = testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    var mix: [RANDAO_MIX_SIZE]u8 = std.mem.zeroes([RANDAO_MIX_SIZE]u8);
    mix[0] = 0xFF;
    mix[31] = 0xAA;

    try store.writeRandaoMix(42, mix);
    const read = store.readRandaoMix(42).?;
    try testing.expectEqual(@as(u8, 0xFF), read[0]);
    try testing.expectEqual(@as(u8, 0xAA), read[31]);
}

test "ColumnStore - total size" {
    const allocator = testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.writeBalance(0, 100);
    try testing.expect(store.totalSize() > 0);
}

test "IoBatch - build batch" {
    const allocator = testing.allocator;
    var batch = IoBatch.init(allocator);
    defer batch.deinit();

    var buf = try allocAlignedBuffer(allocator, DIRECT_IO_ALIGNMENT);
    defer freeAlignedBuffer(allocator, buf);

    try batch.addRead(.validators, 0, 121, buf);
    try batch.addRead(.balances, 0, 8, buf);

    try testing.expectEqual(@as(usize, 2), batch.count());

    batch.reset();
    try testing.expectEqual(@as(usize, 0), batch.count());
}

test "ColumnFamily - name" {
    try testing.expectEqualStrings("meta", ColumnFamily.meta.name());
    try testing.expectEqualStrings("validators", ColumnFamily.validators.name());
    try testing.expectEqualStrings("balances", ColumnFamily.balances.name());
}
