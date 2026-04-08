//! # Typed SSZ View Layer
//!
//! Comptime-generated typed accessors over a `MerkleBackend`.  Each view is a
//! small stack-allocated struct (pointer + gindex) that compiles down to a
//! single vtable call per field access — no heap allocation, no tree traversal.
//!
//! ## View types
//!
//! | Type                        | SSZ concept                  |
//! |-----------------------------|------------------------------|
//! | `BasicView(T)`              | uint8/uint64/bool/Bytes32    |
//! | `Bytes32View`               | fixed 32-byte hash / root    |
//! | `ContainerView(N)`          | Container with N fields      |
//! | `ListCompositeView(E, lim)` | List[CompositeType, limit]   |
//! | `ListBasicView(T, lim)`     | List[BasicType, limit]       |
//! | `VectorCompositeView(E, n)` | Vector[CompositeType, n]     |
//! | `VectorBasicView(T, n)`     | Vector[BasicType, n]         |
//!
//! ## Gindex computation
//!
//! All gindex arithmetic is `comptime` where possible; runtime-variable parts
//! (element index for lists) use integer multiply/add at runtime with no
//! additional overhead.

const std = @import("std");
const testing = std.testing;

const merkle_backend = @import("../state/merkle_backend.zig");
pub const MerkleBackend = merkle_backend.MerkleBackend;
pub const Chunk = merkle_backend.Chunk;

const gindex_mod = @import("gindex.zig");
pub const Gindex = gindex_mod.Gindex;
const nextPow2 = gindex_mod.nextPow2;

// ─── BasicView ────────────────────────────────────────────────────────

/// Read/write a single primitive value `T` packed into a 32-byte chunk.
///
/// `T` must be one of: u8, u16, u32, u64, u128, bool, [32]u8.
///
/// The value occupies the first `@sizeOf(T)` bytes of the chunk in
/// little-endian byte order (SSZ canonical encoding).
pub fn BasicView(comptime T: type) type {
    return struct {
        backend: *MerkleBackend,
        gindex: Gindex,

        const Self = @This();

        /// Read the value.  Returns zero if no chunk has been written yet.
        pub inline fn get(self: Self) T {
            const chunk = self.backend.getChunk(self.gindex) orelse return zeroValue(T);
            return decode(T, &chunk);
        }

        /// Write the value.
        pub inline fn set(self: Self, value: T) !void {
            var chunk: Chunk = std.mem.zeroes(Chunk);
            encode(T, &chunk, value);
            try self.backend.putChunk(self.gindex, chunk);
        }
    };
}

fn zeroValue(comptime T: type) T {
    return switch (@typeInfo(T)) {
        .int, .comptime_int => 0,
        .bool => false,
        .array => std.mem.zeroes(T),
        else => @compileError("unsupported BasicView type: " ++ @typeName(T)),
    };
}

fn decode(comptime T: type, chunk: *const Chunk) T {
    return switch (@typeInfo(T)) {
        .int => std.mem.readInt(T, chunk[0..@sizeOf(T)], .little),
        .bool => chunk[0] != 0,
        .array => |a| blk: {
            if (a.child != u8) @compileError("only [N]u8 arrays supported in BasicView");
            var arr: T = undefined;
            @memcpy(&arr, chunk[0..@sizeOf(T)]);
            break :blk arr;
        },
        else => @compileError("unsupported BasicView type: " ++ @typeName(T)),
    };
}

fn encode(comptime T: type, chunk: *Chunk, value: T) void {
    switch (@typeInfo(T)) {
        .int => std.mem.writeInt(T, chunk[0..@sizeOf(T)], value, .little),
        .bool => chunk[0] = if (value) 1 else 0,
        .array => |a| {
            if (a.child != u8) @compileError("only [N]u8 arrays supported in BasicView");
            @memcpy(chunk[0..@sizeOf(T)], &value);
        },
        else => @compileError("unsupported BasicView type: " ++ @typeName(T)),
    }
}

// ─── Bytes32View ─────────────────────────────────────────────────────

/// A view over a single 32-byte SSZ Bytes32 / root stored as one chunk.
pub const Bytes32View = struct {
    backend: *MerkleBackend,
    gindex: Gindex,

    const Self = @This();

    pub inline fn get(self: Self) [32]u8 {
        return self.backend.getChunk(self.gindex) orelse std.mem.zeroes([32]u8);
    }

    pub inline fn set(self: Self, value: [32]u8) !void {
        try self.backend.putChunk(self.gindex, value);
    }
};

// ─── ContainerView ───────────────────────────────────────────────────

/// A view over a Container with `num_fields` fields.
///
/// The container's Merkle tree has `pad = nextPow2(num_fields)` leaf slots.
/// Field `i` lives at absolute gindex `base * pad + i`.
///
/// Callers typically do not use `ContainerView` directly; instead they wrap
/// it in a concrete type (like `ValidatorView`) that exposes typed field
/// accessors.
pub fn ContainerView(comptime num_fields: u64) type {
    const PAD = comptime nextPow2(num_fields);

    return struct {
        backend: *MerkleBackend,
        base_gindex: Gindex,

        const Self = @This();
        pub const field_count = num_fields;
        pub const field_pad = PAD;

        /// Return the absolute gindex of field `idx` (comptime).
        pub inline fn fieldGindex(self: Self, comptime idx: u64) Gindex {
            comptime std.debug.assert(idx < num_fields);
            return self.base_gindex * PAD + idx;
        }

        /// Convenience: build a `BasicView(T)` for field `idx`.
        pub inline fn basicField(self: Self, comptime T: type, comptime idx: u64) BasicView(T) {
            return .{ .backend = self.backend, .gindex = self.fieldGindex(idx) };
        }

        /// Convenience: build a `Bytes32View` for field `idx`.
        pub inline fn bytes32Field(self: Self, comptime idx: u64) Bytes32View {
            return .{ .backend = self.backend, .gindex = self.fieldGindex(idx) };
        }
    };
}

// ─── ListCompositeView ────────────────────────────────────────────────

/// A view over `List[ElementView, limit]` where `ElementView` is a composite
/// type (each element occupies its own subtree of depth `element_depth`).
///
/// ## Gindex layout
///
///   list_base               — the list root
///   list_base * 2           — data tree root
///   list_base * 2 + 1       — length mixin (u64 LE in first 8 bytes)
///   data_root * 2^list_depth + i  — element i root
///
/// where `list_depth = log2(nextPow2(limit))`.
pub fn ListCompositeView(comptime ElementView: type, comptime limit: u64) type {
    const LIST_PAD = comptime nextPow2(limit);

    return struct {
        backend: *MerkleBackend,
        base_gindex: Gindex,

        const Self = @This();
        pub const list_limit = limit;
        pub const list_pad = LIST_PAD;

        /// The current length of the list (reads the length-mixin chunk).
        pub inline fn length(self: Self) u64 {
            const len_g = self.base_gindex * 2 + 1;
            const chunk = self.backend.getChunk(len_g) orelse return 0;
            return std.mem.readInt(u64, chunk[0..8], .little);
        }

        /// Set the list length (writes the length-mixin chunk).
        pub inline fn setLength(self: Self, len: u64) !void {
            const len_g = self.base_gindex * 2 + 1;
            var chunk: Chunk = std.mem.zeroes(Chunk);
            std.mem.writeInt(u64, chunk[0..8], len, .little);
            try self.backend.putChunk(len_g, chunk);
        }

        /// Return an `ElementView` for element `index`.
        /// O(1): one multiply + one add — no tree traversal.
        pub inline fn get(self: Self, index: u64) ElementView {
            const data_g = self.base_gindex * 2;
            const elem_g = data_g * LIST_PAD + index;
            return .{ .backend = self.backend, .gindex = elem_g };
        }
    };
}

// ─── ListBasicView ───────────────────────────────────────────────────

/// A view over `List[T, limit]` where `T` is a basic type (uint64, etc.).
///
/// Multiple values of type `T` are packed into 32-byte chunks.
/// For uint64: 4 values per chunk.
///
/// ## Gindex layout
///
///   data_root * nextPow2(chunk_count) + chunk_idx  — the chunk
///
/// where `chunk_count = ceil(limit / items_per_chunk)`.
pub fn ListBasicView(comptime T: type, comptime limit: u64) type {
    const ITEM_SIZE = @sizeOf(T);
    const ITEMS_PER_CHUNK = 32 / ITEM_SIZE;
    const CHUNK_COUNT = (limit + ITEMS_PER_CHUNK - 1) / ITEMS_PER_CHUNK;
    const DATA_PAD = comptime nextPow2(CHUNK_COUNT);

    return struct {
        backend: *MerkleBackend,
        base_gindex: Gindex,

        const Self = @This();
        pub const list_limit = limit;
        pub const items_per_chunk = ITEMS_PER_CHUNK;
        pub const chunk_count = CHUNK_COUNT;

        pub inline fn length(self: Self) u64 {
            const len_g = self.base_gindex * 2 + 1;
            const chunk = self.backend.getChunk(len_g) orelse return 0;
            return std.mem.readInt(u64, chunk[0..8], .little);
        }

        pub inline fn setLength(self: Self, len: u64) !void {
            const len_g = self.base_gindex * 2 + 1;
            var chunk: Chunk = std.mem.zeroes(Chunk);
            std.mem.writeInt(u64, chunk[0..8], len, .little);
            try self.backend.putChunk(len_g, chunk);
        }

        /// Read element at `index`.  Returns zero if never written.
        pub inline fn get(self: Self, index: u64) T {
            const data_g = self.base_gindex * 2;
            const chunk_idx = index / ITEMS_PER_CHUNK;
            const chunk_g = data_g * DATA_PAD + chunk_idx;
            const chunk = self.backend.getChunk(chunk_g) orelse return zeroValue(T);
            const byte_off = (index % ITEMS_PER_CHUNK) * ITEM_SIZE;
            return std.mem.readInt(T, chunk[byte_off..][0..ITEM_SIZE], .little);
        }

        /// Write element at `index`.  Reads the current chunk first to
        /// preserve sibling values packed into the same chunk.
        pub inline fn set(self: Self, index: u64, value: T) !void {
            const data_g = self.base_gindex * 2;
            const chunk_idx = index / ITEMS_PER_CHUNK;
            const chunk_g = data_g * DATA_PAD + chunk_idx;
            var chunk: Chunk = self.backend.getChunk(chunk_g) orelse std.mem.zeroes(Chunk);
            const byte_off = (index % ITEMS_PER_CHUNK) * ITEM_SIZE;
            std.mem.writeInt(T, chunk[byte_off..][0..ITEM_SIZE], value, .little);
            try self.backend.putChunk(chunk_g, chunk);
        }
    };
}

// ─── VectorCompositeView ─────────────────────────────────────────────

/// A view over `Vector[ElementView, n]` (fixed length, composite elements).
///
///   element i gindex = base * nextPow2(n) + i
pub fn VectorCompositeView(comptime ElementView: type, comptime n: u64) type {
    const PAD = comptime nextPow2(n);

    return struct {
        backend: *MerkleBackend,
        base_gindex: Gindex,

        const Self = @This();
        pub const vector_len = n;
        pub const vector_pad = PAD;

        pub inline fn get(self: Self, index: u64) ElementView {
            return .{ .backend = self.backend, .gindex = self.base_gindex * PAD + index };
        }
    };
}

// ─── VectorBasicView ─────────────────────────────────────────────────

/// A view over `Vector[T, n]` where `T` is a basic type.
///
/// Multiple values are packed into chunks exactly like `ListBasicView`, but
/// without a length mixin.
pub fn VectorBasicView(comptime T: type, comptime n: u64) type {
    const ITEM_SIZE = @sizeOf(T);
    const ITEMS_PER_CHUNK = 32 / ITEM_SIZE;
    const CHUNK_COUNT = (n + ITEMS_PER_CHUNK - 1) / ITEMS_PER_CHUNK;
    const PAD = comptime nextPow2(CHUNK_COUNT);

    return struct {
        backend: *MerkleBackend,
        base_gindex: Gindex,

        const Self = @This();
        pub const vector_len = n;
        pub const items_per_chunk = ITEMS_PER_CHUNK;

        pub inline fn get(self: Self, index: u64) T {
            const chunk_idx = index / ITEMS_PER_CHUNK;
            const chunk_g = self.base_gindex * PAD + chunk_idx;
            const chunk = self.backend.getChunk(chunk_g) orelse return zeroValue(T);
            const byte_off = (index % ITEMS_PER_CHUNK) * ITEM_SIZE;
            return std.mem.readInt(T, chunk[byte_off..][0..ITEM_SIZE], .little);
        }

        pub inline fn set(self: Self, index: u64, value: T) !void {
            const chunk_idx = index / ITEMS_PER_CHUNK;
            const chunk_g = self.base_gindex * PAD + chunk_idx;
            var chunk: Chunk = self.backend.getChunk(chunk_g) orelse std.mem.zeroes(Chunk);
            const byte_off = (index % ITEMS_PER_CHUNK) * ITEM_SIZE;
            std.mem.writeInt(T, chunk[byte_off..][0..ITEM_SIZE], value, .little);
            try self.backend.putChunk(chunk_g, chunk);
        }
    };
}

/// Convenience alias: a `Vector[Bytes32, n]` view (e.g. block_roots, state_roots).
pub fn VectorBytes32View(comptime n: u64) type {
    const PAD = comptime nextPow2(n);
    return struct {
        backend: *MerkleBackend,
        base_gindex: Gindex,

        const Self = @This();
        pub const vector_len = n;

        pub inline fn get(self: Self, index: u64) [32]u8 {
            const g = self.base_gindex * PAD + index;
            return self.backend.getChunk(g) orelse std.mem.zeroes([32]u8);
        }

        pub inline fn set(self: Self, index: u64, value: [32]u8) !void {
            const g = self.base_gindex * PAD + index;
            try self.backend.putChunk(g, value);
        }
    };
}

// ─── Tests ────────────────────────────────────────────────────────────

test "BasicView u64 - get and set" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const view = BasicView(u64){ .backend = &backend, .gindex = 34 };
    try testing.expectEqual(@as(u64, 0), view.get());

    try view.set(12345678);
    try testing.expectEqual(@as(u64, 12345678), view.get());
}

test "BasicView bool - get and set" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const view = BasicView(bool){ .backend = &backend, .gindex = 50 };
    try testing.expectEqual(false, view.get());
    try view.set(true);
    try testing.expectEqual(true, view.get());
}

test "ListBasicView u64 - pack 4 per chunk" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    // Balances list: base gindex 44, limit 2^40.
    const Balances = ListBasicView(u64, 1 << 40);
    const view = Balances{ .backend = &backend, .base_gindex = 44 };

    try view.set(0, 32_000_000_000); // 32 ETH in Gwei
    try view.set(1, 16_000_000_000);
    try view.set(2, 24_000_000_000);
    try view.set(3, 8_000_000_000);
    // Elements 0..3 all share one chunk (4 × u64 = 32 bytes).

    try testing.expectEqual(@as(u64, 32_000_000_000), view.get(0));
    try testing.expectEqual(@as(u64, 16_000_000_000), view.get(1));
    try testing.expectEqual(@as(u64, 24_000_000_000), view.get(2));
    try testing.expectEqual(@as(u64, 8_000_000_000), view.get(3));
}

test "VectorBasicView u64 - get and set" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const Slashings = VectorBasicView(u64, 8192);
    const view = Slashings{ .backend = &backend, .base_gindex = 46 };

    try view.set(0, 100);
    try view.set(4, 200);
    try testing.expectEqual(@as(u64, 100), view.get(0));
    try testing.expectEqual(@as(u64, 200), view.get(4));
    try testing.expectEqual(@as(u64, 0), view.get(1));
}

test "VectorBytes32View - get and set" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const BlockRoots = VectorBytes32View(8192);
    const view = BlockRoots{ .backend = &backend, .base_gindex = 37 };

    var root: [32]u8 = std.mem.zeroes([32]u8);
    root[0] = 0xDE;
    root[31] = 0xAD;
    try view.set(0, root);

    const got = view.get(0);
    try testing.expectEqual(@as(u8, 0xDE), got[0]);
    try testing.expectEqual(@as(u8, 0xAD), got[31]);
}

test "ListCompositeView - length mixin" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    // Use a dummy element view type for testing.
    const DummyView = struct {
        backend: *MerkleBackend,
        gindex: Gindex,
    };

    const ValidatorList = ListCompositeView(DummyView, 1 << 40);
    const view = ValidatorList{ .backend = &backend, .base_gindex = 43 };

    try testing.expectEqual(@as(u64, 0), view.length());
    try view.setLength(500_000);
    try testing.expectEqual(@as(u64, 500_000), view.length());
}
