//! # ValidatorView — Typed SSZ View over a Single Validator Record
//!
//! A Validator is an SSZ Container with 8 fields:
//!
//!   0: pubkey                        BLSPubkey  (48 bytes = 2 chunks)
//!   1: withdrawal_credentials        Bytes32    (1 chunk)
//!   2: effective_balance             uint64     (1 chunk)
//!   3: slashed                       bool       (1 chunk)
//!   4: activation_eligibility_epoch  uint64     (1 chunk)
//!   5: activation_epoch              uint64     (1 chunk)
//!   6: exit_epoch                    uint64     (1 chunk)
//!   7: withdrawable_epoch            uint64     (1 chunk)
//!
//! 8 fields → pad = 8 → depth = 3 from validator root.
//!
//! Gindex layout (relative to this validator's root gindex `G`):
//!
//!   pubkey root           = G * 8 + 0   (subtree with 2 chunks at *2 and *2+1)
//!   withdrawal_creds      = G * 8 + 1   (single chunk)
//!   effective_balance     = G * 8 + 2
//!   slashed               = G * 8 + 3
//!   activation_eligibility_epoch = G * 8 + 4
//!   activation_epoch      = G * 8 + 5
//!   exit_epoch            = G * 8 + 6
//!   withdrawable_epoch    = G * 8 + 7

const std = @import("std");
const testing = std.testing;

const merkle_backend = @import("../state/merkle_backend.zig");
pub const MerkleBackend = merkle_backend.MerkleBackend;
pub const Chunk = merkle_backend.Chunk;

const Gindex = @import("../ssz/gindex.zig").Gindex;
const types = @import("types.zig");

/// Number of fields in a Validator container.
pub const VALIDATOR_FIELD_COUNT: u64 = 8;
/// nextPow2(8) = 8
pub const VALIDATOR_FIELD_PAD: u64 = 8;
/// Subtree depth of a single validator (log2(8) = 3).
pub const VALIDATOR_DEPTH: usize = 3;

/// Typed view over a single Validator stored in a `MerkleBackend`.
///
/// The `gindex` field is the absolute gindex of this validator's subtree root.
/// All field gindexes are derived from it using comptime arithmetic.
pub const ValidatorView = struct {
    backend: *MerkleBackend,
    /// Absolute gindex of this validator's Merkle subtree root in the global tree.
    gindex: Gindex,

    const Self = @This();

    // ── Field gindex helpers (comptime) ─────────────────────────────

    inline fn fieldG(self: Self, comptime idx: u64) Gindex {
        return self.gindex * VALIDATOR_FIELD_PAD + idx;
    }

    // ── pubkey (BLSPubkey = 48 bytes → 2 chunks) ────────────────────

    /// Read the validator's 48-byte BLS public key.
    pub fn pubkey(self: Self) types.BLSPubkey {
        const pk_root = self.fieldG(0);
        var result: types.BLSPubkey = std.mem.zeroes(types.BLSPubkey);
        // Chunk 0: bytes [0..32)
        if (self.backend.getChunk(pk_root * 2)) |c| {
            @memcpy(result[0..32], &c);
        }
        // Chunk 1: bytes [32..48)
        if (self.backend.getChunk(pk_root * 2 + 1)) |c| {
            @memcpy(result[32..48], c[0..16]);
        }
        return result;
    }

    /// Write the validator's 48-byte BLS public key.
    pub fn setPubkey(self: Self, pk: types.BLSPubkey) !void {
        const pk_root = self.fieldG(0);
        var chunk0: Chunk = std.mem.zeroes(Chunk);
        @memcpy(&chunk0, pk[0..32]);
        try self.backend.putChunk(pk_root * 2, chunk0);

        var chunk1: Chunk = std.mem.zeroes(Chunk);
        @memcpy(chunk1[0..16], pk[32..48]);
        try self.backend.putChunk(pk_root * 2 + 1, chunk1);
    }

    // ── withdrawal_credentials (Bytes32) ────────────────────────────

    pub fn withdrawalCredentials(self: Self) types.Bytes32 {
        return self.backend.getChunk(self.fieldG(1)) orelse std.mem.zeroes(types.Bytes32);
    }

    pub fn setWithdrawalCredentials(self: Self, creds: types.Bytes32) !void {
        try self.backend.putChunk(self.fieldG(1), creds);
    }

    // ── effective_balance (uint64) ───────────────────────────────────

    pub fn effectiveBalance(self: Self) types.Gwei {
        const chunk = self.backend.getChunk(self.fieldG(2)) orelse return 0;
        return std.mem.readInt(u64, chunk[0..8], .little);
    }

    pub fn setEffectiveBalance(self: Self, balance: types.Gwei) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        std.mem.writeInt(u64, chunk[0..8], balance, .little);
        try self.backend.putChunk(self.fieldG(2), chunk);
    }

    // ── slashed (bool) ───────────────────────────────────────────────

    pub fn slashed(self: Self) bool {
        const chunk = self.backend.getChunk(self.fieldG(3)) orelse return false;
        return chunk[0] != 0;
    }

    pub fn setSlashed(self: Self, s: bool) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        chunk[0] = if (s) 1 else 0;
        try self.backend.putChunk(self.fieldG(3), chunk);
    }

    // ── activation_eligibility_epoch (uint64) ────────────────────────

    pub fn activationEligibilityEpoch(self: Self) types.Epoch {
        const chunk = self.backend.getChunk(self.fieldG(4)) orelse return std.math.maxInt(u64);
        return std.mem.readInt(u64, chunk[0..8], .little);
    }

    pub fn setActivationEligibilityEpoch(self: Self, e: types.Epoch) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        std.mem.writeInt(u64, chunk[0..8], e, .little);
        try self.backend.putChunk(self.fieldG(4), chunk);
    }

    // ── activation_epoch (uint64) ────────────────────────────────────

    pub fn activationEpoch(self: Self) types.Epoch {
        const chunk = self.backend.getChunk(self.fieldG(5)) orelse return std.math.maxInt(u64);
        return std.mem.readInt(u64, chunk[0..8], .little);
    }

    pub fn setActivationEpoch(self: Self, e: types.Epoch) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        std.mem.writeInt(u64, chunk[0..8], e, .little);
        try self.backend.putChunk(self.fieldG(5), chunk);
    }

    // ── exit_epoch (uint64) ──────────────────────────────────────────

    pub fn exitEpoch(self: Self) types.Epoch {
        const chunk = self.backend.getChunk(self.fieldG(6)) orelse return std.math.maxInt(u64);
        return std.mem.readInt(u64, chunk[0..8], .little);
    }

    pub fn setExitEpoch(self: Self, e: types.Epoch) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        std.mem.writeInt(u64, chunk[0..8], e, .little);
        try self.backend.putChunk(self.fieldG(6), chunk);
    }

    // ── withdrawable_epoch (uint64) ──────────────────────────────────

    pub fn withdrawableEpoch(self: Self) types.Epoch {
        const chunk = self.backend.getChunk(self.fieldG(7)) orelse return std.math.maxInt(u64);
        return std.mem.readInt(u64, chunk[0..8], .little);
    }

    pub fn setWithdrawableEpoch(self: Self, e: types.Epoch) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        std.mem.writeInt(u64, chunk[0..8], e, .little);
        try self.backend.putChunk(self.fieldG(7), chunk);
    }
};

// ─── Tests ────────────────────────────────────────────────────────────

test "ValidatorView - field gindexes" {
    // With validator root at gindex G, field i is at G*8+i.
    // Using G=1 (hypothetical isolated validator):
    const validator_g: Gindex = 1;
    // field 2 (effective_balance) should be at gindex 1*8+2 = 10
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const v = ValidatorView{ .backend = &backend, .gindex = validator_g };
    try v.setEffectiveBalance(32_000_000_000);
    try testing.expectEqual(@as(u64, 32_000_000_000), v.effectiveBalance());

    try v.setSlashed(true);
    try testing.expectEqual(true, v.slashed());

    try v.setActivationEpoch(100);
    try testing.expectEqual(@as(u64, 100), v.activationEpoch());

    try v.setExitEpoch(9999);
    try testing.expectEqual(@as(u64, 9999), v.exitEpoch());
}

test "ValidatorView - pubkey read/write" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const v = ValidatorView{ .backend = &backend, .gindex = 1 };
    var pk: types.BLSPubkey = undefined;
    for (&pk, 0..) |*b, i| b.* = @intCast(i % 256);

    try v.setPubkey(pk);
    const got = v.pubkey();
    try testing.expectEqualSlices(u8, &pk, &got);
}

test "ValidatorView - withdrawal credentials" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const v = ValidatorView{ .backend = &backend, .gindex = 1 };
    var creds: types.Bytes32 = std.mem.zeroes(types.Bytes32);
    creds[0] = 0x01; // ETH1 withdrawal prefix
    creds[11] = 0xAB;

    try v.setWithdrawalCredentials(creds);
    const got = v.withdrawalCredentials();
    try testing.expectEqualSlices(u8, &creds, &got);
}
