//! # BeaconStateView — Comptime-Generated Typed View of the Beacon State
//!
//! `BeaconStateView` is the top-level typed cursor over the Beacon State stored
//! in a `MerkleBackend`.  All 28 fields (Capella/Deneb spec) are exposed as
//! inline accessors that compile to a single arithmetic expression + one vtable
//! call per access — no tree traversal, no deserialization.
//!
//! ## SSZ Merkle layout
//!
//! BeaconState has 28 fields → pad = nextPow2(28) = 32 → depth = 5.
//! Root gindex = 1.  Field `i` absolute gindex = 32 + i.
//!
//! | Field | Name                            | Index | Gindex |
//! |-------|---------------------------------|-------|--------|
//! | 0     | genesis_time                    |   0   |  32    |
//! | 1     | genesis_validators_root         |   1   |  33    |
//! | 2     | slot                            |   2   |  34    |
//! | 3     | fork                            |   3   |  35    |
//! | 4     | latest_block_header             |   4   |  36    |
//! | 5     | block_roots                     |   5   |  37    |
//! | 6     | state_roots                     |   6   |  38    |
//! | 7     | historical_roots                |   7   |  39    |
//! | 8     | eth1_data                       |   8   |  40    |
//! | 9     | eth1_data_votes                 |   9   |  41    |
//! | 10    | eth1_deposit_index              |  10   |  42    |
//! | 11    | validators                      |  11   |  43    |
//! | 12    | balances                        |  12   |  44    |
//! | 13    | randao_mixes                    |  13   |  45    |
//! | 14    | slashings                       |  14   |  46    |
//! | 15    | previous_epoch_participation    |  15   |  47    |
//! | 16    | current_epoch_participation     |  16   |  48    |
//! | 17    | justification_bits              |  17   |  49    |
//! | 18    | previous_justified_checkpoint   |  18   |  50    |
//! | 19    | current_justified_checkpoint    |  19   |  51    |
//! | 20    | finalized_checkpoint            |  20   |  52    |
//! | 21    | inactivity_scores               |  21   |  53    |
//! | 22    | current_sync_committee          |  22   |  54    |
//! | 23    | next_sync_committee             |  23   |  55    |
//! | 24    | latest_execution_payload_header |  24   |  56    |
//! | 25    | next_withdrawal_index           |  25   |  57    |
//! | 26    | next_withdrawal_validator_index |  26   |  58    |
//! | 27    | historical_summaries            |  27   |  59    |
//!
//! ## hashTreeRoot
//!
//! `hashTreeRoot()` first computes the hash of each complex field (lists and
//! vectors) and caches them at their respective gindexes.  Then the 5-level
//! container hash is computed via `incrementalHashTreeRoot(backend, 1, 5)`.

const std = @import("std");
const testing = std.testing;

const merkle_backend = @import("../state/merkle_backend.zig");
pub const MerkleBackend = merkle_backend.MerkleBackend;
pub const Chunk = merkle_backend.Chunk;
pub const Hash = merkle_backend.Hash;

const ssz_view = @import("../ssz/view.zig");
const Gindex = @import("../ssz/gindex.zig").Gindex;
const htr = @import("../ssz/hash_tree_root.zig");
const types = @import("types.zig");
const validator_view = @import("validator_view.zig");
pub const ValidatorView = validator_view.ValidatorView;

// ─── Compile-time gindex constants ───────────────────────────────────

// BeaconState: 28 fields, pad = 32, root at gindex 1.
const BS_PAD: u64 = 32;

pub const GENESIS_TIME_GINDEX: Gindex = BS_PAD + 0; //  32
pub const GENESIS_VALIDATORS_ROOT_GINDEX: Gindex = BS_PAD + 1; //  33
pub const SLOT_GINDEX: Gindex = BS_PAD + 2; //  34
pub const FORK_GINDEX: Gindex = BS_PAD + 3; //  35
pub const LATEST_BLOCK_HEADER_GINDEX: Gindex = BS_PAD + 4; //  36
pub const BLOCK_ROOTS_GINDEX: Gindex = BS_PAD + 5; //  37
pub const STATE_ROOTS_GINDEX: Gindex = BS_PAD + 6; //  38
pub const HISTORICAL_ROOTS_GINDEX: Gindex = BS_PAD + 7; //  39
pub const ETH1_DATA_GINDEX: Gindex = BS_PAD + 8; //  40
pub const ETH1_DATA_VOTES_GINDEX: Gindex = BS_PAD + 9; //  41
pub const ETH1_DEPOSIT_INDEX_GINDEX: Gindex = BS_PAD + 10; //  42
pub const VALIDATORS_GINDEX: Gindex = BS_PAD + 11; //  43
pub const BALANCES_GINDEX: Gindex = BS_PAD + 12; //  44
pub const RANDAO_MIXES_GINDEX: Gindex = BS_PAD + 13; //  45
pub const SLASHINGS_GINDEX: Gindex = BS_PAD + 14; //  46
pub const PREVIOUS_EPOCH_PARTICIPATION_GINDEX: Gindex = BS_PAD + 15; //  47
pub const CURRENT_EPOCH_PARTICIPATION_GINDEX: Gindex = BS_PAD + 16; //  48
pub const JUSTIFICATION_BITS_GINDEX: Gindex = BS_PAD + 17; //  49
pub const PREVIOUS_JUSTIFIED_CHECKPOINT_GINDEX: Gindex = BS_PAD + 18; //  50
pub const CURRENT_JUSTIFIED_CHECKPOINT_GINDEX: Gindex = BS_PAD + 19; //  51
pub const FINALIZED_CHECKPOINT_GINDEX: Gindex = BS_PAD + 20; //  52
pub const INACTIVITY_SCORES_GINDEX: Gindex = BS_PAD + 21; //  53
pub const CURRENT_SYNC_COMMITTEE_GINDEX: Gindex = BS_PAD + 22; //  54
pub const NEXT_SYNC_COMMITTEE_GINDEX: Gindex = BS_PAD + 23; //  55
pub const LATEST_EXECUTION_PAYLOAD_HEADER_GINDEX: Gindex = BS_PAD + 24; //  56
pub const NEXT_WITHDRAWAL_INDEX_GINDEX: Gindex = BS_PAD + 25; //  57
pub const NEXT_WITHDRAWAL_VALIDATOR_INDEX_GINDEX: Gindex = BS_PAD + 26; //  58
pub const HISTORICAL_SUMMARIES_GINDEX: Gindex = BS_PAD + 27; //  59

/// BeaconState container subtree depth (log2(32) = 5).
pub const BEACON_STATE_DEPTH: usize = 5;

// ─── Concrete list / vector view types ───────────────────────────────

/// List[Validator, VALIDATOR_REGISTRY_LIMIT] stored at gindex 43.
pub const ValidatorListView = ssz_view.ListCompositeView(ValidatorView, types.VALIDATOR_REGISTRY_LIMIT);

/// List[Gwei, VALIDATOR_REGISTRY_LIMIT] stored at gindex 44.
pub const BalanceListView = ssz_view.ListBasicView(u64, types.VALIDATOR_REGISTRY_LIMIT);

/// Vector[Root, SLOTS_PER_HISTORICAL_ROOT] for block_roots and state_roots.
pub const RootsVectorView = ssz_view.VectorBytes32View(types.SLOTS_PER_HISTORICAL_ROOT);

/// List[Root, HISTORICAL_ROOTS_LIMIT] for historical_roots.
pub const HistoricalRootsView = ssz_view.ListCompositeView(
    ssz_view.Bytes32View,
    types.HISTORICAL_ROOTS_LIMIT,
);

/// Vector[Bytes32, EPOCHS_PER_HISTORICAL_VECTOR] for randao_mixes.
pub const RandaoMixesView = ssz_view.VectorBytes32View(types.EPOCHS_PER_HISTORICAL_VECTOR);

/// Vector[Gwei, EPOCHS_PER_SLASHINGS_VECTOR] for slashings.
pub const SlashingsView = ssz_view.VectorBasicView(u64, types.EPOCHS_PER_SLASHINGS_VECTOR);

/// List[uint8, VALIDATOR_REGISTRY_LIMIT] for epoch participation flags.
pub const ParticipationFlagsView = ssz_view.ListBasicView(u8, types.VALIDATOR_REGISTRY_LIMIT);

/// List[uint64, VALIDATOR_REGISTRY_LIMIT] for inactivity scores.
pub const InactivityScoresView = ssz_view.ListBasicView(u64, types.VALIDATOR_REGISTRY_LIMIT);

// Depths for complex field hash computation.
//
// Validators list data tree depth:
//   40 (list levels for 2^40 limit) + 3 (validator container depth) = 43
//   from data_gindex (base*2) to the validator field leaves.
const VALIDATORS_DATA_DEPTH: usize = 40 + validator_view.VALIDATOR_DEPTH;

// Balances list: u64, 4 per chunk → chunk_count = 2^40/4 = 2^38 → depth 38.
const BALANCES_DATA_DEPTH: usize = 38;

// Block/state roots: Vector[Bytes32, 8192=2^13] → depth 13.
const ROOTS_VECTOR_DEPTH: usize = 13;

// Historical roots: List[Root, 2^24] → data depth 24.
const HISTORICAL_ROOTS_DATA_DEPTH: usize = 24;

// RANDAO mixes: Vector[Bytes32, 2^16=65536] → depth 16.
const RANDAO_MIXES_DEPTH: usize = 16;

// Slashings: Vector[Gwei, 8192=2^13], 4 per chunk → chunk depth 13-2=11? No:
//   chunk_count = 8192/4 = 2048 = 2^11 → depth 11.
const SLASHINGS_DEPTH: usize = 11;

// Participation: List[u8, 2^40], 32 per chunk → chunk_count = 2^40/32 = 2^35 → depth 35.
const PARTICIPATION_DATA_DEPTH: usize = 35;

// Inactivity scores: List[u64, 2^40], 4 per chunk → depth 38 (same as balances).
const INACTIVITY_SCORES_DATA_DEPTH: usize = 38;

// ─── BeaconStateView ─────────────────────────────────────────────────

/// Typed cursor over the Ethereum Beacon State stored in a `MerkleBackend`.
///
/// Create one from any backend:
/// ```zig
/// var backend = try ChunkedArenaAdapter.create(allocator);
/// const state = BeaconStateView{ .backend = &backend };
/// ```
pub const BeaconStateView = struct {
    backend: *MerkleBackend,

    const Self = @This();

    // ── Scalar fields ────────────────────────────────────────────────

    pub inline fn genesisTime(self: Self) u64 {
        return readU64(self.backend, GENESIS_TIME_GINDEX);
    }
    pub inline fn setGenesisTime(self: Self, v: u64) !void {
        try writeU64(self.backend, GENESIS_TIME_GINDEX, v);
    }

    pub inline fn genesisValidatorsRoot(self: Self) types.Bytes32 {
        return self.backend.getChunk(GENESIS_VALIDATORS_ROOT_GINDEX) orelse std.mem.zeroes(types.Bytes32);
    }
    pub inline fn setGenesisValidatorsRoot(self: Self, v: types.Bytes32) !void {
        try self.backend.putChunk(GENESIS_VALIDATORS_ROOT_GINDEX, v);
    }

    pub inline fn slot(self: Self) types.Slot {
        return readU64(self.backend, SLOT_GINDEX);
    }
    pub inline fn setSlot(self: Self, v: types.Slot) !void {
        try writeU64(self.backend, SLOT_GINDEX, v);
    }

    pub inline fn eth1DepositIndex(self: Self) u64 {
        return readU64(self.backend, ETH1_DEPOSIT_INDEX_GINDEX);
    }
    pub inline fn setEth1DepositIndex(self: Self, v: u64) !void {
        try writeU64(self.backend, ETH1_DEPOSIT_INDEX_GINDEX, v);
    }

    pub inline fn justificationBits(self: Self) u8 {
        const chunk = self.backend.getChunk(JUSTIFICATION_BITS_GINDEX) orelse return 0;
        return chunk[0];
    }
    pub inline fn setJustificationBits(self: Self, v: u8) !void {
        var chunk: Chunk = std.mem.zeroes(Chunk);
        chunk[0] = v;
        try self.backend.putChunk(JUSTIFICATION_BITS_GINDEX, chunk);
    }

    pub inline fn nextWithdrawalIndex(self: Self) u64 {
        return readU64(self.backend, NEXT_WITHDRAWAL_INDEX_GINDEX);
    }
    pub inline fn setNextWithdrawalIndex(self: Self, v: u64) !void {
        try writeU64(self.backend, NEXT_WITHDRAWAL_INDEX_GINDEX, v);
    }

    pub inline fn nextWithdrawalValidatorIndex(self: Self) types.ValidatorIndex {
        return readU64(self.backend, NEXT_WITHDRAWAL_VALIDATOR_INDEX_GINDEX);
    }
    pub inline fn setNextWithdrawalValidatorIndex(self: Self, v: types.ValidatorIndex) !void {
        try writeU64(self.backend, NEXT_WITHDRAWAL_VALIDATOR_INDEX_GINDEX, v);
    }

    // ── List / vector field accessors ────────────────────────────────

    pub inline fn validators(self: Self) ValidatorListView {
        return .{ .backend = self.backend, .base_gindex = VALIDATORS_GINDEX };
    }

    pub inline fn balances(self: Self) BalanceListView {
        return .{ .backend = self.backend, .base_gindex = BALANCES_GINDEX };
    }

    pub inline fn blockRoots(self: Self) RootsVectorView {
        return .{ .backend = self.backend, .base_gindex = BLOCK_ROOTS_GINDEX };
    }

    pub inline fn stateRoots(self: Self) RootsVectorView {
        return .{ .backend = self.backend, .base_gindex = STATE_ROOTS_GINDEX };
    }

    pub inline fn historicalRoots(self: Self) HistoricalRootsView {
        return .{ .backend = self.backend, .base_gindex = HISTORICAL_ROOTS_GINDEX };
    }

    pub inline fn randaoMixes(self: Self) RandaoMixesView {
        return .{ .backend = self.backend, .base_gindex = RANDAO_MIXES_GINDEX };
    }

    pub inline fn slashings(self: Self) SlashingsView {
        return .{ .backend = self.backend, .base_gindex = SLASHINGS_GINDEX };
    }

    pub inline fn previousEpochParticipation(self: Self) ParticipationFlagsView {
        return .{ .backend = self.backend, .base_gindex = PREVIOUS_EPOCH_PARTICIPATION_GINDEX };
    }

    pub inline fn currentEpochParticipation(self: Self) ParticipationFlagsView {
        return .{ .backend = self.backend, .base_gindex = CURRENT_EPOCH_PARTICIPATION_GINDEX };
    }

    pub inline fn inactivityScores(self: Self) InactivityScoresView {
        return .{ .backend = self.backend, .base_gindex = INACTIVITY_SCORES_GINDEX };
    }

    // ── hashTreeRoot ─────────────────────────────────────────────────

    /// Compute the SSZ hash_tree_root of the BeaconState.
    ///
    /// Complex fields (lists and vectors) are hashed first; their results are
    /// cached at their respective gindexes.  Then the 5-level container
    /// Merkle root is computed using `incrementalHashTreeRoot`.
    ///
    /// Only dirty paths are visited — unchanged fields reuse cached hashes.
    pub fn hashTreeRoot(self: Self) !Hash {
        const b = self.backend;

        // ── Precompute hashes for complex (non-scalar) fields ────────

        if (b.isDirty(VALIDATORS_GINDEX)) {
            const h = try htr.listHashTreeRoot(b, VALIDATORS_GINDEX, VALIDATORS_DATA_DEPTH);
            try b.setHash(VALIDATORS_GINDEX, h);
        }

        if (b.isDirty(BALANCES_GINDEX)) {
            const h = try htr.listHashTreeRoot(b, BALANCES_GINDEX, BALANCES_DATA_DEPTH);
            try b.setHash(BALANCES_GINDEX, h);
        }

        if (b.isDirty(BLOCK_ROOTS_GINDEX)) {
            const h = try htr.incrementalHashTreeRoot(b, BLOCK_ROOTS_GINDEX, ROOTS_VECTOR_DEPTH);
            try b.setHash(BLOCK_ROOTS_GINDEX, h);
        }

        if (b.isDirty(STATE_ROOTS_GINDEX)) {
            const h = try htr.incrementalHashTreeRoot(b, STATE_ROOTS_GINDEX, ROOTS_VECTOR_DEPTH);
            try b.setHash(STATE_ROOTS_GINDEX, h);
        }

        if (b.isDirty(HISTORICAL_ROOTS_GINDEX)) {
            const h = try htr.listHashTreeRoot(b, HISTORICAL_ROOTS_GINDEX, HISTORICAL_ROOTS_DATA_DEPTH);
            try b.setHash(HISTORICAL_ROOTS_GINDEX, h);
        }

        if (b.isDirty(RANDAO_MIXES_GINDEX)) {
            const h = try htr.incrementalHashTreeRoot(b, RANDAO_MIXES_GINDEX, RANDAO_MIXES_DEPTH);
            try b.setHash(RANDAO_MIXES_GINDEX, h);
        }

        if (b.isDirty(SLASHINGS_GINDEX)) {
            const h = try htr.incrementalHashTreeRoot(b, SLASHINGS_GINDEX, SLASHINGS_DEPTH);
            try b.setHash(SLASHINGS_GINDEX, h);
        }

        if (b.isDirty(PREVIOUS_EPOCH_PARTICIPATION_GINDEX)) {
            const h = try htr.listHashTreeRoot(b, PREVIOUS_EPOCH_PARTICIPATION_GINDEX, PARTICIPATION_DATA_DEPTH);
            try b.setHash(PREVIOUS_EPOCH_PARTICIPATION_GINDEX, h);
        }

        if (b.isDirty(CURRENT_EPOCH_PARTICIPATION_GINDEX)) {
            const h = try htr.listHashTreeRoot(b, CURRENT_EPOCH_PARTICIPATION_GINDEX, PARTICIPATION_DATA_DEPTH);
            try b.setHash(CURRENT_EPOCH_PARTICIPATION_GINDEX, h);
        }

        if (b.isDirty(INACTIVITY_SCORES_GINDEX)) {
            const h = try htr.listHashTreeRoot(b, INACTIVITY_SCORES_GINDEX, INACTIVITY_SCORES_DATA_DEPTH);
            try b.setHash(INACTIVITY_SCORES_GINDEX, h);
        }

        // ── 5-level container hash ───────────────────────────────────
        return htr.incrementalHashTreeRoot(b, 1, BEACON_STATE_DEPTH);
    }
};

// ─── Internal helpers ─────────────────────────────────────────────────

fn readU64(backend: *const MerkleBackend, g: Gindex) u64 {
    const chunk = backend.getChunk(g) orelse return 0;
    return std.mem.readInt(u64, chunk[0..8], .little);
}

fn writeU64(backend: *MerkleBackend, g: Gindex, v: u64) !void {
    var chunk: Chunk = std.mem.zeroes(Chunk);
    std.mem.writeInt(u64, chunk[0..8], v, .little);
    try backend.putChunk(g, chunk);
}

// ─── Tests ────────────────────────────────────────────────────────────

test "BeaconStateView - gindex constants match spec" {
    const t = std.testing;
    try t.expectEqual(@as(Gindex, 34), SLOT_GINDEX);
    try t.expectEqual(@as(Gindex, 43), VALIDATORS_GINDEX);
    try t.expectEqual(@as(Gindex, 44), BALANCES_GINDEX);
    try t.expectEqual(@as(Gindex, 37), BLOCK_ROOTS_GINDEX);
    try t.expectEqual(@as(Gindex, 45), RANDAO_MIXES_GINDEX);
}

test "BeaconStateView - slot read/write" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };
    try testing.expectEqual(@as(u64, 0), state.slot());

    try state.setSlot(12345);
    try testing.expectEqual(@as(u64, 12345), state.slot());

    try state.setSlot(9_999_999);
    try testing.expectEqual(@as(u64, 9_999_999), state.slot());
}

test "BeaconStateView - genesis fields" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };

    try state.setGenesisTime(1606824023); // mainnet genesis
    try testing.expectEqual(@as(u64, 1606824023), state.genesisTime());

    var gvr: types.Bytes32 = std.mem.zeroes(types.Bytes32);
    gvr[0] = 0x4b;
    gvr[31] = 0x39;
    try state.setGenesisValidatorsRoot(gvr);
    const got = state.genesisValidatorsRoot();
    try testing.expectEqual(@as(u8, 0x4b), got[0]);
    try testing.expectEqual(@as(u8, 0x39), got[31]);
}

test "BeaconStateView - validators list basic usage" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };
    const vals = state.validators();

    try testing.expectEqual(@as(u64, 0), vals.length());
    try vals.setLength(500_000);
    try testing.expectEqual(@as(u64, 500_000), vals.length());

    // Write effective_balance for validator 0.
    const v0 = vals.get(0);
    try v0.setEffectiveBalance(32_000_000_000);
    try testing.expectEqual(@as(u64, 32_000_000_000), v0.effectiveBalance());

    // Write effective_balance for validator 1.
    const v1 = vals.get(1);
    try v1.setEffectiveBalance(16_000_000_000);
    try testing.expectEqual(@as(u64, 16_000_000_000), v1.effectiveBalance());

    // Validator 0 is unaffected.
    try testing.expectEqual(@as(u64, 32_000_000_000), vals.get(0).effectiveBalance());
}

test "BeaconStateView - balances list" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };
    const bals = state.balances();

    try bals.set(0, 32_000_000_000);
    try bals.set(1, 16_000_000_000);
    try bals.set(3, 8_000_000_000);

    try testing.expectEqual(@as(u64, 32_000_000_000), bals.get(0));
    try testing.expectEqual(@as(u64, 16_000_000_000), bals.get(1));
    try testing.expectEqual(@as(u64, 0), bals.get(2));
    try testing.expectEqual(@as(u64, 8_000_000_000), bals.get(3));
}

test "BeaconStateView - block_roots and randao_mixes" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };

    var root: types.Bytes32 = std.mem.zeroes(types.Bytes32);
    root[0] = 0xCA;
    try state.blockRoots().set(0, root);
    const got_root = state.blockRoots().get(0);
    try testing.expectEqual(@as(u8, 0xCA), got_root[0]);

    var mix: types.Bytes32 = std.mem.zeroes(types.Bytes32);
    mix[0] = 0xFF;
    try state.randaoMixes().set(0, mix);
    try testing.expectEqual(@as(u8, 0xFF), state.randaoMixes().get(0)[0]);
}

test "BeaconStateView - hashTreeRoot is deterministic" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };
    try state.setSlot(1);
    try state.setGenesisTime(1606824023);

    const r1 = try state.hashTreeRoot();
    const r2 = try state.hashTreeRoot(); // second call must use cached result
    try testing.expectEqual(r1, r2);
}

test "BeaconStateView - hashTreeRoot changes after mutation" {
    const allocator = testing.allocator;
    var backend = try merkle_backend.ChunkedArenaAdapter.create(allocator);
    defer backend.deinit();

    const state = BeaconStateView{ .backend = &backend };
    try state.setSlot(1);
    const r1 = try state.hashTreeRoot();

    try state.setSlot(2); // mutation → dirty
    const r2 = try state.hashTreeRoot();
    try testing.expect(!std.mem.eql(u8, &r1, &r2));
}
