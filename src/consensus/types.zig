//! # Ethereum Beacon Chain — Consensus Type Aliases
//!
//! Thin type aliases and domain-specific wrappers over the primitive Zig types.
//! These match the Ethereum consensus specification's type names exactly.

const std = @import("std");

/// Slot number (monotonically increasing time unit; 12 seconds per slot on mainnet).
pub const Slot = u64;

/// Epoch number (32 slots per epoch on mainnet).
pub const Epoch = u64;

/// Index into the validator registry.
pub const ValidatorIndex = u64;

/// Balance in Gwei (1 ETH = 10^9 Gwei).
pub const Gwei = u64;

/// BLS public key (48 bytes).
pub const BLSPubkey = [48]u8;

/// BLS signature (96 bytes).
pub const BLSSignature = [96]u8;

/// Execution address (Ethereum EVM address; 20 bytes).
pub const ExecutionAddress = [20]u8;

/// A 32-byte SHA-256 hash or SSZ hash_tree_root.
pub const Bytes32 = [32]u8;

/// A 4-byte fork version.
pub const Version = [4]u8;

/// A 32-byte domain.
pub const Domain = [32]u8;

// ─── Consensus constants (mainnet) ───────────────────────────────────

/// Slots per epoch.
pub const SLOTS_PER_EPOCH: Slot = 32;

/// Maximum number of validators (mainnet).
pub const VALIDATOR_REGISTRY_LIMIT: u64 = 1_099_511_627_776; // 2^40

/// Length of the block-roots and state-roots ring buffers.
pub const SLOTS_PER_HISTORICAL_ROOT: u64 = 8192; // 2^13

/// Length of the RANDAO mixes ring buffer.
pub const EPOCHS_PER_HISTORICAL_VECTOR: u64 = 65536; // 2^16

/// Length of the slashings ring buffer.
pub const EPOCHS_PER_SLASHINGS_VECTOR: u64 = 8192; // 2^13

/// Maximum number of historical roots/summaries.
pub const HISTORICAL_ROOTS_LIMIT: u64 = 16_777_216; // 2^24

/// Maximum number of `Eth1Data` votes per epoch.
pub const SLOTS_PER_ETH1_VOTING_PERIOD: u64 = 2048; // 64 epochs × 32 slots

/// Maximum number of pending attestations / participation records per epoch.
pub const MAX_VALIDATORS_PER_COMMITTEE: u64 = 2048;

/// Maximum inactivity scores per validator.
pub const INACTIVITY_SCORE_LIMIT: u64 = 1_099_511_627_776; // same as registry limit

/// Maximum pending withdrawals index.
pub const MAX_WITHDRAWALS_PER_PAYLOAD: u64 = 16;

/// Helper: convert a slot to the containing epoch.
pub fn slotToEpoch(slot: Slot) Epoch {
    return slot / SLOTS_PER_EPOCH;
}
