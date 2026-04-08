//! Consensus module — re-exports beacon chain types and views.

const std = @import("std");
const testing = std.testing;

pub const types = @import("consensus/types.zig");
pub const ValidatorView = @import("consensus/validator_view.zig").ValidatorView;
pub const BeaconStateView = @import("consensus/beacon_state_view.zig").BeaconStateView;
pub const beacon_state_view = @import("consensus/beacon_state_view.zig");

// Re-export the most commonly used consensus types.
pub const Slot = types.Slot;
pub const Epoch = types.Epoch;
pub const ValidatorIndex = types.ValidatorIndex;
pub const Gwei = types.Gwei;
pub const Bytes32 = types.Bytes32;
pub const BLSPubkey = types.BLSPubkey;

test {
    testing.refAllDeclsRecursive(@This());
}
