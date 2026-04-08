//! SSZ module — re-exports all public SSZ types and helpers.

const std = @import("std");
const testing = std.testing;

pub const gindex = @import("ssz/gindex.zig");
pub const view = @import("ssz/view.zig");
pub const hash_tree_root = @import("ssz/hash_tree_root.zig");

// Re-export commonly used types at the module level.
pub const Gindex = gindex.Gindex;
pub const MerkleBackend = @import("state/merkle_backend.zig").MerkleBackend;
pub const Hash = hash_tree_root.Hash;
pub const Chunk = view.Chunk;

pub const BasicView = view.BasicView;
pub const Bytes32View = view.Bytes32View;
pub const ContainerView = view.ContainerView;
pub const ListCompositeView = view.ListCompositeView;
pub const ListBasicView = view.ListBasicView;
pub const VectorCompositeView = view.VectorCompositeView;
pub const VectorBasicView = view.VectorBasicView;
pub const VectorBytes32View = view.VectorBytes32View;

pub const incrementalHashTreeRoot = hash_tree_root.incrementalHashTreeRoot;
pub const listHashTreeRoot = hash_tree_root.listHashTreeRoot;
pub const sha256Pair = hash_tree_root.sha256Pair;
pub const zeroHash = hash_tree_root.zeroHash;

test {
    testing.refAllDeclsRecursive(@This());
}
