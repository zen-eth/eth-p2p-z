const std = @import("std");
const libp2p = @import("root.zig");
const PeerId = @import("peer_id").PeerId;

pub const plain = @import("security/plain.zig");
pub const insecure = @import("security/insecure.zig");
pub const tls = @import("security/tls.zig");

pub const Session = struct {
    local_id: []const u8,
    remote_id: []const u8,
    remote_public_key: []const u8,
};

pub const Session1 = struct {
    local_id: PeerId,
    remote_id: PeerId,
    remote_public_key: libp2p.protobuf.keys.PublicKey,
};
