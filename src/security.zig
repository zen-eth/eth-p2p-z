const std = @import("std");
const PeerId = @import("peer_id").PeerId;
const PublicKey = @import("peer_id").PublicKey;

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
    remote_public_key: PublicKey,
};
