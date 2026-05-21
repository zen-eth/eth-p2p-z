const PeerId = @import("peer_id").PeerId;
const PublicKey = @import("peer_id").PublicKey;

pub const tls = @import("security/tls.zig");

pub const Session = struct {
    local_id: PeerId,
    remote_id: PeerId,
    remote_public_key: PublicKey,
};
