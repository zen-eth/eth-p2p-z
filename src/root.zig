const std = @import("std");

pub const std_options = @import("std_options.zig").options;
pub const identity = @import("identity.zig");
pub const security = @import("security.zig");
pub const quic = @import("quic.zig");
pub const protocols = @import("protocols.zig");
pub const protobuf = @import("protobuf.zig");
pub const peer_record = @import("peer_record.zig");
pub const secp_context = @import("secp_context.zig");
pub const ref_count = @import("ref_count.zig");
pub const RefCount = ref_count.RefCount;
pub const swarm = @import("switch.zig");
// gossipsub lives on the root import path (NOT routed through protocols.zig):
// it imports switch.zig, which imports protocols.zig, so exporting it through
// the protocols chain would risk an import cycle.
pub const gossipsub = @import("protocols/pubsub/gossipsub.zig");

pub const Connection = quic.Connection;
pub const ConnectionStats = quic.ConnectionStats;
pub const Datagram = quic.Datagram;
pub const EndpointStats = quic.EndpointStats;
pub const PathStats = quic.PathStats;
pub const QuicEndpoint = quic.QuicEndpoint;
pub const Stream = quic.Stream;
pub const Switch = swarm.Switch;
pub const SwitchConnection = swarm.SwitchConnection;
pub const ManagedConnection = swarm.ManagedConnection;

pub const PubSubMessage = protobuf.rpc.Message;

test {
    std.testing.refAllDecls(@This());
    _ = @import("quic/config.zig");
    _ = @import("quic/endpoint/mod.zig");
    _ = @import("quic/io/socket_control.zig");
    _ = @import("quic/router/retry_token.zig");
    _ = @import("switch.zig");
    _ = @import("protocols/pubsub/pubsub.zig");
    _ = @import("protocols/pubsub/router.zig");
    _ = @import("protocols/pubsub/gossipsub.zig");
    _ = @import("protocols/pubsub/signing.zig");
    _ = @import("protocols/identify.zig");
    _ = @import("peer_record.zig");
    _ = @import("ref_count.zig");
}
