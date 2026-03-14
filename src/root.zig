const std = @import("std");

// Core modules
pub const transport = @import("transport/transport.zig");
pub const protocol = @import("protocol/protocol.zig");
pub const multistream = @import("protocol/multistream.zig");
pub const ping = @import("protocol/ping.zig");
pub const identify = @import("protocol/identify.zig");
pub const gossipsub = @import("protocol/gossipsub/mcache.zig");
pub const security = @import("security/security.zig");
pub const io_adapter = @import("io/adapter.zig");
pub const secp_context = @import("security/secp_context.zig");
// NOTE: identity requires ssl and peer_id deps — will be added once those are ported (Task 4/7)
// pub const identity = @import("security/identity.zig");

// Re-export key types
pub const AnyStream = transport.AnyStream;
pub const ProtocolId = protocol.ProtocolId;

// Utility modules
pub const linear_fifo = @import("util/linear_fifo.zig");
pub const protobuf = @import("util/protobuf.zig");
pub const stream = @import("util/stream.zig");

test {
    std.testing.refAllDecls(@This());
}
