const std = @import("std");

// Core modules
pub const transport = @import("transport/transport.zig");
pub const protocol = @import("protocol/protocol.zig");
pub const multistream = @import("protocol/multistream.zig");
pub const ping = @import("protocol/ping.zig");
pub const identify = @import("protocol/identify.zig");
pub const gossipsub = @import("protocol/gossipsub/gossipsub.zig");
pub const security = @import("security/security.zig");
pub const io_adapter = @import("io/adapter.zig");
pub const secp_context = @import("security/secp_context.zig");
pub const identity = @import("security/identity.zig");
pub const tls = @import("security/tls.zig");
pub const quic_engine = @import("transport/quic/engine.zig");
pub const quic_transport = @import("transport/quic/quic.zig");
pub const @"switch" = @import("switch.zig");

// Re-export key types
pub const Switch = @"switch".Switch;
pub const SwitchConfig = @"switch".SwitchConfig;
pub const AnyStream = transport.AnyStream;
pub const ProtocolId = protocol.ProtocolId;

// Utility modules
pub const linear_fifo = @import("util/linear_fifo.zig");
pub const protobuf = @import("util/protobuf.zig");
pub const stream = @import("util/stream.zig");

test {
    std.testing.refAllDecls(@This());
}
