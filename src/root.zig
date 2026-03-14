const std = @import("std");

// Core modules
pub const transport = @import("transport/transport.zig");
pub const protocol = @import("protocol/protocol.zig");
pub const security = @import("security/security.zig");
pub const io_adapter = @import("io/adapter.zig");
pub const secp_context = @import("security/secp_context.zig");
// NOTE: identity requires ssl and peer_id deps — will be added once those are ported (Task 4/7)
// pub const identity = @import("security/identity.zig");

// Re-export key types
pub const AnyStream = transport.AnyStream;
pub const ProtocolId = protocol.ProtocolId;

test {
    std.testing.refAllDecls(@This());
}
