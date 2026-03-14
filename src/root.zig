const std = @import("std");

// Core modules
pub const transport = @import("transport/transport.zig");
pub const protocol = @import("protocol/protocol.zig");
pub const security = @import("security/security.zig");
pub const io_adapter = @import("io/adapter.zig");

// Re-export key types
pub const AnyStream = transport.AnyStream;
pub const ProtocolId = protocol.ProtocolId;

test {
    std.testing.refAllDecls(@This());
}
