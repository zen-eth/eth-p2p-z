//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;

pub const std_options = @import("std_options.zig").options;
pub const concurrent = @import("concurrent.zig");
pub const multistream = @import("multistream/lib.zig");
pub const security = @import("security.zig");
pub const swarm = @import("switch.zig");
pub const protobuf = @import("protobuf.zig");
pub const event = @import("event.zig");
pub const identity = @import("identity.zig");
pub const secp_context = @import("secp_context.zig");

pub const PubSubMessage = protobuf.rpc.Message;

// New comptime API modules (Zig 0.16 rewrite)
pub const tls = security.tls;
pub const ping = @import("protocol/ping.zig");
pub const quic_new = @import("transport/quic/quic.zig");
pub const quic_engine_new = @import("transport/quic/engine.zig");
pub const Switch = swarm.Switch;

test {
    std.testing.refAllDecls(@This());
}
