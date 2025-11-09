//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;

pub const std_options = @import("std_options.zig").options;
pub const concurrent = @import("concurrent.zig");
pub const conn = @import("conn.zig");
pub const thread_event_loop = @import("thread_event_loop.zig");
pub const transport = @import("transport.zig");
pub const multistream = @import("multistream/lib.zig");
pub const security = @import("security.zig");
pub const swarm = @import("switch.zig");
pub const protobuf = @import("protobuf.zig");
pub const protocols = @import("protocols.zig");
pub const event = @import("event.zig");
pub const xev = @import("xev_backend.zig").xev;
pub const identity = @import("identity.zig");
pub const secp_context = @import("secp_context.zig");
pub const QuicStream = transport.QuicStream;
pub const QuicTransport = transport.QuicTransport;

pub const PubSubMessage = protobuf.rpc.Message;

test {
    std.testing.refAllDeclsRecursive(@This());
}
