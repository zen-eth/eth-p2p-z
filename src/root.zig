//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;

pub const concurrent = @import("concurrent.zig");
pub const conn = @import("conn.zig");
pub const p2p_transport = @import("transport.zig");
pub const thread_event_loop = @import("thread_event_loop.zig");
pub const transport = @import("transport.zig");
pub const multistream = @import("multistream/lib.zig");
pub const security = @import("security.zig");
pub const @"switch" = @import("switch.zig");
pub const protobuf = @import("protobuf.zig");
pub const protocols = @import("protocols.zig");
pub const mss = @import("multistream/mss.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
