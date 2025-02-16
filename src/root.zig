//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;
// pub const zig_aio = @import("transport/zigaio.zig");
// pub const network_switch = @import("network/switch.zig");
pub const xev_transport = @import("transport/libxev.zig");
pub const future = @import("utils/future.zig");
pub const queue = @import("utils/queue_mpsc.zig");
// pub const tcp = @import("transport/tcp/tcp.zig");
// pub const libuv = @import("transport/libuv.zig");
// pub const transport = @import("transport/transport.zig");
// pub const network = @import("network/network.zig");

// pub const peer_id = @import("peer/id.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
