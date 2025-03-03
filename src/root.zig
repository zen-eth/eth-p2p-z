//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;

pub const xev_transport = @import("transport/tcp/libxev.zig");
pub const queue = @import("queue_mpsc.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
