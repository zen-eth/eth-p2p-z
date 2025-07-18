const std = @import("std");
const testing = std.testing;

pub const tcp = @import("tcp/lib.zig");
pub const quic = @import("quic/root.zig");
pub const upgrader = @import("upgrader.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
