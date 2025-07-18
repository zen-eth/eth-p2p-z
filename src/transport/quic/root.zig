const std = @import("std");
const testing = std.testing;

pub const lsquic_transport = @import("lsquic.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
