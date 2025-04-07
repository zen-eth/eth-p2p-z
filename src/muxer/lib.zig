const std = @import("std");
const testing = std.testing;

pub const yamux = @import("yamux/lib.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
