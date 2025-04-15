const std = @import("std");
const testing = std.testing;

pub const tcp = @import("tcp/lib.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
