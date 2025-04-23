const std = @import("std");
const testing = std.testing;

pub const noise = @import("noise/noise.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}
