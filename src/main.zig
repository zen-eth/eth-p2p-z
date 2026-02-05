//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");

pub fn main() !void {
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});

    const stdout: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(&buf);

    try writer.interface.print("Run `zig build test` to run the tests.\n", .{});
    try writer.interface.flush();
}
