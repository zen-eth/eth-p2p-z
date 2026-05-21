const std = @import("std");
const Stream = @import("../quic.zig").Stream;

pub const protocol_id = "/ipfs/ping/1.0.0";

pub const PingHandler = struct {
    pub fn init(_: std.mem.Allocator) PingHandler {
        return .{};
    }

    pub fn run(_: *PingHandler, io: std.Io, stream: *Stream) !void {
        var buf: [32]u8 = undefined;
        try stream.readAll(io, &buf, .{});
        try stream.writeAll(io, &buf, .{});
    }
};
