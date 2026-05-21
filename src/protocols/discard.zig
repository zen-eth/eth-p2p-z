const std = @import("std");
const Stream = @import("../quic.zig").Stream;

pub const protocol_id = "/discard/1.0.0";

pub const DiscardHandler = struct {
    pub fn init(_: std.mem.Allocator) DiscardHandler {
        return .{};
    }

    pub fn run(_: *DiscardHandler, io: std.Io, stream: *Stream) !void {
        var buf: [1024]u8 = undefined;
        while (true) {
            _ = stream.read(io, &buf, .{}) catch break;
        }
    }
};
