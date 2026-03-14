const std = @import("std");
const Io = std.Io;

/// Read exactly `buf.len` bytes from a stream, blocking until complete.
pub fn readExact(io: *Io, stream: anytype, buf: []u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try stream.read(io, buf[total..]);
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }
}

/// Write all bytes to a stream.
pub fn writeAll(io: *Io, stream: anytype, data: []const u8) !void {
    var total: usize = 0;
    while (total < data.len) {
        const n = try stream.write(io, data[total..]);
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

test "adapter placeholder" {
    // Will be filled with real tests once std.Io.Evented is available
}
