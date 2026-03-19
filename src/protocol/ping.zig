const std = @import("std");
const Io = std.Io;
const stream_util = @import("../util/stream.zig");

const readExact = stream_util.readExact;
const writeAll = stream_util.writeAll;

/// Length of a ping payload in bytes.
pub const payload_length: u32 = 32;

pub const Error = error{
    UnexpectedEof,
    PayloadMismatch,
};

/// Ping protocol handler.
/// Generates random payload internally, measures RTT, stores result in last_rtt_ns.
/// Follows go-libp2p's PingService pattern: self-contained, no caller-provided payload.
pub const Handler = struct {
    /// Protocol identifier for libp2p ping.
    pub const id = "/ipfs/ping/1.0.0";

    /// Last measured round-trip time in nanoseconds (0 = no measurement yet).
    last_rtt_ns: u64 = 0,

    /// Handle an inbound ping: read 32 bytes from the stream, echo them back.
    pub fn handleInbound(_: *Handler, io: Io, stream: anytype, _: anytype) Error!void {
        var buf: [payload_length]u8 = undefined;
        readExact(io, stream, &buf) catch return Error.UnexpectedEof;
        writeAll(io, stream, &buf) catch return Error.UnexpectedEof;
    }

    /// Handle an outbound ping: generate random payload, send it, wait for echo,
    /// verify match, and measure RTT. Called by Switch.newStream automatically.
    pub fn handleOutbound(self: *Handler, io: Io, stream: anytype, _: anytype) Error!void {
        var payload: [payload_length]u8 = undefined;
        fillRandom(&payload);

        const start_ns = timestampNs();
        writeAll(io, stream, &payload) catch return Error.UnexpectedEof;

        var response: [payload_length]u8 = undefined;
        readExact(io, stream, &response) catch return Error.UnexpectedEof;
        const end_ns = timestampNs();

        if (!std.mem.eql(u8, &payload, &response)) {
            return Error.PayloadMismatch;
        }

        self.last_rtt_ns = end_ns - start_ns;
    }

    fn fillRandom(buf: []u8) void {
        std.c.arc4random_buf(buf.ptr, buf.len);
    }
};

fn timestampNs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(std.c.CLOCK.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

// --- Tests ---

const MockStream = stream_util.MockStream;

test "handleInbound echoes payload" {
    const allocator = std.testing.allocator;

    const payload = [_]u8{0x42} ** payload_length;
    var stream = MockStream.init(allocator, &payload);
    defer stream.deinit();

    var handler: Handler = .{};
    try handler.handleInbound(undefined, &stream, .{});
    try std.testing.expectEqual(payload_length, stream.write_buf.items.len);
    try std.testing.expectEqualSlices(u8, &payload, stream.write_buf.items);
}

test "handleInbound returns error on short read" {
    const allocator = std.testing.allocator;

    const short_payload = [_]u8{0x42} ** 16;
    var stream = MockStream.init(allocator, &short_payload);
    defer stream.deinit();

    var handler: Handler = .{};
    const result = handler.handleInbound(undefined, &stream, .{});
    try std.testing.expectError(Error.UnexpectedEof, result);
}

test "handleOutbound succeeds with echo (self-contained)" {
    const allocator = std.testing.allocator;

    // MockStream echoes: handleOutbound writes random payload, then reads it back.
    // We use an EchoMockStream that returns whatever was written.
    var stream = EchoMockStream.init(allocator);
    defer stream.deinit();

    var handler: Handler = .{};
    try handler.handleOutbound(undefined, &stream, .{});
    // RTT should be measured (non-zero on real clock)
    // Payload was 32 bytes written
    try std.testing.expectEqual(payload_length, stream.write_buf.items.len);
}

/// Mock stream that echoes back whatever is written (for outbound ping tests).
const EchoMockStream = struct {
    write_buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) EchoMockStream {
        return .{ .write_buf = .empty, .allocator = allocator };
    }

    fn deinit(self: *EchoMockStream) void {
        self.write_buf.deinit(self.allocator);
    }

    pub fn read(_: *EchoMockStream, _: Io, buf: []u8) anyerror!usize {
        // Echo: return the last written payload.
        // handleOutbound writes first, then reads — g_echo_buf holds the data.
        @memcpy(buf, g_echo_buf[0..buf.len]);
        return buf.len;
    }

    pub fn write(self: *EchoMockStream, _: Io, data: []const u8) anyerror!usize {
        self.write_buf.appendSlice(self.allocator, data) catch return error.OutOfMemory;
        // Store for echo read
        @memcpy(g_echo_buf[0..data.len], data);
        return data.len;
    }

    pub fn close(_: *EchoMockStream, _: Io) void {}

    threadlocal var g_echo_buf: [payload_length]u8 = undefined;
};
