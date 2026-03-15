const std = @import("std");
const Io = std.Io;
const stream_util = @import("../util/stream.zig");

const readExact = stream_util.readExact;
const writeAll = stream_util.writeAll;

/// Protocol identifier for libp2p ping.
pub const id = "/ipfs/ping/1.0.0";

/// Length of a ping payload in bytes.
pub const payload_length: u32 = 32;

pub const Error = error{
    UnexpectedEof,
    PayloadMismatch,
};

/// Handle an inbound ping: read 32 bytes from the stream, echo them back.
pub fn handleInbound(io: Io, stream: anytype, ctx: anytype) Error!void {
    _ = ctx;
    var buf: [payload_length]u8 = undefined;
    readExact(io, stream, &buf) catch return Error.UnexpectedEof;
    writeAll(io, stream, &buf) catch return Error.UnexpectedEof;
}

/// Handle an outbound ping: send the given payload, wait for echo, verify match.
/// Caller provides the payload via ctx.payload (typically 32 random bytes) and handles RTT measurement.
pub fn handleOutbound(io: Io, stream: anytype, ctx: anytype) Error!void {
    const payload = ctx.payload;
    writeAll(io, stream, payload) catch return Error.UnexpectedEof;

    var response: [payload_length]u8 = undefined;
    readExact(io, stream, &response) catch return Error.UnexpectedEof;

    if (!std.mem.eql(u8, payload, &response)) {
        return Error.PayloadMismatch;
    }
}

// --- Tests ---

const MockStream = stream_util.MockStream;

test "handleInbound echoes payload" {
    const allocator = std.testing.allocator;

    const payload = [_]u8{0x42} ** payload_length;
    var stream = MockStream.init(allocator, &payload);
    defer stream.deinit();

    try handleInbound(undefined, &stream, .{});

    try std.testing.expectEqual(payload_length, stream.write_buf.items.len);
    try std.testing.expectEqualSlices(u8, &payload, stream.write_buf.items);
}

test "handleInbound returns error on short read" {
    const allocator = std.testing.allocator;

    const short_payload = [_]u8{0x42} ** 16;
    var stream = MockStream.init(allocator, &short_payload);
    defer stream.deinit();

    const result = handleInbound(undefined, &stream, .{});
    try std.testing.expectError(Error.UnexpectedEof, result);
}

test "handleOutbound succeeds with matching echo" {
    const allocator = std.testing.allocator;

    const payload = [_]u8{0xAB} ** payload_length;
    var stream = MockStream.init(allocator, &payload);
    defer stream.deinit();

    try handleOutbound(undefined, &stream, .{ .payload = &payload });
    try std.testing.expectEqualSlices(u8, &payload, stream.write_buf.items);
}

test "handleOutbound detects payload mismatch" {
    const allocator = std.testing.allocator;

    const wrong_response = [_]u8{0x00} ** payload_length;
    var stream = MockStream.init(allocator, &wrong_response);
    defer stream.deinit();

    const payload = [_]u8{0xFF} ** payload_length;
    const result = handleOutbound(undefined, &stream, .{ .payload = &payload });
    try std.testing.expectError(Error.PayloadMismatch, result);
}
