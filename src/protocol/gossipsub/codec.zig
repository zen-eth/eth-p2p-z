const std = @import("std");
const rpc = @import("../../proto/rpc.proto.zig");

/// Maximum RPC message size (1 MiB, per libp2p spec).
pub const max_rpc_size: u32 = 1024 * 1024;

/// Maximum bytes needed for a u64 unsigned varint.
const max_uvarint_bytes: u32 = 10;

pub const Error = error{
    UnexpectedEof,
    MessageTooLarge,
    InvalidVarint,
    InvalidProtobuf,
    OutOfMemory,
};

/// Encode a uvarint into the buffer. Returns the number of bytes written.
fn encodeUvarint(value: usize, buf: []u8) u32 {
    std.debug.assert(buf.len >= max_uvarint_bytes);
    var v = value;
    var i: u32 = 0;
    while (v >= 0x80) : (i += 1) {
        buf[i] = @intCast((v & 0x7f) | 0x80);
        v >>= 7;
    }
    buf[i] = @intCast(v);
    return i + 1;
}

/// Decode a uvarint from a byte slice.
/// Returns the decoded value and the number of bytes consumed.
fn decodeUvarint(data: []const u8) error{ InvalidVarint, UnexpectedEof }!struct { value: usize, bytes_read: u32 } {
    var value: usize = 0;
    var i: u32 = 0;
    while (i < max_uvarint_bytes) : (i += 1) {
        if (i >= data.len) return error.UnexpectedEof;
        const b = data[i];
        const shift: u6 = std.math.cast(u6, i * 7) orelse return error.InvalidVarint;
        value |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) return .{ .value = value, .bytes_read = i + 1 };
    }
    return error.InvalidVarint;
}

/// Encode an RPC message as a varint-length-prefixed protobuf frame.
/// Returns the encoded frame. Caller owns the returned slice.
pub fn encodeRpc(allocator: std.mem.Allocator, rpc_msg: *const rpc.RPC) Error![]const u8 {
    const proto_bytes = rpc_msg.encode(allocator) catch return error.InvalidProtobuf;
    errdefer allocator.free(proto_bytes);

    if (proto_bytes.len > max_rpc_size) {
        allocator.free(proto_bytes);
        return error.MessageTooLarge;
    }

    var len_buf: [max_uvarint_bytes]u8 = undefined;
    const len_bytes = encodeUvarint(proto_bytes.len, &len_buf);

    const frame = allocator.alloc(u8, len_bytes + proto_bytes.len) catch return error.OutOfMemory;
    @memcpy(frame[0..len_bytes], len_buf[0..len_bytes]);
    @memcpy(frame[len_bytes..], proto_bytes);
    allocator.free(proto_bytes);

    return frame;
}

/// Write an RPC message as a varint-length-prefixed protobuf frame to a stream.
pub fn writeRpc(allocator: std.mem.Allocator, stream: anytype, rpc_msg: *const rpc.RPC) Error!void {
    const frame = try encodeRpc(allocator, rpc_msg);
    defer allocator.free(frame);

    var total: usize = 0;
    while (total < frame.len) {
        const n = stream.write(frame[total..]) catch return error.UnexpectedEof;
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }
}

/// A streaming RPC frame decoder.
///
/// Buffers incoming bytes and emits complete RPC frames. Handles partial reads
/// across multiple `feed()` calls. Useful for reading from a stream where
/// message boundaries do not align with read boundaries.
///
/// ## Usage
///
/// ```
/// var decoder = FrameDecoder.init(allocator);
/// defer decoder.deinit();
///
/// decoder.feed(chunk1);
/// while (try decoder.next()) |frame| {
///     defer allocator.free(frame);
///     const reader = try rpc.RPCReader.init(frame);
///     // process reader...
/// }
/// ```
pub const FrameDecoder = struct {
    buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) FrameDecoder {
        return .{
            .buf = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FrameDecoder) void {
        self.buf.deinit(self.allocator);
        self.* = undefined;
    }

    /// Append incoming data to the internal buffer.
    pub fn feed(self: *FrameDecoder, data: []const u8) error{OutOfMemory}!void {
        try self.buf.appendSlice(self.allocator, data);
    }

    /// Try to extract the next complete frame from the buffer.
    /// Returns the frame bytes (caller owns), or null if not enough data yet.
    pub fn next(self: *FrameDecoder) Error!?[]const u8 {
        if (self.buf.items.len == 0) return null;

        const varint = decodeUvarint(self.buf.items) catch |err| switch (err) {
            error.UnexpectedEof => return null,
            error.InvalidVarint => return error.InvalidVarint,
        };

        if (varint.value > max_rpc_size) return error.MessageTooLarge;

        const total_frame = varint.bytes_read + varint.value;
        if (self.buf.items.len < total_frame) return null;

        // Extract the protobuf payload
        const frame = self.allocator.alloc(u8, varint.value) catch return error.OutOfMemory;
        @memcpy(frame, self.buf.items[varint.bytes_read..total_frame]);

        // Compact the buffer: shift remaining data left
        const remaining = self.buf.items.len - total_frame;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buf.items[0..remaining], self.buf.items[total_frame..]);
        }
        self.buf.items.len = remaining;

        return frame;
    }
};

// --- Tests ---

const MockStream = @import("../../util/stream.zig").MockStream;

test "encodeUvarint single byte" {
    var buf: [max_uvarint_bytes]u8 = undefined;
    const n = encodeUvarint(42, &buf);
    try std.testing.expectEqual(@as(u32, 1), n);
    try std.testing.expectEqual(@as(u8, 42), buf[0]);
}

test "encodeUvarint multi-byte" {
    var buf: [max_uvarint_bytes]u8 = undefined;
    const n = encodeUvarint(300, &buf);
    try std.testing.expectEqual(@as(u32, 2), n);
    try std.testing.expectEqual(@as(u8, 0xAC), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x02), buf[1]);
}

test "decodeUvarint roundtrip" {
    var buf: [max_uvarint_bytes]u8 = undefined;
    const values = [_]usize{ 0, 1, 127, 128, 300, 16384, 1048576 };
    for (values) |v| {
        const n = encodeUvarint(v, &buf);
        const result = try decodeUvarint(buf[0..n]);
        try std.testing.expectEqual(v, result.value);
        try std.testing.expectEqual(n, result.bytes_read);
    }
}

test "decodeUvarint rejects truncated input" {
    const result = decodeUvarint(&.{0x80});
    try std.testing.expectError(error.UnexpectedEof, result);
}

test "encodeRpc produces valid frame" {
    const allocator = std.testing.allocator;

    var rpc_msg = rpc.RPC{};
    const frame = try encodeRpc(allocator, &rpc_msg);
    defer allocator.free(frame);

    // Empty RPC encodes to 0 bytes protobuf, so frame is just varint(0)
    try std.testing.expectEqual(@as(u8, 0), frame[0]);
    try std.testing.expectEqual(@as(usize, 1), frame.len);
}

test "encodeRpc with subscriptions" {
    const allocator = std.testing.allocator;

    var subs = [_]?rpc.RPC.SubOpts{
        .{ .subscribe = true, .topicid = "test-topic" },
    };
    var rpc_msg = rpc.RPC{
        .subscriptions = &subs,
    };

    const frame = try encodeRpc(allocator, &rpc_msg);
    defer allocator.free(frame);

    // Frame should be: varint(protobuf_len) ++ protobuf_bytes
    const varint_result = try decodeUvarint(frame);
    try std.testing.expectEqual(frame.len - varint_result.bytes_read, varint_result.value);

    // Decode the protobuf part
    const proto_bytes = frame[varint_result.bytes_read..];
    var reader = rpc.RPCReader.init(proto_bytes) catch return error.InvalidProtobuf;
    const sub = reader.subscriptionsNext().?;
    try std.testing.expect(sub.getSubscribe());
    try std.testing.expectEqualStrings("test-topic", sub.getTopicid());
}

test "writeRpc writes to stream" {
    const allocator = std.testing.allocator;

    var rpc_msg = rpc.RPC{};

    var stream = MockStream.init(allocator, &.{});
    defer stream.deinit();

    try writeRpc(allocator, &stream, &rpc_msg);

    // Should have written a valid frame
    try std.testing.expect(stream.write_buf.items.len > 0);
    const varint_result = try decodeUvarint(stream.write_buf.items);
    try std.testing.expectEqual(stream.write_buf.items.len - varint_result.bytes_read, varint_result.value);
}

test "FrameDecoder single complete message" {
    const allocator = std.testing.allocator;

    // Build a frame
    var rpc_msg = rpc.RPC{};
    const frame = try encodeRpc(allocator, &rpc_msg);
    defer allocator.free(frame);

    var decoder = FrameDecoder.init(allocator);
    defer decoder.deinit();

    try decoder.feed(frame);
    const result = try decoder.next();
    try std.testing.expect(result != null);
    defer allocator.free(result.?);

    // No more messages
    const result2 = try decoder.next();
    try std.testing.expect(result2 == null);
}

test "FrameDecoder partial reads" {
    const allocator = std.testing.allocator;

    var subs = [_]?rpc.RPC.SubOpts{
        .{ .subscribe = true, .topicid = "topic-1" },
    };
    var rpc_msg = rpc.RPC{ .subscriptions = &subs };
    const frame = try encodeRpc(allocator, &rpc_msg);
    defer allocator.free(frame);

    var decoder = FrameDecoder.init(allocator);
    defer decoder.deinit();

    // Feed one byte at a time
    for (0..frame.len - 1) |i| {
        try decoder.feed(frame[i .. i + 1]);
        const result = try decoder.next();
        try std.testing.expect(result == null);
    }

    // Feed last byte
    try decoder.feed(frame[frame.len - 1 ..]);
    const result = try decoder.next();
    try std.testing.expect(result != null);
    defer allocator.free(result.?);

    // Verify decoded content
    var reader = rpc.RPCReader.init(result.?) catch unreachable;
    const sub = reader.subscriptionsNext().?;
    try std.testing.expect(sub.getSubscribe());
    try std.testing.expectEqualStrings("topic-1", sub.getTopicid());
}

test "FrameDecoder multiple messages in one feed" {
    const allocator = std.testing.allocator;

    var rpc_msg = rpc.RPC{};
    const frame1 = try encodeRpc(allocator, &rpc_msg);
    defer allocator.free(frame1);
    const frame2 = try encodeRpc(allocator, &rpc_msg);
    defer allocator.free(frame2);

    const combined = try std.mem.concat(allocator, u8, &.{ frame1, frame2 });
    defer allocator.free(combined);

    var decoder = FrameDecoder.init(allocator);
    defer decoder.deinit();

    try decoder.feed(combined);

    const r1 = try decoder.next();
    try std.testing.expect(r1 != null);
    defer allocator.free(r1.?);

    const r2 = try decoder.next();
    try std.testing.expect(r2 != null);
    defer allocator.free(r2.?);

    const r3 = try decoder.next();
    try std.testing.expect(r3 == null);
}

test "FrameDecoder rejects oversized message" {
    const allocator = std.testing.allocator;

    // Encode a varint claiming max_rpc_size + 1 bytes
    var len_buf: [max_uvarint_bytes]u8 = undefined;
    const n = encodeUvarint(max_rpc_size + 1, &len_buf);

    var decoder = FrameDecoder.init(allocator);
    defer decoder.deinit();

    try decoder.feed(len_buf[0..n]);
    const result = decoder.next();
    try std.testing.expectError(error.MessageTooLarge, result);
}

test "FrameDecoder memory management" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.testing.expect(false) catch @panic("Memory leak detected!");
        }
    }
    const allocator = gpa.allocator();

    {
        var decoder = FrameDecoder.init(allocator);
        defer decoder.deinit();

        var subs = [_]?rpc.RPC.SubOpts{
            .{ .subscribe = true, .topicid = "leak-test" },
        };
        var rpc_msg = rpc.RPC{ .subscriptions = &subs };
        const frame = try encodeRpc(allocator, &rpc_msg);
        defer allocator.free(frame);

        try decoder.feed(frame);
        const result = try decoder.next();
        try std.testing.expect(result != null);
        allocator.free(result.?);
    }
}
