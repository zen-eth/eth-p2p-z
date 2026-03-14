const std = @import("std");
const identify_pb = @import("../proto/identify.proto.zig");
const stream_util = @import("../util/stream.zig");

const writeAll = stream_util.writeAll;

/// Protocol identifier for libp2p identify.
pub const id = "/ipfs/id/1.0.0";

/// Protocol identifier for libp2p identify push.
pub const push_id = "/ipfs/id/push/1.0.0";

/// Maximum identify message size (8 KiB, per spec).
const max_message_size: u32 = 8 * 1024;

pub const Error = error{
    UnexpectedEof,
    MessageTooLarge,
    InvalidProtobuf,
    TooManyListenAddrs,
    TooManyProtocols,
};

/// Configuration for building identify messages.
pub const Config = struct {
    protocol_version: ?[]const u8 = null,
    agent_version: ?[]const u8 = null,
    public_key: ?[]const u8 = null,
    listen_addrs: ?[]const []const u8 = null,
    observed_addr: ?[]const u8 = null,
    supported_protocols: ?[]const []const u8 = null,
};

const max_listen_addrs: u32 = 64;
const max_protocols: u32 = 128;

/// Handle inbound identify (responder): encode and send our identity.
pub fn handleInbound(
    allocator: std.mem.Allocator,
    stream: anytype,
    config: Config,
) Error!void {
    var msg = identify_pb.Identify{};
    msg.protocol_version = config.protocol_version;
    msg.agent_version = config.agent_version;
    msg.public_key = config.public_key;
    msg.observed_addr = config.observed_addr;

    // Convert listen_addrs []const []const u8 -> []const ?[]const u8
    var listen_addrs_buf: [max_listen_addrs]?[]const u8 = undefined;
    if (config.listen_addrs) |addrs| {
        if (addrs.len > max_listen_addrs) return Error.TooManyListenAddrs;
        for (addrs, 0..) |addr, i| {
            listen_addrs_buf[i] = addr;
        }
        msg.listen_addrs = listen_addrs_buf[0..addrs.len];
    }

    // Convert protocols
    var protocols_buf: [max_protocols]?[]const u8 = undefined;
    if (config.supported_protocols) |protos| {
        if (protos.len > max_protocols) return Error.TooManyProtocols;
        for (protos, 0..) |proto, i| {
            protocols_buf[i] = proto;
        }
        msg.protocols = protocols_buf[0..protos.len];
    }

    const encoded = msg.encode(allocator) catch return Error.InvalidProtobuf;
    defer allocator.free(encoded);

    if (encoded.len > max_message_size) return Error.MessageTooLarge;

    writeAll(stream, encoded) catch return Error.UnexpectedEof;
}

/// Handle outbound identify (initiator): read the remote's identity.
/// Caller owns the returned IdentifyResult and must call `deinit()`.
pub fn handleOutbound(
    allocator: std.mem.Allocator,
    stream: anytype,
) Error!IdentifyResult {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);

    var tmp: [4096]u8 = undefined;
    while (buf.items.len < max_message_size) {
        const n = stream.read(&tmp) catch return Error.UnexpectedEof;
        if (n == 0) break;
        buf.appendSlice(allocator, tmp[0..n]) catch return Error.UnexpectedEof;
    }

    if (buf.items.len == 0) return Error.UnexpectedEof;
    if (buf.items.len > max_message_size) return Error.MessageTooLarge;

    const owned = buf.toOwnedSlice(allocator) catch return Error.UnexpectedEof;

    const reader = identify_pb.IdentifyReader.init(owned) catch {
        allocator.free(owned);
        return Error.InvalidProtobuf;
    };

    return .{
        .reader = reader,
        .raw_bytes = owned,
    };
}

/// Result from an outbound identify handshake.
/// Caller must call `deinit()` to free the backing buffer.
pub const IdentifyResult = struct {
    reader: identify_pb.IdentifyReader,
    raw_bytes: []const u8,

    pub fn deinit(self: *IdentifyResult, allocator: std.mem.Allocator) void {
        allocator.free(self.raw_bytes);
        self.* = undefined;
    }

    pub fn protocolVersion(self: *const IdentifyResult) []const u8 {
        return self.reader.getProtocolVersion();
    }

    pub fn agentVersion(self: *const IdentifyResult) []const u8 {
        return self.reader.getAgentVersion();
    }

    pub fn publicKey(self: *const IdentifyResult) []const u8 {
        return self.reader.getPublicKey();
    }

    pub fn observedAddr(self: *const IdentifyResult) []const u8 {
        return self.reader.getObservedAddr();
    }
};

// --- Tests ---

const MockStream = stream_util.MockStream;

test "handleInbound encodes and writes identify message" {
    const allocator = std.testing.allocator;

    var stream = MockStream.init(allocator, &.{});
    defer stream.deinit();

    try handleInbound(allocator, &stream, .{
        .protocol_version = "test/1.0.0",
        .agent_version = "zig-libp2p/0.1.0",
    });

    // Decode what was written
    var reader = try identify_pb.IdentifyReader.init(stream.write_buf.items);
    try std.testing.expectEqualStrings("test/1.0.0", reader.getProtocolVersion());
    try std.testing.expectEqualStrings("zig-libp2p/0.1.0", reader.getAgentVersion());
}

test "handleOutbound reads and decodes identify message" {
    const allocator = std.testing.allocator;

    var msg = identify_pb.Identify{
        .protocol_version = "ipfs/0.1.0",
        .agent_version = "go-libp2p/0.35.0",
        .public_key = "test-key",
    };
    const encoded = try msg.encode(allocator);
    defer allocator.free(encoded);

    var stream = MockStream.init(allocator, encoded);
    defer stream.deinit();

    var result = try handleOutbound(allocator, &stream);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("ipfs/0.1.0", result.protocolVersion());
    try std.testing.expectEqualStrings("go-libp2p/0.35.0", result.agentVersion());
    try std.testing.expectEqualStrings("test-key", result.publicKey());
}

test "handleOutbound rejects empty stream" {
    const allocator = std.testing.allocator;

    var stream = MockStream.init(allocator, &.{});
    defer stream.deinit();

    const result = handleOutbound(allocator, &stream);
    try std.testing.expectError(Error.UnexpectedEof, result);
}
