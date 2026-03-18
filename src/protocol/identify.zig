const std = @import("std");
const Io = std.Io;
const identify_pb = @import("../proto/identify.proto.zig");
const stream_util = @import("../util/stream.zig");

const writeAll = stream_util.writeAll;

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

/// Identify protocol handler.
/// Identify is stateful — stores per-peer results.
/// Follows go-libp2p pattern: auto-triggered on new connections by the Switch.
pub const Handler = struct {
    allocator: std.mem.Allocator,
    config: Config,
    /// Per-peer identify results. Keys are owned copies of peer_id bytes.
    peer_results: std.StringHashMap(IdentifyResult),

    /// Protocol identifier for libp2p identify.
    pub const id = "/ipfs/id/1.0.0";

    /// Protocol identifier for libp2p identify push.
    pub const push_id = "/ipfs/id/push/1.0.0";

    /// Clean up all stored peer results.
    pub fn deinit(self: *Handler) void {
        var it = self.peer_results.iterator();
        while (it.next()) |entry| {
            var result = entry.value_ptr.*;
            result.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.peer_results.deinit();
    }

    /// Called by Switch when a peer disconnects. Frees stored identify result.
    pub fn onPeerDisconnected(self: *Handler, peer_id: []const u8) void {
        if (self.peer_results.fetchRemove(peer_id)) |kv| {
            var result = kv.value;
            result.deinit(self.allocator);
            self.allocator.free(kv.key);
        }
    }

    /// Get the stored identify result for a peer, if available.
    pub fn getPeerResult(self: *const Handler, peer_id: []const u8) ?*const IdentifyResult {
        return self.peer_results.getPtr(peer_id);
    }

    /// Handle inbound identify (responder): encode and send our identity.
    pub fn handleInbound(self: *Handler, io: Io, stream: anytype, _: anytype) Error!void {
        const allocator = self.allocator;
        const config = self.config;

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

        writeAll(io, stream, encoded) catch return Error.UnexpectedEof;
    }

    /// Handle outbound identify (initiator): read the remote's identity.
    /// Stores result in peer_results if ctx.peer_id is provided (via Switch.newStream).
    pub fn handleOutbound(self: *Handler, io: Io, stream: anytype, ctx: anytype) Error!void {
        const allocator = self.allocator;

        var buf = std.ArrayList(u8).empty;
        errdefer buf.deinit(allocator);

        var tmp: [4096]u8 = undefined;
        while (buf.items.len < max_message_size) {
            const n = stream.read(io, &tmp) catch return Error.UnexpectedEof;
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

        var result: IdentifyResult = .{
            .reader = reader,
            .raw_bytes = owned,
        };

        // Store per-peer result if peer_id is available
        const peer_id: ?[]const u8 = if (@hasField(@TypeOf(ctx), "peer_id")) ctx.peer_id else null;
        if (peer_id) |pid| {
            // Remove old result if any
            if (self.peer_results.fetchRemove(pid)) |kv| {
                var old = kv.value;
                old.deinit(allocator);
                allocator.free(kv.key);
            }
            const key = allocator.dupe(u8, pid) catch {
                result.deinit(allocator);
                return Error.UnexpectedEof;
            };
            self.peer_results.put(key, result) catch {
                allocator.free(key);
                result.deinit(allocator);
                return Error.UnexpectedEof;
            };
        } else {
            // No peer_id context, just discard
            result.deinit(allocator);
        }
    }
};

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

    var handler: Handler = .{
        .allocator = allocator,
        .config = .{
            .protocol_version = "test/1.0.0",
            .agent_version = "zig-libp2p/0.1.0",
        },
        .peer_results = std.StringHashMap(IdentifyResult).init(allocator),
    };
    defer handler.deinit();
    try handler.handleInbound(undefined, &stream, .{});

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

    var handler: Handler = .{
        .allocator = allocator,
        .config = .{},
        .peer_results = std.StringHashMap(IdentifyResult).init(allocator),
    };
    defer handler.deinit();

    const peer_id = "test-peer-id";
    try handler.handleOutbound(undefined, &stream, .{ .peer_id = @as(?[]const u8, peer_id) });

    // Result should be stored
    const result = handler.getPeerResult(peer_id) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqualStrings("ipfs/0.1.0", result.protocolVersion());
    try std.testing.expectEqualStrings("go-libp2p/0.35.0", result.agentVersion());
    try std.testing.expectEqualStrings("test-key", result.publicKey());
}

test "handleOutbound rejects empty stream" {
    const allocator = std.testing.allocator;

    var stream = MockStream.init(allocator, &.{});
    defer stream.deinit();

    var handler: Handler = .{
        .allocator = allocator,
        .config = .{},
        .peer_results = std.StringHashMap(IdentifyResult).init(allocator),
    };
    defer handler.deinit();
    const result = handler.handleOutbound(undefined, &stream, .{});
    try std.testing.expectError(Error.UnexpectedEof, result);
}
