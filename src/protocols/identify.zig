const std = @import("std");
const identify_pb = @import("../protobuf.zig").identify;
const Stream = @import("../quic.zig").Stream;

pub const protocol_id = "/ipfs/id/1.0.0";
const max_identify_message_len = 1024 * 1024;
const max_varint_len = 10;

pub const Options = struct {
    protocol_version: []const u8 = "ipfs/0.1.0",
    agent_version: []const u8 = "eth-p2p-z/0.1.0",
    public_key: ?[]const u8 = null,
    listen_addrs: []const []const u8 = &.{},
    observed_addr: ?[]const u8 = null,
    protocols: []const []const u8 = &.{},
};

pub const IdentifyHandler = struct {
    allocator: std.mem.Allocator,
    options: Options = .{},

    pub fn init(allocator: std.mem.Allocator) IdentifyHandler {
        return .{ .allocator = allocator };
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, options: Options) IdentifyHandler {
        return .{ .allocator = allocator, .options = options };
    }

    pub fn run(self: *IdentifyHandler, io: std.Io, stream: *Stream) !void {
        try writeIdentify(self.allocator, io, stream, self.options);
        try stream.closeWrite(io);
    }
};

pub const OwnedIdentify = struct {
    bytes: []u8,
    reader: identify_pb.IdentifyReader,

    pub fn deinit(self: *OwnedIdentify, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub fn writeIdentify(allocator: std.mem.Allocator, io: std.Io, stream: *Stream, options: Options) !void {
    const listen_addrs = try nullableSliceList(allocator, options.listen_addrs);
    defer allocator.free(listen_addrs);
    const protocols = try nullableSliceList(allocator, options.protocols);
    defer allocator.free(protocols);

    const msg = identify_pb.Identify{
        .protocol_version = options.protocol_version,
        .agent_version = options.agent_version,
        .public_key = options.public_key,
        .listen_addrs = if (listen_addrs.len == 0) null else listen_addrs,
        .observed_addr = options.observed_addr,
        .protocols = if (protocols.len == 0) null else protocols,
    };
    const payload = try msg.encode(allocator);
    defer allocator.free(payload);

    var len_buf: [max_varint_len]u8 = undefined;
    const len_len = encodeUvarint(&len_buf, payload.len);
    try stream.writeAll(io, len_buf[0..len_len], .{});
    try stream.writeAll(io, payload, .{});
}

pub fn readIdentify(allocator: std.mem.Allocator, io: std.Io, stream: *Stream) !OwnedIdentify {
    const len = try readUvarint(io, stream);
    if (len > max_identify_message_len) return error.MessageTooLarge;

    const bytes = try allocator.alloc(u8, len);
    errdefer allocator.free(bytes);
    try stream.readAll(io, bytes, .{});

    return .{
        .bytes = bytes,
        .reader = try identify_pb.IdentifyReader.init(bytes),
    };
}

fn nullableSliceList(allocator: std.mem.Allocator, values: []const []const u8) std.mem.Allocator.Error![]const ?[]const u8 {
    if (values.len == 0) return &.{};
    const out = try allocator.alloc(?[]const u8, values.len);
    for (values, 0..) |value, i| out[i] = value;
    return out;
}

fn encodeUvarint(out: *[max_varint_len]u8, value: usize) usize {
    var remaining = value;
    var i: usize = 0;
    while (remaining >= 0x80) {
        out[i] = @as(u8, @intCast(remaining & 0x7f)) | 0x80;
        remaining >>= 7;
        i += 1;
    }
    out[i] = @intCast(remaining);
    return i + 1;
}

fn readUvarint(io: std.Io, stream: *Stream) !usize {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;
    while (i < max_varint_len) : (i += 1) {
        var byte: [1]u8 = undefined;
        try stream.readAll(io, &byte, .{});
        result |= (@as(usize, byte[0] & 0x7f) << @intCast(shift));
        if ((byte[0] & 0x80) == 0) return result;
        shift += 7;
    }
    return error.VarintTooLong;
}
