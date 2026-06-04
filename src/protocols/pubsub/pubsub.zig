const std = @import("std");
const PeerId = @import("peer_id").PeerId;
const rpc_pb = @import("../../protobuf.zig").rpc;
const Stream = @import("../../quic.zig").Stream;

pub const rpc = @import("rpc.zig");

pub const protocol_id = "/meshsub/1.1.0";
pub const protocol_id_v1_0 = "/meshsub/1.0.0";
const max_rpc_message_len = 1024 * 1024;
const max_varint_len = 10;

pub const PubSubMessage = rpc_pb.Message;
pub const PublishResult = []PeerId;

pub const OwnedRpc = struct {
    bytes: []u8,
    reader: rpc_pb.RPCReader,

    pub fn deinit(self: *OwnedRpc, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const Gossipsub = struct {
    allocator: std.mem.Allocator,
    inbox: *std.Io.Queue(OwnedRpc),

    pub fn init(allocator: std.mem.Allocator, inbox: *std.Io.Queue(OwnedRpc)) Gossipsub {
        return .{ .allocator = allocator, .inbox = inbox };
    }

    pub fn run(self: *Gossipsub, io: std.Io, stream: *Stream) !void {
        while (true) {
            var msg = readRpc(self.allocator, io, stream) catch |err| switch (err) {
                error.EndOfStream, error.StreamShutdown, error.ConnectionClosed => return,
                else => |e| return e,
            };
            errdefer msg.deinit(self.allocator);
            try self.inbox.putOne(io, msg);
        }
    }
};

pub fn writePublish(allocator: std.mem.Allocator, io: std.Io, stream: *Stream, message: rpc_pb.Message) !void {
    const publish = [_]?rpc_pb.Message{message};
    const frame = rpc_pb.RPC{ .publish = &publish };
    try writeRpc(allocator, io, stream, frame);
}

pub fn writeRpc(allocator: std.mem.Allocator, io: std.Io, stream: *Stream, frame: rpc_pb.RPC) !void {
    const payload = try frame.encode(allocator);
    defer if (payload.len > 0) allocator.free(payload);

    var len_buf: [max_varint_len]u8 = undefined;
    const len_len = encodeUvarint(&len_buf, payload.len);
    try stream.writeAll(io, len_buf[0..len_len], .{});
    try stream.writeAll(io, payload, .{});
}

pub fn readRpc(allocator: std.mem.Allocator, io: std.Io, stream: *Stream) !OwnedRpc {
    const len = try readUvarint(io, stream);
    if (len > max_rpc_message_len) return error.MessageTooLarge;

    const bytes = try allocator.alloc(u8, len);
    errdefer allocator.free(bytes);
    try stream.readAll(io, bytes, .{});

    return .{
        .bytes = bytes,
        .reader = try rpc_pb.RPCReader.init(bytes),
    };
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

// Aggregate test blocks for all files in this subdirectory so that their
// test {} blocks are reachable from the root test aggregator. Add each new
// pubsub source file here as `_ = @import("filename.zig");`.
test {
    _ = @import("rpc.zig");
}
