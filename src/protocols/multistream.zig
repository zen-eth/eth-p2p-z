const std = @import("std");
const Stream = @import("../quic.zig").Stream;

pub const protocol_id = "/multistream/1.0.0";
const na = "na";
const newline = '\n';
const max_message_len = 1024;
const max_varint_len = 9;

pub const Role = enum { initiator, responder };
pub const Error = error{
    NoSupportedProtocol,
    ProtocolRejected,
    InvalidMultistreamHeader,
    InvalidMessage,
    MessageTooLarge,
    VarintTooLong,
} || Stream.ReadError || Stream.WriteError;
pub const Options = struct {
    role: Role = .responder,
    timeout: std.Io.Timeout = .none,
};

pub fn negotiate(io: std.Io, stream: *Stream, supported: []const []const u8, opts: Options) Error![]const u8 {
    if (supported.len == 0) return error.NoSupportedProtocol;
    return switch (opts.role) {
        .initiator => negotiateInitiator(io, stream, supported, opts.timeout),
        .responder => negotiateResponder(io, stream, supported, opts.timeout),
    };
}

fn negotiateInitiator(io: std.Io, stream: *Stream, supported: []const []const u8, timeout: std.Io.Timeout) Error![]const u8 {
    try writeMessage(io, stream, protocol_id, timeout);

    var read_buf: [max_message_len]u8 = undefined;
    const header = try readMessage(io, stream, &read_buf, timeout);
    if (!std.mem.eql(u8, header, protocol_id)) return error.InvalidMultistreamHeader;

    for (supported) |candidate| {
        try writeMessage(io, stream, candidate, timeout);
        const response = try readMessage(io, stream, &read_buf, timeout);
        if (std.mem.eql(u8, response, candidate)) return candidate;
        if (std.mem.eql(u8, response, na)) continue;
        return error.ProtocolRejected;
    }

    return error.NoSupportedProtocol;
}

fn negotiateResponder(io: std.Io, stream: *Stream, supported: []const []const u8, timeout: std.Io.Timeout) Error![]const u8 {
    var read_buf: [max_message_len]u8 = undefined;
    const header = try readMessage(io, stream, &read_buf, timeout);
    if (!std.mem.eql(u8, header, protocol_id)) return error.InvalidMultistreamHeader;
    try writeMessage(io, stream, protocol_id, timeout);

    while (true) {
        const proposed = try readMessage(io, stream, &read_buf, timeout);
        for (supported) |candidate| {
            if (std.mem.eql(u8, proposed, candidate)) {
                try writeMessage(io, stream, candidate, timeout);
                return candidate;
            }
        }
        try writeMessage(io, stream, na, timeout);
    }
}

fn writeMessage(io: std.Io, stream: *Stream, message: []const u8, timeout: std.Io.Timeout) Error!void {
    if (message.len + 1 > max_message_len) return error.MessageTooLarge;

    var len_buf: [max_varint_len]u8 = undefined;
    const len_len = encodeUvarint(&len_buf, message.len + 1);
    try stream.writeAll(io, len_buf[0..len_len], .{ .timeout = timeout });
    try stream.writeAll(io, message, .{ .timeout = timeout });
    try stream.writeAll(io, &.{newline}, .{ .timeout = timeout });
}

fn readMessage(io: std.Io, stream: *Stream, out: *[max_message_len]u8, timeout: std.Io.Timeout) Error![]const u8 {
    const len = try readUvarint(io, stream, timeout);
    if (len == 0) return error.InvalidMessage;
    if (len > max_message_len) return error.MessageTooLarge;
    try stream.readAll(io, out[0..len], .{ .timeout = timeout });
    if (out[len - 1] != newline) return error.InvalidMessage;
    return out[0 .. len - 1];
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

fn readUvarint(io: std.Io, stream: *Stream, timeout: std.Io.Timeout) Error!usize {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;
    while (i < max_varint_len) : (i += 1) {
        var byte: [1]u8 = undefined;
        try stream.readAll(io, &byte, .{ .timeout = timeout });
        result |= (@as(usize, byte[0] & 0x7f) << @intCast(shift));
        if ((byte[0] & 0x80) == 0) return result;
        shift += 7;
    }
    return error.VarintTooLong;
}
