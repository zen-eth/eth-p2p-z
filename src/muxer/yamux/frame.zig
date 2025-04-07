const std = @import("std");

/// Protocol version - currently only version 0 is supported
pub const PROTOCOL_VERSION: u8 = 0;

/// Frame types as defined in the Yamux protocol
pub const FrameType = enum(u8) {
    /// Data frame - carries stream data
    DATA = 0,
    /// WindowUpdate frame - updates flow control window
    WINDOW_UPDATE = 1,
    /// Ping frame - used for keepalives and RTT measurement
    PING = 2,
    /// GoAway frame - used to terminate a session
    GO_AWAY = 3,
};

/// Frame flags for stream control
pub const FrameFlags = struct {
    /// SYN is sent to signal a new stream
    pub const SYN: u16 = 1 << 0;
    /// ACK is sent to acknowledge a new stream
    pub const ACK: u16 = 1 << 1;
    /// FIN is sent to half-close the given stream
    pub const FIN: u16 = 1 << 2;
    /// RST is used to hard close a given stream
    pub const RST: u16 = 1 << 3;
};

/// GoAway error codes
pub const GoAwayCode = enum(u32) {
    /// Normal termination
    NORMAL = 0,
    /// Protocol error
    PROTOCOL_ERROR = 1,
    /// Internal error
    INTERNAL_ERROR = 2,
};

/// Frame header structure
pub const Header = struct {
    /// Protocol version size
    pub const VERSION_SIZE: usize = 1;
    /// Frame type size
    pub const TYPE_SIZE: usize = 1;
    /// Flags size
    pub const FLAGS_SIZE: usize = 2;
    /// Stream ID size
    pub const STREAM_ID_SIZE: usize = 4;
    /// Length size
    pub const LENGTH_SIZE: usize = 4;
    /// The header is 12 bytes in total
    pub const SIZE: usize = VERSION_SIZE + TYPE_SIZE + FLAGS_SIZE + STREAM_ID_SIZE + LENGTH_SIZE;

    /// Protocol version
    version: u8 = PROTOCOL_VERSION,
    /// Type of frame
    frame_type: FrameType,
    /// Control flags
    flags: u16,
    /// Stream identifier
    stream_id: u32,
    /// Length of the frame payload
    length: u32,

    /// Create a new header with specified parameters
    pub fn init(frame_type: FrameType, flags: u16, stream_id: u32, length: u32) Header {
        return .{
            .frame_type = frame_type,
            .flags = flags,
            .stream_id = stream_id,
            .length = length,
        };
    }

    /// Encode header to wire format
    pub fn encode(self: Header, buffer: []u8) !void {
        if (buffer.len < SIZE) return Error.BufferTooSmall;

        buffer[0] = self.version;
        buffer[1] = @intFromEnum(self.frame_type);
        std.mem.writeInt(u16, buffer[2..4], self.flags, .big);
        std.mem.writeInt(u32, buffer[4..8], self.stream_id, .big);
        std.mem.writeInt(u32, buffer[8..12], self.length, .big);
    }

    /// Decode header from wire format
    pub fn decode(buffer: []const u8) !Header {
        if (buffer.len < SIZE) return Error.BufferTooSmall;

        if (buffer[0] != PROTOCOL_VERSION) {
            return Error.InvalidProtocolVersion;
        }

        return .{
            .version = buffer[0],
            .frame_type = @enumFromInt(buffer[1]),
            .flags = std.mem.readInt(u16, buffer[2..4], .big),
            .stream_id = std.mem.readInt(u32, buffer[4..8], .big),
            .length = std.mem.readInt(u32, buffer[8..12], .big),
        };
    }

    /// Get a string representation of the header (useful for debugging)
    pub fn format(
        self: Header,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(
            writer,
            "Vsn:{d} Type:{d} Flags:{d} StreamID:{d} Length:{d}",
            .{
                self.version,
                @intFromEnum(self.frame_type),
                self.flags,
                self.stream_id,
                self.length,
            },
        );
    }
};

/// Errors that can occur during frame operations
pub const Error = error{
    BufferTooSmall,
    InvalidProtocolVersion,
    InvalidFrameType,
};

test "Frame header encode/decode" {
    const testing = std.testing;

    const header = Header.init(.DATA, FrameFlags.SYN, 42, 100);

    var buffer: [Header.SIZE]u8 = undefined;
    try header.encode(&buffer);

    const decoded = try Header.decode(&buffer);

    try testing.expectEqual(header.version, decoded.version);
    try testing.expectEqual(header.frame_type, decoded.frame_type);
    try testing.expectEqual(header.flags, decoded.flags);
    try testing.expectEqual(header.stream_id, decoded.stream_id);
    try testing.expectEqual(header.length, decoded.length);
}

test "Frame flags" {
    const testing = std.testing;

    // Create a header with multiple flags
    const header = Header.init(.DATA, FrameFlags.SYN | FrameFlags.ACK, 1, 0);

    try testing.expectEqual(@as(u16, 3), header.flags); // SYN | ACK = 1 | 2 = 3
    try testing.expect((header.flags & FrameFlags.SYN) != 0);
    try testing.expect((header.flags & FrameFlags.ACK) != 0);
    try testing.expect((header.flags & FrameFlags.FIN) == 0);
    try testing.expect((header.flags & FrameFlags.RST) == 0);
}
