const std = @import("std");
const uv = @import("libuv");
const io = @import("io.zig");
const transport = @import("transport.zig");
const MultiAddr = @import("multiaddr").MultiAddr;
const PeerId = @import("peer").PeerId;

pub const TcpConnection = struct {
    stream: io.IoStream,
    read_buffer: []u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, stream: io.IoStream) !*TcpConnection {
        const conn = try allocator.create(TcpConnection);
        conn.* = .{
            .stream = stream,
            .read_buffer = try allocator.alloc(u8, 4096),
            .allocator = allocator,
        };
        return conn;
    }

    pub fn deinit(self: *TcpConnection) void {
        self.allocator.free(self.read_buffer);
        self.allocator.destroy(self);
    }

    pub fn startRead(self: *TcpConnection, callback: ReadCallback) !void {
        while (true) {
            const bytes_read = try self.stream.read(self.read_buffer);
            if (bytes_read == 0) break;
            callback(&self.stream, self.read_buffer[0..bytes_read]);
        }
    }
};

pub const TcpTransport = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TcpTransport {
        return .{
            .allocator = allocator,
        };
    }

    pub fn dial(
        self: *TcpTransport,
        loop: io.IoLoop,
        remote_addr: MultiAddr,
        peer_id: PeerId,
        timeout_ms: ?u64,
    ) TransportError!CapableConn {
        var stream = try io.IoStream.init(loop, .tcp);
        try stream.connect(remote_addr);

        if (timeout_ms) |timeout| {
            try stream.setTimeout(timeout);
        }

        return CapableConn{
            .stream = stream,
            .transport = .{ .tcp = self.* },
            .peer_id = peer_id,
            .local_addr = stream.getLocalAddr(),
            .remote_addr = remote_addr,
        };
    }

    pub fn listen(
        self: *TcpTransport,
        loop: io.IoLoop,
        local_addr: MultiAddr,
    ) TransportError!Listener {
        var stream = try io.IoStream.init(loop, .tcp);
        try stream.bind(local_addr);
        try stream.listen(128);

        return Listener{
            .stream = stream,
            .addr = local_addr,
            .transport = .{ .tcp = self.* },
        };
    }

    pub fn canDial(self: *Self, addr: MultiAddr) bool {
        _ = self;
        // Check if the multiaddr contains TCP protocol
        return addr.hasProtocol("tcp");
    }

    pub fn protocols(self: *Self) []i32 {
        _ = self;
        return &[_]i32{6}; // TCP protocol number
    }

    pub fn isProxy(self: *Self) bool {
        _ = self;
        return false;
    }

    fn handleNewConnection(self: *Self, stream: io.IoStream) !void {
        var conn = try TcpConnection.init(self.allocator, stream);
        try conn.startRead(self.handleRead);
    }
};
