const std = @import("std");
const io = @import("io.zig");
const MultiAddr = @import("multiaddr").MultiAddr;
const PeerId = @import("peer").PeerId;

pub const TransportError = error{
    DialFailed,
    ListenerClosed,
    BindFailed,
    ConnectionFailed,
};

pub const CapableConn = struct {
    stream: io.IoStream,
    transport: *Transport,
    peer_id: PeerId,
    local_addr: MultiAddr,
    remote_addr: MultiAddr,

    pub fn close(self: *CapableConn) void {
        self.stream.close();
    }

    pub fn read(self: *CapableConn, buffer: []u8) !usize {
        return self.stream.read(buffer);
    }

    pub fn write(self: *CapableConn, data: []const u8) !void {
        try self.stream.write(data);
    }
};

pub const Listener = struct {
    loop: io.IoLoop,
    stream: io.IoStream,
    addr: MultiAddr,

    pub fn accept(self: *Listener) !CapableConn {
        _ = self;
        @panic("Not implemented");
    }

    pub fn close(self: *Listener) void {
        self.stream.close();
    }

    pub fn getMultiaddr(self: Listener) MultiAddr {
        return self.addr;
    }
};
pub const TransportType = enum {
    tcp,
    quic,
    udp,
};

pub const Transport = union(TransportType) {
    tcp: TcpTransport,
    quic: QuicTransport,
    udp: UdpTransport,

    pub fn dial(
        self: *Transport,
        loop: io.IoLoop,
        remote_addr: MultiAddr,
        peer_id: PeerId,
        timeout_ms: ?u64,
    ) TransportError!CapableConn {
        return switch (self.*) {
            .tcp => |*t| t.dial(loop, remote_addr, peer_id, timeout_ms),
            .quic => |*t| t.dial(loop, remote_addr, peer_id, timeout_ms),
            .udp => |*t| t.dial(loop, remote_addr, peer_id, timeout_ms),
        };
    }

    pub fn listen(
        self: *Transport,
        loop: io.IoLoop,
        local_addr: MultiAddr,
    ) TransportError!Listener {
        return switch (self.*) {
            .tcp => |*t| t.listen(loop, local_addr),
            .quic => |*t| t.listen(loop, local_addr),
            .udp => |*t| t.listen(loop, local_addr),
        };
    }
};
