const std = @import("std");
const AtomicRc = @import("ref_count").AtomicRc;
const endpoint_core = @import("../endpoint/core.zig");
const route_table_mod = @import("../router/route_table.zig");
const socket_control = @import("socket_control.zig");

/// Refcounted UDP socket resource shared between the endpoint's router fiber
/// and every QUIC connection that uses it.
pub const SharedUdpSocket = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: std.Io.net.Socket,
    caps: socket_control.Capabilities,
    rc: AtomicRc = .{},

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: std.Io.net.Socket,
        caps: socket_control.Capabilities,
    ) std.mem.Allocator.Error!*SharedUdpSocket {
        const shared = try allocator.create(SharedUdpSocket);
        shared.* = .{
            .allocator = allocator,
            .io = io,
            .socket = socket,
            .caps = caps,
        };
        return shared;
    }

    pub fn retain(shared: *SharedUdpSocket) void {
        shared.rc.retainChecked();
    }

    pub fn release(shared: *SharedUdpSocket) void {
        if (!shared.rc.releaseChecked()) return;

        const allocator = shared.allocator;
        shared.socket.close(shared.io);
        allocator.destroy(shared);
    }

    pub fn address(shared: *const SharedUdpSocket) std.Io.net.IpAddress {
        return shared.socket.address;
    }

    pub fn sourceControlEnabled(shared: *const SharedUdpSocket, from: std.Io.net.IpAddress) bool {
        return socket_control.sourceControlEnabled(shared.caps, from);
    }

    pub fn receive(shared: *const SharedUdpSocket, io: std.Io, buf: []u8) std.Io.net.Socket.ReceiveError!std.Io.net.IncomingMessage {
        return shared.socket.receive(io, buf);
    }

    pub fn receiveWithControlTimeout(
        shared: *const SharedUdpSocket,
        io: std.Io,
        buf: []u8,
        control: []u8,
        timeout: std.Io.Timeout,
    ) std.Io.net.Socket.ReceiveTimeoutError!std.Io.net.IncomingMessage {
        return socket_control.receiveOneTimeout(&shared.socket, io, buf, control, timeout);
    }

    pub fn sendMany(shared: *const SharedUdpSocket, io: std.Io, messages: []std.Io.net.OutgoingMessage, flags: std.Io.net.SendFlags) std.Io.net.Socket.SendError!void {
        return shared.socket.sendMany(io, messages, flags);
    }
};

pub const NetworkTransport = struct {
    io: std.Io,
    /// Retained/released via retainResources()/deinit() (refcounted).
    socket: *SharedUdpSocket,
    local: std.Io.net.IpAddress,
    peer: std.Io.net.IpAddress,
    /// Retained/released via retainResources()/deinit() (refcounted).
    core: *endpoint_core.EndpointCore,
    /// BORROWED pointer into `core` (lifetime tied to core, which this
    /// transport retains); NOT released here. The actor writes its CID
    /// routes directly into this table — see route_table.zig.
    route_table: *route_table_mod.RouteTable,
    outbound_batch_size: usize = 32,

    pub fn retainResources(t: *const NetworkTransport) void {
        t.socket.retain();
        t.core.retain();
    }

    pub fn deinit(t: *NetworkTransport) void {
        t.socket.release();
        t.core.release();
    }

    pub fn addStat(t: *const NetworkTransport, comptime field: []const u8, value: u64) void {
        t.core.addStat(field, value);
    }

    pub fn subStat(t: *const NetworkTransport, comptime field: []const u8, value: u64) void {
        t.core.subStat(field, value);
    }
};

pub const NetworkError = error{
    AddressInvalid,
    QuicheSendFailed,
    QuicheRecvFailed,
} || std.Io.Cancelable || std.Io.ConcurrentError || std.Io.net.Socket.SendError || std.Io.net.Socket.ReceiveTimeoutError;

test "network transport retains and releases its shared resources" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const core = try endpoint_core.EndpointCore.init(allocator, io);
    defer core.release();

    var addr = std.Io.net.IpAddress{ .ip4 = .loopback(0) };
    const socket = try std.Io.net.IpAddress.bind(&addr, io, .{ .mode = .dgram });
    const shared = try SharedUdpSocket.init(allocator, io, socket, .{});

    var transport = NetworkTransport{
        .io = io,
        .socket = shared,
        .local = socket.address,
        .peer = socket.address,
        .core = core,
        .route_table = &core.route_table,
    };
    transport.retainResources();
    transport.deinit();
    try std.testing.expectEqual(@as(usize, 1), shared.rc.count());
    shared.release();
}
