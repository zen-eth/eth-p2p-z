const std = @import("std");
const transport = @import("../transport/transport.zig");
const Transport = transport.Transport;
const libuv = @import("../transport/libuv.zig");
const ConnectionOverLibuv = libuv.Connection;
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;

pub const Connection = union(enum) {
    libuv: ConnectionOverLibuv,

    pub fn on_close(self: *Connection, cb: ?fn (*Connection) void) void {
        self.libuv.on_close = cb;
    }
};

pub const ConnectionHandler = struct {
    pub fn handle_connection(conn: *Connection) void {
        std.debug.print("Connection received\n", .{conn.*});
    }
};

pub const Network = struct {
    transports: std.ArrayList(Transport),
    connection_handler: ConnectionHandler,
    connections: std.ArrayList(Connection),
    mutex: std.Thread.Mutex = .{},
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, transports: std.ArrayList(Transport), connection_handler: ConnectionHandler) !Network {
        const network = Network{
            .transports = transports,
            .connection_handler = connection_handler,
            .connections = std.ArrayList(Connection).init(allocator),
            .allocator = allocator,
            .mutex = .{},
        };
        return network;
    }

    pub fn listen(self: *Network, addr: *const Multiaddr) !void {
        const t = try self.get_transport(addr);
        try t.listen(addr);
    }

    fn get_transport(self: *Network, _: *const Multiaddr) !Transport {
        for (self.transports.items) |t| {
            return t;
        }
        return error.TransportNotSupported;
    }

    fn handle_connection(self: *Network, conn: *Connection) void {
        self.mutex.lock();
        self.connections.append(conn);

        self.mutex.unlock();
    }
};
