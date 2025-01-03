const std = @import("std");
const transport = @import("../transport/transport.zig");
const Transport = transport.Transport;
const libuv = @import("../transport/libuv.zig");
const ConnectionOverLibuv = libuv.Connection;

pub const Connection = union(enum) {
    libuv: ConnectionOverLibuv,
};

pub const ConnectionHandler = struct {
    pub fn handle_connection(conn: *Connection) void {
        std.debug.print("Connection received\n", .{conn.*});
    }
};

pub const Network = struct {
    transports: std.ArrayList(Transport),
};
