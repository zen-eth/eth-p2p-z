const std = @import("std");
const xev = @import("../libxev.zig");
const XevSocketChannel = xev.SocketChannel;
const XevTransport = xev.XevTransport;
const Allocator = std.mem.Allocator;
const Future = @import("../../utils/future.zig").Future;

pub const Connection = union(enum) {
    xev_channel: *XevSocketChannel,
};

pub const Transport = union(enum) {
    xev_transport: XevTransport,

    pub fn listen(self: *Transport, addr: std.net.Address) !void {
        return switch (self.*) {
            .xev_transport => |*transport| transport.listen(addr),
        };
    }

    pub fn dial(self: *Transport, addr: std.net.Address, connection_future: *Future(*Connection), allocator: Allocator) !void {
        return switch (self.*) {
            .xev_transport => |*transport| {
                const channel_future = try allocator.create(Future(*XevSocketChannel));
                channel_future.* = Future(*XevSocketChannel).init();
                try transport.dial(addr, channel_future);

                const Context = struct {
                    pub fn on_success(socket_channel: *XevSocketChannel) void {
                        const connection = try allocator.create(Connection);
                        connection.* = Connection{ .xev_channel = socket_channel };
                        connection_future.complete(Connection{ .xev_channel = socket_channel });
                    }

                    pub fn on_failure(err: anyerror) void {
                        connection_future.completeError(err);
                    }
                };

                channel_future.listen();
            },
        };
    }
};
