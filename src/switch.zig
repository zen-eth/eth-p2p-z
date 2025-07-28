const std = @import("std");
const quic = @import("./transport/quic/root.zig").lsquic_transport;
const proto_handler = @import("./proto_handler.zig");
const Allocator = std.mem.Allocator;

pub const Switch = struct {
    proto_handlers: std.ArrayList(proto_handler.AnyProtocolHandler),

    // Only one transport is supported at a time.
    transport: *quic.QuicTransport,

    // TODO: Once peerid is implemented, we can use it to identify connections.
    // For now, we use the peer address as the key.
    connections: std.StringArrayHashMap(*quic.QuicConnection),

    allocator: Allocator,

    const ConnectCallbackCtx = struct {
        @"switch": *Switch,
        protocols: []const []const u8,
        // user-defined context for the callback
        callback_ctx: ?*anyopaque,
        // user-defined callback function
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void,

        fn connectCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const conn = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            conn.newStream(self, newStreamCallback);
        }

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const stream = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            // TODO: To use multistreams, we need to find the protocol handler for the stream.
            // For now, we just use the first protocol handler.
            self.@"switch".proto_handlers.items[0].onInitiatorStart(stream, self.callback_ctx, self.callback);

            // Here we would typically activate the protocol handler for the stream.
            // For now, we just log the new stream.
            std.debug.print("New stream established: {any}\n", .{stream});
            self.@"switch".allocator.destroy(self);
        }
    };

    pub fn newStream(
        self: *Switch,
        address: std.net.Address,
        protocols: []const []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void,
    ) void {
        const connect_ctx = self.transport.allocator.create(ConnectCallbackCtx) catch unreachable;
        connect_ctx.* = ConnectCallbackCtx{
            .@"switch" = self,
            .protocols = protocols,
            .callback_ctx = callback_ctx,
            .callback = callback,
        };

        const address_str = std.fmt.allocPrint(self.allocator, "{}", .{address}) catch |err| {
            callback(callback_ctx, err);
            self.allocator.destroy(connect_ctx);
            return;
        };
        defer self.allocator.free(address_str);

        // Check if the connection already exists.
        if (self.connections.get(address_str)) |conn| {
            // If the connection already exists, we can just create a new stream on it.
            conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);
        } else {
            // Dial the peer and pass the connect context as the callback context.
            self.transport.dial(address, connect_ctx, ConnectCallbackCtx.connectCallback);
        }
    }
};
