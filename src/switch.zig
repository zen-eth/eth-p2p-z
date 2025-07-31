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

    listeners: std.StringArrayHashMap(quic.QuicListener),

    pub fn init(self: *Switch, allocator: Allocator, transport: *quic.QuicTransport) void {
        self.* = Switch{
            .proto_handlers = std.ArrayList(proto_handler.AnyProtocolHandler).init(allocator),
            .transport = transport,
            .connections = std.StringArrayHashMap(*quic.QuicConnection).init(allocator),
            .allocator = allocator,
            .listeners = std.StringArrayHashMap(quic.QuicListener).init(allocator),
        };
    }

    pub fn deinit(self: *Switch) void {
        // TODO: Properly close all connections and listeners.
        self.proto_handlers.deinit();
        self.connections.deinit();

        var iter = self.listeners.iterator();
        while (iter.next()) |entry| {
            const listener = entry.value_ptr;
            if (listener.accept_callback_ctx) |ctx| {
                const value: *Switch.AcceptCallbackCtx = @ptrCast(@alignCast(ctx));
                self.allocator.destroy(value);
            }

            self.allocator.free(entry.key_ptr.*);
        }
        self.listeners.deinit();
    }

    const AcceptCallbackCtx = struct {
        @"switch": *Switch,
        // user-defined context for the callback
        callback_ctx: ?*anyopaque,
        // user-defined callback function
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        fn acceptCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
            const self: *AcceptCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const conn = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            conn.onStream(self, newStreamCallback);
        }

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            const self: *AcceptCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const stream = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            // TODO: To use multistreams, we need to find the protocol handler for the stream.
            // For now, we just use the first protocol handler.
            self.@"switch".proto_handlers.items[0].onResponderStart(stream, self.callback_ctx, self.callback);

            // `onResponderStart` should set the stream's protocol message handler.
            stream.proto_msg_handler.onActivated(stream) catch |err| {
                std.log.warn("Proto message handler failed with error: {any}. Closing stream {any}.", .{ err, stream });
                self.callback(self.callback_ctx, err);
                // TODO: Close the stream properly.
                return;
            };
            // Here we would typically activate the protocol handler for the stream.
            // For now, we just log the new stream.
            std.debug.print("New stream established: {any}\n", .{stream});
            self.@"switch".allocator.destroy(self);
        }
    };

    const ConnectCallbackCtx = struct {
        @"switch": *Switch,

        protocols: []const []const u8,
        // user-defined context for the callback
        callback_ctx: ?*anyopaque,
        // user-defined callback function
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        notify: std.Thread.ResetEvent,

        conn: *quic.QuicConnection,

        fn connectCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
            std.debug.print("Connect callback called with ctx: {any}\n", .{ctx});
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));
            // TODO: Should check v4 or v6 address and also multiple transports situation.
            // For now, we assume v4.
            // defer self.@"switch".transport.dialer_v4.?.connecting = null;
            self.conn = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };
            std.debug.print("Connection established111: {*}\n", .{self.conn});
            self.notify.set();
            std.debug.print("Connection established2222: {*}\n", .{self.conn});
            // conn.newStream(self, newStreamCallback);
        }

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            std.debug.print("New stream callback called with ctx: {any}\n", .{ctx});
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const stream = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            // TODO: To use multistreams, we need to find the protocol handler for the stream.
            // For now, we just use the first protocol handler.
            self.@"switch".proto_handlers.items[0].onInitiatorStart(stream, self.callback_ctx, self.callback);

            // `onInitiatorStart` should set the stream's protocol message handler.
            stream.proto_msg_handler.onActivated(stream) catch |err| {
                std.log.warn("Proto message handler failed with error: {any}. Closing stream {any}.", .{ err, stream });
                self.callback(self.callback_ctx, err);
                // TODO: Close the stream properly.
                return;
            };
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
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const connect_ctx = self.allocator.create(ConnectCallbackCtx) catch unreachable;
        connect_ctx.* = ConnectCallbackCtx{
            .@"switch" = self,
            .protocols = protocols,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .notify = .{},
            .conn = undefined,
        };

        const address_str = std.fmt.allocPrint(self.allocator, "{}", .{address}) catch unreachable;
        defer self.allocator.free(address_str);

        // Check if the connection already exists.
        if (self.connections.get(address_str)) |conn| {
            // If the connection already exists, we can just create a new stream on it.
            connect_ctx.conn = conn;
            connect_ctx.notify.set();
            conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);
        } else {
            // Dial the peer and pass the connect context as the callback context.
            self.transport.dial(address, connect_ctx, ConnectCallbackCtx.connectCallback);
            connect_ctx.notify.wait();
            std.debug.print("Connection established5555: {*}\n", .{connect_ctx.conn});
            // connect_ctx.conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);

            // std.time.sleep( * std.time.us_per_s);
            // connect_ctx.conn.engine.allocator.destroy(connect_ctx.conn);
            // self.allocator.destroy(connect_ctx);

        }
    }

    pub fn listen(
        self: *Switch,
        address: std.net.Address,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const address_str = std.fmt.allocPrint(self.allocator, "{}", .{address}) catch unreachable;

        const gop = self.listeners.getOrPut(address_str) catch unreachable;

        if (gop.found_existing) {
            self.allocator.free(address_str);
            try gop.value_ptr.listen(address);
            return;
        } else {
            const accept_callback_ctx = self.allocator.create(AcceptCallbackCtx) catch unreachable;
            accept_callback_ctx.* = AcceptCallbackCtx{
                .@"switch" = self,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };

            gop.value_ptr.* = self.transport.newListener(accept_callback_ctx, AcceptCallbackCtx.acceptCallback);

            gop.value_ptr.listen(address) catch |err| {
                self.allocator.destroy(accept_callback_ctx);
                const removed_entry = self.listeners.fetchOrderedRemove(address_str).?;
                self.allocator.free(removed_entry.key);
                std.log.warn("Failed to start listener on {s}: {s}", .{ address_str, @errorName(err) });
                return error.ListenerStartFailed;
            };
        }
    }
};
