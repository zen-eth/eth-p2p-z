const std = @import("std");
const libp2p = @import("root.zig");
const quic = libp2p.transport.quic;
const protocols = libp2p.protocols;
const Allocator = std.mem.Allocator;

/// The Switch struct is the main entry point for managing connections and protocol handlers.
/// It acts as a central hub for handling incoming and outgoing connections,
/// as well as managing protocol handlers for different protocols.
/// It supports multiple transports, but currently only one transport is supported at a time.
/// The Switch is responsible for creating and managing connections, listeners, and protocol handlers.
/// It also provides methods for dialing peers, listening for incoming connections,
/// and handling protocol messages.
pub const Switch = struct {
    proto_handlers: std.ArrayList(protocols.AnyProtocolHandler),

    // TODO: In the future, we should support multiple transports.
    // Only one transport is supported at a time.
    transport: *quic.QuicTransport,

    // TODO: Once peerid is implemented, we can use it to identify connections.
    // For now, we use the peer address as the key.
    outgoing_connections: std.StringArrayHashMap(*quic.QuicConnection),

    allocator: Allocator,

    listeners: std.StringArrayHashMap(quic.QuicListener),

    incoming_connections: std.ArrayList(*quic.QuicConnection),

    pub fn init(self: *Switch, allocator: Allocator, transport: *quic.QuicTransport) void {
        self.* = Switch{
            .proto_handlers = std.ArrayList(protocols.AnyProtocolHandler).init(allocator),
            .transport = transport,
            .outgoing_connections = std.StringArrayHashMap(*quic.QuicConnection).init(allocator),
            .allocator = allocator,
            .listeners = std.StringArrayHashMap(quic.QuicListener).init(allocator),
            .incoming_connections = std.ArrayList(*quic.QuicConnection).init(allocator),
        };
    }

    pub fn deinit(self: *Switch) void {
        // Close all outgoing connections. The onOutgoingConnectionClose callback
        // will be triggered for each, handling removal and cleanup.
        var out_iter = self.outgoing_connections.iterator();
        while (out_iter.next()) |entry| {
            // TODO: Use a pool for the close context to avoid frequent allocations.
            const close_ctx = self.allocator.create(ConnectionCloseCallbackCtx) catch unreachable;
            close_ctx.* = ConnectionCloseCallbackCtx{
                .notify = .{},
                .@"switch" = self,
            };
            entry.value_ptr.*.close(close_ctx, ConnectionCloseCallbackCtx.closeCallback); // Close the connection, which will trigger the close callback.
            close_ctx.notify.wait();
            self.allocator.destroy(close_ctx);
        }

        self.outgoing_connections.deinit();

        // Close all incoming connections. The onIncomingConnectionClose callback
        // will be triggered for each, handling removal and cleanup.
        for (self.incoming_connections.items) |conn| {
            const close_ctx = self.allocator.create(ConnectionCloseCallbackCtx) catch unreachable;
            close_ctx.* = ConnectionCloseCallbackCtx{
                .notify = .{},
                .@"switch" = self,
            };
            conn.close(close_ctx, ConnectionCloseCallbackCtx.closeCallback); // Close the connection, which will trigger the close callback.
            close_ctx.notify.wait(); // Wait for the close callback to run.
            self.allocator.destroy(close_ctx); // Clean up the close context.
        }

        self.incoming_connections.deinit();

        self.transport.deinit(); // Deinitialize the transport.
        // Close all listeners and free their resources.
        var listener_iter = self.listeners.iterator();
        while (listener_iter.next()) |entry| {
            var listener = entry.value_ptr.*;
            listener.deinit(); // This should close the listener's engine.

            if (listener.listen_callback_ctx) |ctx| {
                const accept_ctx: *ListenCallbackCtx = @ptrCast(@alignCast(ctx));
                self.allocator.destroy(accept_ctx);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.listeners.deinit();

        self.proto_handlers.deinit();
    }

    const ConnectionCloseCallbackCtx = struct {
        notify: std.Thread.ResetEvent,

        @"switch": *Switch,

        fn closeCallback(ctx: ?*anyopaque, _: anyerror!*quic.QuicConnection) void {
            const self: *ConnectionCloseCallbackCtx = @ptrCast(@alignCast(ctx.?));
            // Notify that the connection has been closed.
            self.notify.set();
        }
    };

    const ListenCallbackCtx = struct {
        @"switch": *Switch,
        // user-defined context for the callback
        callback_ctx: ?*anyopaque,
        // user-defined callback function
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        fn listenCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
            const self: *ListenCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const conn = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            conn.onStream(self, newStreamCallback);
            conn.close_ctx = .{
                .callback = Switch.onIncomingConnectionClose,
                .callback_ctx = self.@"switch",
                .active_callback_ctx = null,
                .active_callback = null,
            };
            self.@"switch".incoming_connections.append(conn) catch unreachable;
        }

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            const self: *ListenCallbackCtx = @ptrCast(@alignCast(ctx.?));
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
                stream.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                        // Handle stream close if needed.
                    }
                }.callback);
                return;
            };
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
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));

            self.conn = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            self.conn.close_ctx = .{
                .callback = Switch.onOutgoingConnectionClose,
                .callback_ctx = self.@"switch",
                .active_callback = null,
                .active_callback_ctx = null,
            };

            self.@"switch".outgoing_connections.put(
                std.fmt.allocPrint(self.@"switch".allocator, "{}", .{self.conn.connect_ctx.?.address}) catch unreachable,
                self.conn,
            ) catch unreachable;
            // Can't call newStream in the callback directly, it will cause a re-entrancy issue.
            self.notify.set();
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

            // `onInitiatorStart` should set the stream's protocol message handler.
            stream.proto_msg_handler.onActivated(stream) catch |err| {
                std.log.warn("Proto message handler failed with error: {any}. Closing stream {any}.", .{ err, stream });
                self.callback(self.callback_ctx, err);

                stream.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                        // Handle stream close if needed.
                    }
                }.callback);
                return;
            };
        }
    };

    pub fn newStream(
        self: *Switch,
        address: std.net.Address,
        protos: []const []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const connect_ctx = self.allocator.create(ConnectCallbackCtx) catch unreachable;
        connect_ctx.* = ConnectCallbackCtx{
            .@"switch" = self,
            .protocols = protos,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .notify = .{},
            .conn = undefined,
        };

        const address_str = std.fmt.allocPrint(self.allocator, "{}", .{address}) catch unreachable;
        defer self.allocator.free(address_str);

        // Check if the connection already exists.
        if (self.outgoing_connections.get(address_str)) |conn| {
            // If the connection already exists, we can just create a new stream on it.
            connect_ctx.conn = conn;
            connect_ctx.notify.set();
            conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);
        } else {
            self.transport.dial(address, connect_ctx, ConnectCallbackCtx.connectCallback);
            connect_ctx.notify.wait();
            // TODO: Need check if the event loop thread is same with lsquic thread. If not, we could try to make new stream asynchronously.
            connect_ctx.conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);
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
            const accept_callback_ctx = self.allocator.create(ListenCallbackCtx) catch unreachable;
            accept_callback_ctx.* = ListenCallbackCtx{
                .@"switch" = self,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };

            gop.value_ptr.* = self.transport.newListener(accept_callback_ctx, ListenCallbackCtx.listenCallback);

            gop.value_ptr.listen(address) catch |err| {
                self.allocator.destroy(accept_callback_ctx);
                const removed_entry = self.listeners.fetchOrderedRemove(address_str).?;
                self.allocator.free(removed_entry.key);
                std.log.warn("Failed to start listener on {s}: {s}", .{ address_str, @errorName(err) });
                return error.ListenerStartFailed;
            };
        }
    }

    fn onOutgoingConnectionClose(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
        const self: *Switch = @ptrCast(@alignCast(ctx.?));

        const conn = res catch |err| {
            std.log.warn("Connection close callback failed with error: {any}", .{err});
            return;
        };

        const address_str = std.fmt.allocPrint(self.allocator, "{}", .{conn.connect_ctx.?.address}) catch unreachable;
        defer self.allocator.free(address_str);

        if (self.outgoing_connections.fetchOrderedRemove(address_str)) |removed_entry| {
            // The value of the entry is the connection pointer, which is the same as `conn`.
            // The key is the address string, which was allocated and stored in the HashMap.
            // We must free the key's memory.
            self.allocator.free(removed_entry.key);
            const conn_ctx: *ConnectCallbackCtx = @ptrCast(@alignCast(removed_entry.value.connect_ctx.?.callback_ctx));
            self.allocator.destroy(conn_ctx);

            // The connection object itself is managed by the QuicEngine's allocator
            // and is destroyed in `onConnClosed` after this callback returns.
            // So we should NOT destroy `conn` here.
            return;
        }

        // If the connection was not found in the outgoing connections, it might have been closed already.
        // This might indicate a logic error elsewhere, but is safe to ignore
        std.log.warn("Outgoing connection close callback invoked, but connection not found in the outgoing connections list.", .{});
    }

    fn onIncomingConnectionClose(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
        const self: *Switch = @ptrCast(@alignCast(ctx.?));

        const conn = res catch |err| {
            std.log.warn("Connection close callback failed with error: {any}", .{err});
            return;
        };

        for (self.incoming_connections.items, 0..) |item, i| {
            if (item == conn) {
                // Found the connection, now remove it by its index.
                _ = self.incoming_connections.orderedRemove(i);

                // The connection is removed from the list, but we do not need to free it here.
                // It will be closed and cleaned up by the QuicEngine.
                return;
            }
        }

        // This code is reached if the connection was not found in the list, which might
        // indicate a logic error elsewhere, but is safe to ignore for now.
        std.log.warn("Incoming connection close callback invoked, but connection not found in the list.", .{});
    }
};
