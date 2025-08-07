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

    is_stopping: bool,

    pub fn init(self: *Switch, allocator: Allocator, transport: *quic.QuicTransport) void {
        self.* = Switch{
            .proto_handlers = std.ArrayList(protocols.AnyProtocolHandler).init(allocator),
            .transport = transport,
            .outgoing_connections = std.StringArrayHashMap(*quic.QuicConnection).init(allocator),
            .allocator = allocator,
            .listeners = std.StringArrayHashMap(quic.QuicListener).init(allocator),
            .incoming_connections = std.ArrayList(*quic.QuicConnection).init(allocator),
            .is_stopping = false,
        };
    }

    pub fn deinit(self: *Switch) void {
        self.is_stopping = true;
        self.outgoingConnectionCloseAndClean();
        // Because the `doClose` function of the `QuicEngine` may schedule a timer task, when it runs, the engine
        // may be freed by `deinit` of `QuicTransport` or `QuicListener`, so we need to wait for a short time.
        // Give some time for the connections to close gracefully.
        // **Note**: Is 2s enough?
        std.time.sleep(2 * std.time.ns_per_s);
    }

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

    const StreamCallbackCtx = struct {
        @"switch": *Switch,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            const self: *StreamCallbackCtx = @ptrCast(@alignCast(ctx.?));
            defer self.@"switch".allocator.destroy(self);

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
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                }.callback);
                return;
            };
        }
    };

    const ConnectCallbackCtx = struct {
        @"switch": *Switch,
        address: std.net.Address,
        protos: []const []const u8,
        user_callback_ctx: ?*anyopaque,
        user_callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        fn connectCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));
            defer self.@"switch".allocator.destroy(self);

            const conn = res catch |err| {
                std.log.warn("Connection failed: {}", .{err});
                self.user_callback(self.user_callback_ctx, err);
                return;
            };

            conn.close_ctx = .{
                .callback = Switch.onOutgoingConnectionClose,
                .callback_ctx = self.@"switch",
                .active_callback = null,
                .active_callback_ctx = null,
            };

            const address_str = std.fmt.allocPrint(self.@"switch".allocator, "{}", .{self.address}) catch unreachable;
            self.@"switch".outgoing_connections.put(address_str, conn) catch unreachable;

            std.log.debug("Connection established to {}", .{self.address});

            const stream_ctx = self.@"switch".allocator.create(StreamCallbackCtx) catch unreachable;
            stream_ctx.* = StreamCallbackCtx{
                .@"switch" = self.@"switch",
                .callback_ctx = self.user_callback_ctx,
                .callback = self.user_callback,
            };

            conn.newStream(stream_ctx, StreamCallbackCtx.newStreamCallback);
        }
    };

    pub fn newStream(
        self: *Switch,
        address: std.net.Address,
        protos: []const []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        if (self.is_stopping) {
            callback(callback_ctx, error.SwitchStopped);
            return;
        }
        const address_str = std.fmt.allocPrint(self.allocator, "{}", .{address}) catch unreachable;
        defer self.allocator.free(address_str);

        // Check if the connection already exists.
        if (self.outgoing_connections.get(address_str)) |conn| {
            // If the connection already exists, we can just create a new stream on it.
            const stream_ctx = self.allocator.create(StreamCallbackCtx) catch unreachable;
            stream_ctx.* = StreamCallbackCtx{
                .@"switch" = self,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };

            conn.newStream(stream_ctx, StreamCallbackCtx.newStreamCallback);
        } else {
            const connect_ctx = self.allocator.create(ConnectCallbackCtx) catch unreachable;
            connect_ctx.* = ConnectCallbackCtx{
                .@"switch" = self,
                .address = address,
                .protos = protos,
                .user_callback_ctx = callback_ctx,
                .user_callback = callback,
            };
            self.transport.dial(address, connect_ctx, ConnectCallbackCtx.connectCallback);
        }
    }

    pub fn listen(
        self: *Switch,
        address: std.net.Address,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        if (self.is_stopping) {
            return error.SwitchStopped;
        }
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

        if (self.is_stopping) {
            // If the switch is stopping, we do not need to handle the connection close.
            return;
        }

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

        if (self.is_stopping) {
            // If the switch is stopping, we do not need to handle the connection close.
            return;
        }

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

    fn outgoingConnectionCloseAndCleanCallback(ctx: ?*anyopaque, _: anyerror!*quic.QuicConnection) void {
        const self: *Switch = @ptrCast(@alignCast(ctx.?));

        // This callback is called when an outgoing connection is closed.
        // We will remove it from the list and clean up resources.
        self.outgoingConnectionCloseAndClean();
    }

    fn outgoingConnectionCloseAndClean(self: *Switch) void {
        const outgoing_entry = self.outgoing_connections.pop();
        if (outgoing_entry) |e| {
            self.allocator.free(e.key);
            e.value.close(self, outgoingConnectionCloseAndCleanCallback);
        } else {
            self.incomingConnectionCloseAndClean(); // Clean up resources if no outgoing connections are left.
        }
    }

    fn incomingConnectionCloseAndCleanCallback(ctx: ?*anyopaque, _: anyerror!*quic.QuicConnection) void {
        const self: *Switch = @ptrCast(@alignCast(ctx.?));

        // This callback is called when an incoming connection is closed.
        // We will remove it from the list and clean up resources.
        self.incomingConnectionCloseAndClean();
    }

    fn incomingConnectionCloseAndClean(self: *Switch) void {
        const incoming_entry = self.incoming_connections.pop();
        if (incoming_entry) |conn| {
            conn.close(self, incomingConnectionCloseAndCleanCallback);
        } else {
            self.cleanResources(); // Clean up resources if no incoming connections are left.
        }
    }

    fn cleanResources(self: *Switch) void {
        // Because the `doClose` function of the `QuicEngine` may schedule a timer task, when it runs, the engine
        // may be freed by `deinit` of `QuicTransport` or `QuicListener`, so we need to wait for a short time.
        // Give some time for the connections to close gracefully.
        // **Note**: Is 1s enough?
        std.time.sleep(1 * std.time.ns_per_s);

        self.outgoing_connections.deinit();
        self.incoming_connections.deinit();

        self.transport.deinit();
        var listener_iter = self.listeners.iterator();
        while (listener_iter.next()) |entry| {
            var listener = entry.value_ptr.*;
            listener.deinit(); // This should close the listener's engine.

            if (listener.listen_callback_ctx) |lctx| {
                const accept_ctx: *ListenCallbackCtx = @ptrCast(@alignCast(lctx));
                self.allocator.destroy(accept_ctx);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.listeners.deinit();

        self.proto_handlers.deinit();
    }
};
