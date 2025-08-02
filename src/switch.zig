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
    outgoing_connections: std.StringArrayHashMap(*quic.QuicConnection),

    allocator: Allocator,

    listeners: std.StringArrayHashMap(quic.QuicListener),

    incoming_connections: std.ArrayList(*quic.QuicConnection),

    pub fn init(self: *Switch, allocator: Allocator, transport: *quic.QuicTransport) void {
        self.* = Switch{
            .proto_handlers = std.ArrayList(proto_handler.AnyProtocolHandler).init(allocator),
            .transport = transport,
            .outgoing_connections = std.StringArrayHashMap(*quic.QuicConnection).init(allocator),
            .allocator = allocator,
            .listeners = std.StringArrayHashMap(quic.QuicListener).init(allocator),
            .incoming_connections = std.ArrayList(*quic.QuicConnection).init(allocator),
        };
    }

    pub fn deinit(self: *Switch) void {
        // 1. Close all outgoing connections. The onOutgoingConnectionClose callback
        // will be triggered for each, handling removal and cleanup.
        var out_iter = self.outgoing_connections.iterator();
        while (out_iter.next()) |entry| {
            std.debug.print("Closing outgoing connection: {*}\n", .{entry.value_ptr});
            entry.value_ptr.*.close(null, null); // Close the connection, which will trigger the close callback.
            std.time.sleep(100 * std.time.ns_per_ms); // Give some time for the close callback to run.
        }
        // After all close callbacks have run, the map should be empty.
        // We deinit it to free the map's own memory.
        self.outgoing_connections.deinit();

        // 2. Close all incoming connections. The onIncomingConnectionClose callback
        // will be triggered for each, handling removal and cleanup.
        for (self.incoming_connections.items) |conn| {
            std.debug.print("Closing incoming connection: {*}\n", .{conn});
            conn.close(null, null); // Close the connection, which will trigger the close callback.
            std.time.sleep(100 * std.time.ns_per_ms); // Give some time for the close callback to run.

        }
        // After all close callbacks have run, the list should be empty.
        // We deinit it to free the list's own memory.
        self.incoming_connections.deinit();

        // 3. Close all listeners and free their resources.
        var listener_iter = self.listeners.iterator();
        while (listener_iter.next()) |entry| {
            var listener = entry.value_ptr.*;
            listener.deinit(); // This should close the listener's engine.

            if (listener.accept_callback_ctx) |ctx| {
                const accept_ctx: *AcceptCallbackCtx = @ptrCast(@alignCast(ctx));
                self.allocator.destroy(accept_ctx);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.listeners.deinit();

        // 4. Finally, deinit the protocol handlers list.
        self.proto_handlers.deinit();
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
            conn.close_ctx = .{
                .callback = Switch.onIncomingConnectionClose,
                .callback_ctx = self.@"switch",
                .ud_callback = null,
                .ud_callback_ctx = null,
            };
            self.@"switch".incoming_connections.append(conn) catch unreachable;
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
            std.debug.print("Accept new stream established: {*}\n", .{stream});
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

            self.conn = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            self.conn.close_ctx = .{
                .callback = Switch.onOutgoingConnectionClose,
                .callback_ctx = self.@"switch",
                .ud_callback = null,
                .ud_callback_ctx = null,
            };

            self.@"switch".outgoing_connections.put(
                std.fmt.allocPrint(self.@"switch".allocator, "{}", .{self.conn.connect_ctx.?.address}) catch unreachable,
                self.conn,
            ) catch unreachable;
            // Can't call newStream in the callback directly, it will cause a re-entrancy issue.
            self.notify.set();
            std.debug.print("Connection established: {*}\n", .{self.conn});
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
            std.debug.print("New stream established: {*}\n", .{stream});
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
        if (self.outgoing_connections.get(address_str)) |conn| {
            // If the connection already exists, we can just create a new stream on it.
            connect_ctx.conn = conn;
            connect_ctx.notify.set();
            conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);
        } else {
            // Dial the peer and pass the connect context as the callback context.
            self.transport.dial(address, connect_ctx, ConnectCallbackCtx.connectCallback);
            connect_ctx.notify.wait();
            std.debug.print("Connection established5555: {*}\n", .{connect_ctx.conn});
            connect_ctx.conn.newStream(connect_ctx, ConnectCallbackCtx.newStreamCallback);

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
        }
        // The connection object itself is managed by the QuicEngine's allocator
        // and is destroyed in `onConnClosed` after this callback returns.
        // So we should NOT destroy `conn` here.
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
    }
};
