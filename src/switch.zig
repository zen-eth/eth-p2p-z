const lsquic = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
    @cInclude("lsxpack_header.h");
});
const std = @import("std");
const builtin = @import("builtin");
const libp2p = @import("root.zig");
const quic = libp2p.transport.quic;
const protocols = libp2p.protocols;
const Allocator = std.mem.Allocator;
const mss = protocols.mss;
const multiaddr = @import("multiformats").multiaddr;
const Multiaddr = multiaddr.Multiaddr;
const MultiaddrProtocol = multiaddr.Protocol;
const io_loop = libp2p.thread_event_loop;
const net = std.net;
const posix = std.posix;
const mem = std.mem;

const has_getifaddrs = switch (builtin.target.os.tag) {
    .windows, .wasi => false,
    else => true,
};

const IFF_LOOPBACK: u32 = 0x8;

const IfAddrs = if (has_getifaddrs)
    extern struct {
        ifa_next: ?*IfAddrs,
        ifa_name: ?[*:0]const u8,
        ifa_flags: u32,
        ifa_addr: ?*posix.sockaddr,
        ifa_netmask: ?*posix.sockaddr,
        ifa_dstaddr: ?*posix.sockaddr,
        ifa_data: ?*anyopaque,
    }
else
    opaque {};

const getifaddrs_fn = if (has_getifaddrs)
    @extern(*const fn (*?*IfAddrs) callconv(.C) i32, .{ .name = "getifaddrs" })
else
    @as(*const fn (*?*IfAddrs) callconv(.C) i32, undefined);

const freeifaddrs_fn = if (has_getifaddrs)
    @extern(*const fn (?*IfAddrs) callconv(.C) void, .{ .name = "freeifaddrs" })
else
    @as(*const fn (?*IfAddrs) callconv(.C) void, undefined);

/// The Switch struct is the main entry point for managing connections and protocol handlers.
/// It acts as a central hub for handling incoming and outgoing connections,
/// as well as managing protocol handlers for different protocols.
/// It supports multiple transports, but currently only one transport is supported at a time.
/// The Switch is responsible for creating and managing connections, listeners, and protocol handlers.
/// It also provides methods for dialing peers, listening for incoming connections,
/// and handling protocol messages.
pub const Switch = struct {
    mss_handler: mss.MultistreamSelectHandler,

    // TODO: In the future, we should support multiple transports.
    // Only one transport is supported at a time.
    transport: *quic.QuicTransport,

    // TODO: Once peerid is implemented, we can use it to identify connections.
    // For now, we use the peer address as the key.
    outgoing_connections: std.StringArrayHashMap(*quic.QuicConnection),

    allocator: Allocator,

    listeners: std.StringArrayHashMap(quic.QuicListener),

    incoming_connections: std.ArrayList(*quic.QuicConnection),

    is_stopping: std.atomic.Value(bool),

    stopped_notify: std.Thread.ResetEvent,

    pub fn init(self: *Switch, allocator: Allocator, transport: *quic.QuicTransport) void {
        self.* = Switch{
            .transport = transport,
            .outgoing_connections = std.StringArrayHashMap(*quic.QuicConnection).init(allocator),
            .allocator = allocator,
            .listeners = std.StringArrayHashMap(quic.QuicListener).init(allocator),
            .incoming_connections = std.ArrayList(*quic.QuicConnection).init(allocator),
            .is_stopping = std.atomic.Value(bool).init(false),
            .mss_handler = mss.MultistreamSelectHandler.init(allocator),
            .stopped_notify = .{},
        };
    }

    pub fn deinit(self: *Switch) void {
        self.cleanResources();
    }

    pub fn stop(self: *Switch) void {
        self.doClose();

        self.stopped_notify.wait();
    }

    pub fn doClose(self: *Switch) void {
        if (self.is_stopping.swap(true, .acq_rel)) {
            return;
        }

        var listener_iter = self.listeners.iterator();
        while (listener_iter.next()) |entry| {
            entry.value_ptr.stop() catch unreachable;
        }

        self.transport.stop() catch unreachable;

        self.stopped_notify.set();
    }

    const ListenCallbackCtx = struct {
        network_switch: *Switch,
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
                .callback_ctx = self.network_switch,
                .active_callback_ctx = null,
                .active_callback = null,
            };
            self.network_switch.incoming_connections.append(conn) catch unreachable;
        }

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            const self: *ListenCallbackCtx = @ptrCast(@alignCast(ctx.?));
            const stream = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            if (stream.close_ctx == null) {
                stream.close_ctx = .{
                    .callback_ctx = null,
                    .callback = null,
                    .active_callback_ctx = null,
                    .active_callback = null,
                };
            }

            self.network_switch.mss_handler.onResponderStart(stream, self.callback_ctx, self.callback) catch |err| {
                std.log.warn("Failed to start responder: {}", .{err});
                self.callback(self.callback_ctx, err);

                // TODO: There is an error thrown in the lsquic library when close the stream in the server mode.
                // So that right now the stream will be closed when the connection close function called.
                // When call `stream.close` here, because this function is called in `onNewStream`, the `stream_ctx` hasn't been returned yet,
                // the `stream_ctx` will be null passed to `onStreamClose` function, so that we need to call `stream.deinit` and destroy it manually.
                // stream.close(null, struct {
                //     fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                // }.callback);
                // stream.deinit();
                // stream.conn.engine.allocator.destroy(stream);
                return;
            };

            // `onResponderStart` should set the stream's protocol message handler.
            stream.proto_msg_handler.?.onActivated(stream) catch |err| {
                std.log.warn("Proto message handler failed with error: {any}. Closing stream {any}.", .{ err, stream });
                self.callback(self.callback_ctx, err);

                // TODO: There is an error thrown in the lsquic library when close the stream in the server mode.
                // So that right now the stream will be closed when the connection close function called.
                // When call `stream.close` here, because this function is called in `onNewStream`, the `stream_ctx` hasn't been returned yet,
                // the `stream_ctx` will be null passed to `onStreamClose` function, so that we need to call `stream.deinit` and destroy it manually.
                // stream.close(null, struct {
                //     fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                // }.callback);
                // stream.proto_msg_handler.?.onClose(stream) catch |e| {
                //     std.log.warn("Protocol message handler failed with error: {}.", .{e});
                // };
                // stream.deinit();
                // stream.conn.engine.allocator.destroy(stream);
                return;
            };
        }
    };

    const StreamCallbackCtx = struct {
        network_switch: *Switch,
        proposed_protocols: []const []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        fn newStreamCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
            const self: *StreamCallbackCtx = @ptrCast(@alignCast(ctx.?));
            defer self.network_switch.allocator.destroy(self);

            const stream = res catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };

            if (stream.close_ctx == null) {
                stream.close_ctx = .{
                    .callback_ctx = null,
                    .callback = null,
                    .active_callback_ctx = null,
                    .active_callback = null,
                };
            }

            stream.proposed_protocols = self.proposed_protocols;

            self.network_switch.mss_handler.onInitiatorStart(stream, self.callback_ctx, self.callback) catch |err| {
                std.log.warn("Failed to start initiator: {}", .{err});
                self.callback(self.callback_ctx, err);

                // When call `stream.close` here, because this function is called in `onNewStream`, the `stream_ctx` hasn't been returned yet,
                // the `stream_ctx` will be null passed to `onStreamClose` function, so that we need to call `stream.deinit` and destroy it manually.
                stream.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                }.callback);
                return;
            };

            // `onInitiatorStart` should set the stream's protocol message handler.
            stream.proto_msg_handler.?.onActivated(stream) catch |err| {
                std.log.warn("Proto message handler failed with error: {}. ", .{err});
                self.callback(self.callback_ctx, err);

                // When call `stream.close` here, because this function is called in `onNewStream`, the `stream_ctx` hasn't been returned yet,
                // the `stream_ctx` will be null passed to `onStreamClose` function, so that we need to call `stream.deinit` and destroy it manually.
                stream.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                }.callback);
                stream.proto_msg_handler.?.onClose(stream) catch |e| {
                    std.log.warn("Protocol message handler failed with error: {}.", .{e});
                };
                return;
            };
        }
    };

    const ConnectCallbackCtx = struct {
        network_switch: *Switch,
        address: Multiaddr,
        proposed_protocols: []const []const u8,
        user_callback_ctx: ?*anyopaque,
        user_callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        fn connectCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
            const self: *ConnectCallbackCtx = @ptrCast(@alignCast(ctx.?));
            defer self.network_switch.allocator.destroy(self);

            const conn = res catch |err| {
                std.log.warn("Connection failed: {}", .{err});
                self.user_callback(self.user_callback_ctx, err);
                return;
            };

            conn.close_ctx = .{
                .callback = Switch.onOutgoingConnectionClose,
                .callback_ctx = self.network_switch,
                .active_callback = null,
                .active_callback_ctx = null,
            };

            const address_str = self.address.toString(self.network_switch.allocator) catch |err| {
                std.log.warn("Failed to convert address to string: {}", .{err});
                self.user_callback(self.user_callback_ctx, err);
                return;
            };

            self.network_switch.outgoing_connections.put(address_str, conn) catch unreachable;

            const stream_ctx = self.network_switch.allocator.create(StreamCallbackCtx) catch unreachable;
            stream_ctx.* = StreamCallbackCtx{
                .network_switch = self.network_switch,
                .callback_ctx = self.user_callback_ctx,
                .callback = self.user_callback,
                .proposed_protocols = self.proposed_protocols,
            };

            conn.newStream(stream_ctx, StreamCallbackCtx.newStreamCallback);
        }
    };

    pub fn newStream(
        self: *Switch,
        address: Multiaddr,
        proposed_protocols: []const []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        if (self.is_stopping.load(.acquire)) {
            callback(callback_ctx, error.SwitchStopped);
            return;
        }
        const address_str = address.toString(self.allocator) catch |err| {
            std.log.warn("Failed to convert address to string: {}", .{err});
            callback(callback_ctx, err);
            return;
        };
        defer self.allocator.free(address_str);

        // Check if the connection already exists.
        if (self.outgoing_connections.get(address_str)) |conn| {
            // If the connection already exists, we can just create a new stream on it.
            const stream_ctx = self.allocator.create(StreamCallbackCtx) catch unreachable;
            stream_ctx.* = StreamCallbackCtx{
                .network_switch = self,
                .callback_ctx = callback_ctx,
                .callback = callback,
                .proposed_protocols = proposed_protocols,
            };

            conn.newStream(stream_ctx, StreamCallbackCtx.newStreamCallback);
        } else {
            const connect_ctx = self.allocator.create(ConnectCallbackCtx) catch unreachable;
            connect_ctx.* = ConnectCallbackCtx{
                .network_switch = self,
                .address = address,
                .proposed_protocols = proposed_protocols,
                .user_callback_ctx = callback_ctx,
                .user_callback = callback,
            };
            self.transport.dial(address, connect_ctx, ConnectCallbackCtx.connectCallback);
        }
    }

    pub fn listen(
        self: *Switch,
        address: Multiaddr,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        if (self.is_stopping.load(.acquire)) {
            return error.SwitchStopped;
        }
        const address_str = try address.toString(self.allocator);

        const gop = self.listeners.getOrPut(address_str) catch unreachable;

        if (gop.found_existing) {
            self.allocator.free(address_str);
            try gop.value_ptr.listen(address);
            return;
        } else {
            const accept_callback_ctx = self.allocator.create(ListenCallbackCtx) catch unreachable;
            accept_callback_ctx.* = ListenCallbackCtx{
                .network_switch = self,
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

    pub fn addProtocolHandler(
        self: *Switch,
        proto_id: protocols.ProtocolId,
        handler: protocols.AnyProtocolHandler,
    ) !void {
        try self.mss_handler.addProtocolHandler(proto_id, handler);
    }

    pub const ListenAddressError = Allocator.Error || posix.GetHostNameError || multiaddr.Error || error{ AddressResolutionFailed, NoUsableAddress };

    pub fn listenMultiaddrs(self: *Switch, allocator: Allocator) ListenAddressError!std.ArrayList([]u8) {
        var results = std.ArrayList([]u8).init(allocator);
        errdefer Switch.freeListenMultiaddrs(allocator, &results);

        var host_addrs = try collectHostAddrs(allocator);
        defer host_addrs.deinit();

        var listener_iter = self.listeners.iterator();
        while (listener_iter.next()) |entry| {
            const key_slice = entry.key_ptr.*;
            var base_ma = Multiaddr.fromString(allocator, key_slice) catch |err| {
                std.log.warn("failed to parse listener multiaddr {s}: {s}", .{ key_slice, @errorName(err) });
                continue;
            };
            defer base_ma.deinit();

            const ip_index = findIpComponentIndex(base_ma) orelse continue;

            for (host_addrs.items) |host| {
                const proto = host.toProtocol();
                const maybe_ma = base_ma.replace(allocator, ip_index, proto) catch |err| {
                    std.log.warn(
                        "failed to update listener multiaddr {s} with host protocol: {s}",
                        .{ key_slice, @errorName(err) },
                    );
                    continue;
                };
                if (maybe_ma) |mut_ma| {
                    defer mut_ma.deinit();
                    const addr_str = mut_ma.toString(allocator) catch |err| {
                        std.log.warn(
                            "unable to render listener multiaddr for {s}: {s}",
                            .{ key_slice, @errorName(err) },
                        );
                        continue;
                    };
                    errdefer allocator.free(addr_str);
                    if (!containsString(results.items, addr_str)) {
                        try results.append(addr_str);
                    } else {
                        allocator.free(addr_str);
                    }
                }
            }
        }

        if (results.items.len == 0) {
            return error.NoUsableAddress;
        }

        return results;
    }

    pub fn freeListenMultiaddrs(allocator: Allocator, list: *std.ArrayList([]u8)) void {
        for (list.items) |addr| {
            allocator.free(addr);
        }
        list.deinit();
    }

    const HostAddr = union(enum) {
        ipv4: [4]u8,
        ipv6: [16]u8,

        fn toProtocol(self: HostAddr) MultiaddrProtocol {
            return switch (self) {
                .ipv4 => |bytes| MultiaddrProtocol{ .Ip4 = net.Ip4Address.init(bytes, 0) },
                .ipv6 => |bytes| MultiaddrProtocol{ .Ip6 = net.Ip6Address.init(bytes, 0, 0, 0) },
            };
        }
    };

    fn collectHostAddrs(allocator: Allocator) ListenAddressError!std.ArrayList(HostAddr) {
        if (has_getifaddrs) {
            const maybe_result = collectHostAddrsFromInterfaces(allocator) catch |err| switch (err) {
                error.AddressResolutionFailed, error.NoUsableAddress => null,
                else => return err,
            };
            if (maybe_result) |res| {
                return res;
            }
        }

        return collectHostAddrsFromHostname(allocator);
    }

    fn collectHostAddrsFromInterfaces(allocator: Allocator) ListenAddressError!std.ArrayList(HostAddr) {
        comptime {
            if (!has_getifaddrs) @compileError("collectHostAddrsFromInterfaces is unavailable on this target");
        }

        var result = std.ArrayList(HostAddr).init(allocator);
        errdefer result.deinit();

        var ifaddrs_head: ?*IfAddrs = null;
        if (getifaddrs_fn(&ifaddrs_head) != 0) {
            return error.AddressResolutionFailed;
        }
        defer freeifaddrs_fn(ifaddrs_head);

        var cursor = ifaddrs_head;
        while (cursor) |ifa| : (cursor = ifa.*.ifa_next) {
            const addr_ptr = ifa.*.ifa_addr orelse continue;
            if ((ifa.*.ifa_flags & IFF_LOOPBACK) != 0) continue;

            switch (addr_ptr.*.family) {
                posix.AF.INET => {
                    const bytes: *const [16]u8 = @ptrCast(addr_ptr);
                    // IPv4 address is at offset 4-7 in sockaddr_in (2 bytes family + 2 bytes port + 4 bytes addr)
                    const ip_bytes: [4]u8 = bytes[4..8].*;
                    if (ip_bytes[0] == 127 or ip_bytes[0] == 0) continue;
                    const candidate = HostAddr{ .ipv4 = ip_bytes };
                    if (!containsHostAddr(result.items, candidate)) {
                        try result.append(candidate);
                    }
                },
                posix.AF.INET6 => {
                    const bytes: *const [28]u8 = @ptrCast(addr_ptr);
                    // IPv6 address is at offset 8-23 in sockaddr_in6
                    const ip_bytes: [16]u8 = bytes[8..24].*;
                    if (isIpv6Loopback(ip_bytes) or isIpv6Unspecified(ip_bytes) or isIpv6LinkLocal(ip_bytes)) {
                        continue;
                    }
                    const candidate = HostAddr{ .ipv6 = ip_bytes };
                    if (!containsHostAddr(result.items, candidate)) {
                        try result.append(candidate);
                    }
                },
                else => continue,
            }
        }

        if (result.items.len == 0) {
            return error.NoUsableAddress;
        }

        return result;
    }

    fn collectHostAddrsFromHostname(allocator: Allocator) ListenAddressError!std.ArrayList(HostAddr) {
        var result = std.ArrayList(HostAddr).init(allocator);
        errdefer result.deinit();

        var hostname_buf: [posix.HOST_NAME_MAX]u8 = undefined;
        const hostname = try posix.gethostname(&hostname_buf);

        const addr_list = net.getAddressList(allocator, hostname, 0) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.AddressResolutionFailed,
            };
        };
        defer addr_list.deinit();

        for (addr_list.addrs) |addr| {
            switch (addr.any.family) {
                posix.AF.INET => {
                    const ip_bytes = mem.toBytes(addr.in.sa.addr);
                    if (ip_bytes[0] == 127 or ip_bytes[0] == 0) continue;
                    const candidate = HostAddr{ .ipv4 = ip_bytes };
                    if (!containsHostAddr(result.items, candidate)) {
                        try result.append(candidate);
                    }
                },
                posix.AF.INET6 => {
                    const ip_bytes = mem.toBytes(addr.in6.sa.addr);
                    if (isIpv6Loopback(ip_bytes) or isIpv6Unspecified(ip_bytes) or isIpv6LinkLocal(ip_bytes)) {
                        continue;
                    }
                    const candidate = HostAddr{ .ipv6 = ip_bytes };
                    if (!containsHostAddr(result.items, candidate)) {
                        try result.append(candidate);
                    }
                },
                else => {},
            }
        }

        if (result.items.len == 0) {
            return error.NoUsableAddress;
        }

        return result;
    }

    fn containsHostAddr(haystack: []const HostAddr, needle: HostAddr) bool {
        for (haystack) |existing| {
            switch (existing) {
                .ipv4 => |bytes| switch (needle) {
                    .ipv4 => |other| if (mem.eql(u8, &bytes, &other)) return true,
                    else => {},
                },
                .ipv6 => |bytes| switch (needle) {
                    .ipv6 => |other| if (mem.eql(u8, &bytes, &other)) return true,
                    else => {},
                },
            }
        }
        return false;
    }

    fn containsString(haystack: [][]u8, needle: []const u8) bool {
        for (haystack) |candidate| {
            if (mem.eql(u8, candidate, needle)) {
                return true;
            }
        }
        return false;
    }

    fn findIpComponentIndex(ma: Multiaddr) ?usize {
        var iter = ma.iterator();
        var index: usize = 0;
        while (true) : (index += 1) {
            const opt_proto = iter.next() catch return null;
            const proto = opt_proto orelse break;
            switch (proto) {
                .Ip4, .Ip6 => return index,
                else => {},
            }
        }
        return null;
    }

    fn isIpv6Loopback(bytes: [16]u8) bool {
        for (bytes[0..15]) |b| {
            if (b != 0) return false;
        }
        return bytes[15] == 1;
    }

    fn isIpv6Unspecified(bytes: [16]u8) bool {
        for (bytes) |b| {
            if (b != 0) return false;
        }
        return true;
    }

    fn isIpv6LinkLocal(bytes: [16]u8) bool {
        return bytes[0] == 0xfe and (bytes[1] & 0xc0) == 0x80;
    }

    fn onOutgoingConnectionClose(ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void {
        const self: *Switch = @ptrCast(@alignCast(ctx.?));

        const conn = res catch |err| {
            std.log.warn("Connection close callback failed with error: {any}", .{err});
            return;
        };

        if (conn.connect_ctx == null) {
            std.log.err("Cannot remove outgoing connection: connect_ctx is null.", .{});
            return;
        }

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

    fn cleanResources(self: *Switch) void {
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
        // Call it here because the listeners and transports all used quic engine,
        // so we need to clean up the global state.
        // TODO: Can we not expose lsquic to switch?
        // lsquic.lsquic_global_cleanup();
        self.listeners.deinit();

        self.mss_handler.deinit();

        // Iterate through any remaining outgoing connections and free their keys
        // before deinitializing the HashMap. This prevents memory leaks if connections
        // were not gracefully removed during the shutdown process.
        var out_iter = self.outgoing_connections.iterator();
        while (out_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.outgoing_connections.deinit();
        self.incoming_connections.deinit();

        self.finalCleanup();
    }

    fn finalCleanup(_: *Switch) void {
        lsquic.lsquic_global_cleanup();
    }
};
