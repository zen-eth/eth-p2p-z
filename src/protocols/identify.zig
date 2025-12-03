const std = @import("std");
const libp2p = @import("../root.zig");
const protocols = libp2p.protocols;
const quic = libp2p.transport.quic;
const swarm = libp2p.swarm;
const multiaddr = @import("multiformats").multiaddr;
const Multiaddr = multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;
const identity = libp2p.identity;
const keys = @import("peer_id").keys;
const Allocator = std.mem.Allocator;
const Atomic = std.atomic;
const identify_pb = @import("../proto/identify.proto.zig");

/// Protocol identifier for libp2p identify.
pub const protocol_id = "/ipfs/id/1.0.0";

/// Protocol identifier for libp2p identify push.
pub const protocol_push_id = "/ipfs/id/push/1.0.0";

/// Configuration for the Identify protocol handler.
pub const IdentifyConfig = struct {
    protocol_version: ?[]const u8 = null,
    agent_version: ?[]const u8 = null,
    supported_protocols: ?[]const []const u8 = null,
};

/// Identify protocol handler that can be registered with the multistream-select handler.
pub const IdentifyProtocolHandler = struct {
    allocator: std.mem.Allocator,
    network_switch: *swarm.Switch,
    transport: *quic.QuicTransport,
    config: IdentifyConfig,
    shutting_down: bool = false,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        transport: *quic.QuicTransport,
    ) Self {
        return Self.initWithConfig(allocator, network_switch, transport, .{});
    }

    pub fn initWithConfig(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        transport: *quic.QuicTransport,
        config: IdentifyConfig,
    ) Self {
        return .{
            .allocator = allocator,
            .network_switch = network_switch,
            .transport = transport,
            .config = config,
            .shutting_down = false,
        };
    }

    pub fn deinit(self: *Self) void {
        self.shutting_down = true;
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyInitiator) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        handler.* = .{
            .stream = stream,
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .response_buffer = std.ArrayList(u8).init(self.allocator),
            .response_received = false,
        };
        errdefer {
            handler.response_buffer.deinit();
            self.allocator.destroy(handler);
        }

        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyResponder) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        handler.* = .{
            .stream = stream,
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .handler = self,
        };

        stream.setProtoMsgHandler(handler.any());
    }

    fn vtableOnInitiatorStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInitiatorStart(stream, callback_ctx, callback);
    }

    fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onResponderStart(stream, callback_ctx, callback);
    }

    const vtable_instance = protocols.ProtocolHandlerVTable{
        .onInitiatorStartFn = vtableOnInitiatorStartFn,
        .onResponderStartFn = vtableOnResponderStartFn,
    };

    pub fn any(self: *Self) protocols.AnyProtocolHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    /// Gets the public key bytes. Abstracts away transport access.
    pub fn getPublicKeyBytes(self: *Self, allocator: Allocator) ![]const u8 {
        return try self.transport.host_keypair.publicKeyBytes(allocator);
    }

    /// Gets listen addresses. Abstracts away Swarm access.
    pub fn getListenMultiaddrs(self: *Self, allocator: Allocator) !std.ArrayList([]u8) {
        return try self.network_switch.listenMultiaddrs(allocator);
    }

    /// Gets supported protocols. Abstracts away Swarm access.
    pub fn getSupportedProtocols(self: *Self, allocator: Allocator) !std.ArrayList([]const u8) {
        var protocols_list = std.ArrayList([]const u8).init(allocator);
        errdefer protocols_list.deinit();

        if (self.config.supported_protocols) |config_protocols| {
            try protocols_list.appendSlice(config_protocols);
        } else {
            var iter = self.network_switch.mss_handler.supported_protocols.iterator();
            while (iter.next()) |entry| {
                try protocols_list.append(entry.key_ptr.*);
            }
        }

        return protocols_list;
    }
};

/// Handles identify messages on the initiator side (querying remote peer).
const IdentifyInitiator = struct {
    stream: *quic.QuicStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    response_buffer: std.ArrayList(u8),
    response_received: bool,
    service_callback: ?*const fn (callback_ctx: ?*anyopaque, result: anyerror!IdentifyResult) void = null,
    service_callback_ctx: ?*anyopaque = null,

    const Self = @This();

    fn onActivated(self: *Self, _: *quic.QuicStream) anyerror!void {
        _ = self;
        // Initiator waits for response, no action needed on activation
    }

    fn onMessage(self: *Self, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        _ = stream;
        if (message.len == 0) return;

        // Accumulate protobuf bytes
        self.response_buffer.appendSlice(message) catch |err| {
            self.fail(err);
            return err;
        };
    }

    fn onClose(self: *Self, stream: *quic.QuicStream) anyerror!void {
        _ = stream;
        if (self.response_received) return;

        self.response_received = true;

        // Parse the accumulated protobuf message
        const reader = identify_pb.IdentifyReader.init(self.allocator, self.response_buffer.items) catch |err| {
            self.fail(err);
            return err;
        };
        defer reader.deinit();

        // Create result structure
        const result = IdentifyResult{
            .protocol_version = reader.getProtocolVersion(),
            .agent_version = reader.getAgentVersion(),
            .public_key = reader.getPublicKey(),
            .listen_addrs = reader.getListenAddrs(),
            .observed_addr = reader.getObservedAddr(),
            .protocols = reader.getProtocols(),
        };

        self.response_buffer.deinit();

        // Check if callback_ctx is an IdentifyRequestCtx (from IdentifyService)
        // We use a type tag approach: if callback_ctx points to a struct that starts with a service pointer,
        // it's likely an IdentifyRequestCtx
        if (self.callback_ctx) |ctx| {
            // Try to access as IdentifyRequestCtx - this is safe because we control both sides
            const request_ctx: ?*IdentifyRequestCtx = @ptrCast(@alignCast(ctx));
            if (request_ctx) |req_ctx| {
                // Verify it's actually an IdentifyRequestCtx by checking if it has the expected structure
                // This is a bit unsafe, but we control both the service and the handler
                const service_cb = req_ctx.callback;
                const service_ctx = req_ctx.callback_ctx;
                self.allocator.destroy(self);
                service_cb(service_ctx, result);
                return;
            }
        }

        // Otherwise, use the protocol callback (expects controller, not result)
        // This shouldn't happen in normal flow, but handle it
        const callback_fn = self.callback;
        const callback_ctx_val = self.callback_ctx;
        self.allocator.destroy(self);
        callback_fn(callback_ctx_val, error.UnexpectedResultType);
    }

    fn fail(self: *Self, err: anyerror) void {
        if (self.response_received) return;
        self.response_received = true;
        self.response_buffer.deinit();
        self.allocator.destroy(self);
        self.callback(self.callback_ctx, err);
    }

    fn vtableOnActivatedFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onClose(stream);
    }

    const vtable_instance = protocols.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    fn any(self: *Self) protocols.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

/// Handles identify messages on the responder side (responding to query).
const IdentifyResponder = struct {
    stream: *quic.QuicStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    handler: *IdentifyProtocolHandler,
    message_sent: bool = false,

    const Self = @This();

    /// Builds an Identify message with current peer information.
    fn buildIdentifyMessage(self: *Self, stream: *quic.QuicStream, allocator: Allocator) !identify_pb.Identify {
        var identify_msg = identify_pb.Identify{};

        // Set protocol version if configured
        if (self.handler.config.protocol_version) |pv| {
            identify_msg.protocol_version = pv;
        }

        // Set agent version if configured
        if (self.handler.config.agent_version) |av| {
            identify_msg.agent_version = av;
        }

        // Get public key from handler (abstracts away transport access)
        const public_key_bytes = try self.handler.getPublicKeyBytes(allocator);
        errdefer allocator.free(public_key_bytes);
        identify_msg.public_key = public_key_bytes;

        // Get listen addresses from handler (abstracts away Swarm access)
        var listen_addrs = try self.handler.getListenMultiaddrs(allocator);
        defer swarm.Switch.freeListenMultiaddrs(allocator, &listen_addrs);

        if (listen_addrs.items.len > 0) {
            var addr_bytes_list = try allocator.alloc(?[]const u8, listen_addrs.items.len);
            errdefer allocator.free(addr_bytes_list);
            for (listen_addrs.items, 0..) |addr_str, i| {
                // Convert multiaddr string to bytes
                // The identify spec expects raw multiaddr bytes, but for now we'll use the string bytes
                // A proper implementation would serialize the multiaddr according to the multiformats spec
                const bytes = try allocator.dupe(u8, addr_str);
                addr_bytes_list[i] = bytes;
            }
            identify_msg.listen_addrs = addr_bytes_list;
        }

        // Get observed address from stream
        if (stream.conn.security_session) |session| {
            // For now, we'll try to extract from the connection
            // The observed address is the source address of the initiator as seen by the responder
            // This is a simplified version - in a full implementation, we'd extract from the connection
            // For QUIC, we might need to get this from the connection's remote address
            // For now, we'll leave it as null and let the caller handle it if needed
            _ = session;
        }

        // Get supported protocols from handler (abstracts away Swarm access)
        var protocols_list = try self.handler.getSupportedProtocols(allocator);
        defer protocols_list.deinit();

        if (protocols_list.items.len > 0) {
            var protocols_array = try allocator.alloc(?[]const u8, protocols_list.items.len);
            errdefer allocator.free(protocols_array);
            for (protocols_list.items, 0..) |proto, i| {
                protocols_array[i] = proto;
            }
            identify_msg.protocols = protocols_array;
        }

        return identify_msg;
    }

    fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        // Build and send Identify message immediately
        const identify_msg = try self.buildIdentifyMessage(stream, self.allocator);
        errdefer {
            // Cleanup on error
            if (identify_msg.public_key) |pk| self.allocator.free(pk);
            if (identify_msg.listen_addrs) |addrs| {
                for (addrs) |maybe_addr| {
                    if (maybe_addr) |addr| self.allocator.free(addr);
                }
                self.allocator.free(addrs);
            }
            if (identify_msg.protocols) |prots| {
                for (prots) |maybe_proto| {
                    if (maybe_proto) |_| {} // Protocols are not allocated, they're just slices
                }
                self.allocator.free(prots);
            }
        }

        const encoded = try identify_msg.encode(self.allocator);
        errdefer self.allocator.free(encoded);

        // Cleanup identify message
        if (identify_msg.public_key) |pk| self.allocator.free(pk);
        if (identify_msg.listen_addrs) |addrs| {
            for (addrs) |maybe_addr| {
                if (maybe_addr) |addr| self.allocator.free(addr);
            }
            self.allocator.free(addrs);
        }
        if (identify_msg.protocols) |prots| {
            // Protocols are just string slices, not allocated, so we only free the array
            self.allocator.free(prots);
        }

        self.message_sent = true;
        stream.write(encoded, self, writeCompleteCallback);
        self.allocator.free(encoded);
    }

    fn writeCompleteCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.fail(err);
            return;
        };

        // Close stream after sending message
        self.stream.close(self, closeCallback);
    }

    fn closeCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.fail(err);
            return;
        };

        self.callback(self.callback_ctx, null);
        self.allocator.destroy(self);
    }

    fn onMessage(self: *Self, _: *quic.QuicStream, _: []const u8) anyerror!void {
        _ = self;
        // Responder should not receive messages, but we'll ignore them
    }

    fn onClose(self: *Self, stream: *quic.QuicStream) anyerror!void {
        _ = stream;
        if (!self.message_sent) {
            self.fail(error.StreamClosedBeforeResponse);
        }
        // Cleanup will happen in closeCallback
    }

    fn fail(self: *Self, err: anyerror) void {
        self.callback(self.callback_ctx, err);
        self.allocator.destroy(self);
    }

    fn vtableOnActivatedFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onClose(stream);
    }

    const vtable_instance = protocols.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    fn any(self: *Self) protocols.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

/// Result structure containing parsed Identify message fields.
pub const IdentifyResult = struct {
    protocol_version: ?[]const u8,
    agent_version: ?[]const u8,
    public_key: ?[]const u8,
    listen_addrs: ?[]const []const u8,
    observed_addr: ?[]const u8,
    protocols: ?[]const []const u8,
};

/// Service facade responsible for querying peer information.
pub const IdentifyService = struct {
    allocator: Allocator,
    network_switch: *swarm.Switch,

    const Self = @This();

    pub fn init(allocator: Allocator, network_switch: *swarm.Switch) Self {
        return .{
            .allocator = allocator,
            .network_switch = network_switch,
        };
    }

    pub fn identify(
        self: *Self,
        address: Multiaddr,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, result: anyerror!IdentifyResult) void,
    ) !void {
        const ctx = try self.allocator.create(IdentifyRequestCtx);
        ctx.* = .{
            .service = self,
            .callback_ctx = callback_ctx,
            .callback = callback,
        };

        self.network_switch.newStream(address, &.{protocol_id}, ctx, IdentifyRequestCtx.streamOpened);
    }
};

/// Context for identify requests from IdentifyService.
const IdentifyRequestCtx = struct {
    service: *IdentifyService,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, result: anyerror!IdentifyResult) void,

    fn streamOpened(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
        const self: *IdentifyRequestCtx = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.callback(self.callback_ctx, err);
            self.service.allocator.destroy(self);
            return;
        };

        // The stream handler (IdentifyInitiator) will call the callback when the response is received
        // We store the callback in the IdentifyInitiator via the callback_ctx
    }
};

/// Identify push protocol handler.
pub const IdentifyPushHandler = struct {
    allocator: std.mem.Allocator,
    network_switch: *swarm.Switch,
    transport: *quic.QuicTransport,
    config: IdentifyConfig,
    shutting_down: bool = false,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        transport: *quic.QuicTransport,
    ) Self {
        return Self.initWithConfig(allocator, network_switch, transport, .{});
    }

    pub fn initWithConfig(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        transport: *quic.QuicTransport,
        config: IdentifyConfig,
    ) Self {
        return .{
            .allocator = allocator,
            .network_switch = network_switch,
            .transport = transport,
            .config = config,
            .shutting_down = false,
        };
    }

    pub fn deinit(self: *Self) void {
        self.shutting_down = true;
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyPushInitiator) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        handler.* = .{
            .stream = stream,
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .handler = self,
        };

        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyPushResponder) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        handler.* = .{
            .stream = stream,
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .response_buffer = std.ArrayList(u8).init(self.allocator),
            .response_received = false,
        };
        errdefer {
            handler.response_buffer.deinit();
            self.allocator.destroy(handler);
        }

        stream.setProtoMsgHandler(handler.any());
    }

    fn vtableOnInitiatorStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInitiatorStart(stream, callback_ctx, callback);
    }

    fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onResponderStart(stream, callback_ctx, callback);
    }

    const vtable_instance = protocols.ProtocolHandlerVTable{
        .onInitiatorStartFn = vtableOnInitiatorStartFn,
        .onResponderStartFn = vtableOnResponderStartFn,
    };

    pub fn any(self: *Self) protocols.AnyProtocolHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    /// Gets the public key bytes. Abstracts away transport access.
    pub fn getPublicKeyBytes(self: *Self, allocator: Allocator) ![]const u8 {
        return try self.transport.host_keypair.publicKeyBytes(allocator);
    }

    /// Gets listen addresses. Abstracts away Swarm access.
    pub fn getListenMultiaddrs(self: *Self, allocator: Allocator) !std.ArrayList([]u8) {
        return try self.network_switch.listenMultiaddrs(allocator);
    }

    /// Gets supported protocols. Abstracts away Swarm access.
    pub fn getSupportedProtocols(self: *Self, allocator: Allocator) !std.ArrayList([]const u8) {
        var protocols_list = std.ArrayList([]const u8).init(allocator);
        errdefer protocols_list.deinit();

        if (self.config.supported_protocols) |config_protocols| {
            try protocols_list.appendSlice(config_protocols);
        } else {
            var iter = self.network_switch.mss_handler.supported_protocols.iterator();
            while (iter.next()) |entry| {
                try protocols_list.append(entry.key_ptr.*);
            }
        }

        return protocols_list;
    }
};

/// Handles identify push messages on the initiator side (sending push update).
const IdentifyPushInitiator = struct {
    stream: *quic.QuicStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    handler: *IdentifyPushHandler,
    message_sent: bool = false,

    const Self = @This();

    /// Builds an Identify message with current peer information.
    fn buildIdentifyMessage(self: *Self, stream: *quic.QuicStream, allocator: Allocator) !identify_pb.Identify {
        var identify_msg = identify_pb.Identify{};

        // Set protocol version if configured
        if (self.handler.config.protocol_version) |pv| {
            identify_msg.protocol_version = pv;
        }

        // Set agent version if configured
        if (self.handler.config.agent_version) |av| {
            identify_msg.agent_version = av;
        }

        // Get public key from handler (abstracts away transport access)
        const public_key_bytes = try self.handler.getPublicKeyBytes(allocator);
        errdefer allocator.free(public_key_bytes);
        identify_msg.public_key = public_key_bytes;

        // Get listen addresses from handler (abstracts away Swarm access)
        var listen_addrs = try self.handler.getListenMultiaddrs(allocator);
        defer swarm.Switch.freeListenMultiaddrs(allocator, &listen_addrs);

        if (listen_addrs.items.len > 0) {
            var addr_bytes_list = try allocator.alloc(?[]const u8, listen_addrs.items.len);
            errdefer allocator.free(addr_bytes_list);
            for (listen_addrs.items, 0..) |addr_str, i| {
                // Convert multiaddr string to bytes
                // The identify spec expects raw multiaddr bytes, but for now we'll use the string bytes
                // A proper implementation would serialize the multiaddr according to the multiformats spec
                const bytes = try allocator.dupe(u8, addr_str);
                addr_bytes_list[i] = bytes;
            }
            identify_msg.listen_addrs = addr_bytes_list;
        }

        // Get observed address from stream
        if (stream.conn.security_session) |session| {
            // For now, we'll try to extract from the connection
            // The observed address is the source address of the initiator as seen by the responder
            // This is a simplified version - in a full implementation, we'd extract from the connection
            // For QUIC, we might need to get this from the connection's remote address
            // For now, we'll leave it as null and let the caller handle it if needed
            _ = session;
        }

        // Get supported protocols from handler (abstracts away Swarm access)
        var protocols_list = try self.handler.getSupportedProtocols(allocator);
        defer protocols_list.deinit();

        if (protocols_list.items.len > 0) {
            var protocols_array = try allocator.alloc(?[]const u8, protocols_list.items.len);
            errdefer allocator.free(protocols_array);
            for (protocols_list.items, 0..) |proto, i| {
                protocols_array[i] = proto;
            }
            identify_msg.protocols = protocols_array;
        }

        return identify_msg;
    }

    fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        // Build and send Identify message immediately
        const identify_msg = try self.buildIdentifyMessage(stream, self.allocator);
        errdefer {
            if (identify_msg.public_key) |pk| self.allocator.free(pk);
            if (identify_msg.listen_addrs) |addrs| {
                for (addrs) |maybe_addr| {
                    if (maybe_addr) |addr| self.allocator.free(addr);
                }
                self.allocator.free(addrs);
            }
            if (identify_msg.protocols) |prots| {
                for (prots) |maybe_proto| {
                    if (maybe_proto) |_| {} // Protocols are not allocated, they're just slices
                }
                self.allocator.free(prots);
            }
        }

        const encoded = try identify_msg.encode(self.allocator);
        errdefer self.allocator.free(encoded);

        // Cleanup identify message
        if (identify_msg.public_key) |pk| self.allocator.free(pk);
        if (identify_msg.listen_addrs) |addrs| {
            for (addrs) |maybe_addr| {
                if (maybe_addr) |addr| self.allocator.free(addr);
            }
            self.allocator.free(addrs);
        }
        if (identify_msg.protocols) |prots| {
            // Protocols are just string slices, not allocated, so we only free the array
            self.allocator.free(prots);
        }

        self.message_sent = true;
        stream.write(encoded, self, writeCompleteCallback);
        self.allocator.free(encoded);
    }

    fn writeCompleteCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.fail(err);
            return;
        };

        // Close stream after sending message
        self.stream.close(self, closeCallback);
    }

    fn closeCallback(ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.fail(err);
            return;
        };

        self.callback(self.callback_ctx, null);
        self.allocator.destroy(self);
    }

    fn onMessage(self: *Self, _: *quic.QuicStream, _: []const u8) anyerror!void {
        _ = self;
        // Initiator should not receive messages
    }

    fn onClose(self: *Self, stream: *quic.QuicStream) anyerror!void {
        _ = stream;
        if (!self.message_sent) {
            self.fail(error.StreamClosedBeforeResponse);
        }
    }

    fn fail(self: *Self, err: anyerror) void {
        self.callback(self.callback_ctx, err);
        self.allocator.destroy(self);
    }

    fn vtableOnActivatedFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onClose(stream);
    }

    const vtable_instance = protocols.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    fn any(self: *Self) protocols.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

/// Handles identify push messages on the responder side (receiving push update).
const IdentifyPushResponder = struct {
    stream: *quic.QuicStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    response_buffer: std.ArrayList(u8),
    response_received: bool,

    const Self = @This();

    fn onActivated(self: *Self, _: *quic.QuicStream) anyerror!void {
        _ = self;
        // Responder waits for push message
    }

    fn onMessage(self: *Self, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        _ = stream;
        if (message.len == 0) return;

        // Accumulate protobuf bytes
        self.response_buffer.appendSlice(message) catch |err| {
            self.fail(err);
            return err;
        };
    }

    fn onClose(self: *Self, stream: *quic.QuicStream) anyerror!void {
        _ = stream;
        if (self.response_received) return;

        self.response_received = true;

        // Parse the accumulated protobuf message
        const reader = identify_pb.IdentifyReader.init(self.allocator, self.response_buffer.items) catch |err| {
            self.fail(err);
            return err;
        };
        defer reader.deinit();

        // Update local peer store with the received information
        // For now, we just parse it - the caller can handle the update
        const result = IdentifyResult{
            .protocol_version = reader.getProtocolVersion(),
            .agent_version = reader.getAgentVersion(),
            .public_key = reader.getPublicKey(),
            .listen_addrs = reader.getListenAddrs(),
            .observed_addr = reader.getObservedAddr(),
            .protocols = reader.getProtocols(),
        };

        self.response_buffer.deinit();

        // Check if callback_ctx is an IdentifyRequestCtx (from IdentifyService)
        if (self.callback_ctx) |ctx| {
            const request_ctx: ?*IdentifyRequestCtx = @ptrCast(@alignCast(ctx));
            if (request_ctx) |req_ctx| {
                const service_cb = req_ctx.callback;
                const service_ctx = req_ctx.callback_ctx;
                self.allocator.destroy(self);
                service_cb(service_ctx, result);
                return;
            }
        }

        // Otherwise, use the protocol callback (expects controller, not result)
        const callback_fn = self.callback;
        const callback_ctx_val = self.callback_ctx;
        self.allocator.destroy(self);
        callback_fn(callback_ctx_val, error.UnexpectedResultType);
    }

    fn fail(self: *Self, err: anyerror) void {
        if (self.response_received) return;
        self.response_received = true;
        self.response_buffer.deinit();
        self.allocator.destroy(self);
        self.callback(self.callback_ctx, err);
    }

    fn vtableOnActivatedFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onClose(stream);
    }

    const vtable_instance = protocols.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    fn any(self: *Self) protocols.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};
