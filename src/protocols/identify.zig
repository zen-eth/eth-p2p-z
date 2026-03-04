const std = @import("std");
const libp2p = @import("../root.zig");
const protocols = libp2p.protocols;
const quic = libp2p.transport.quic;
const swarm = libp2p.swarm;
const multiaddr = @import("multiaddr");
const Multiaddr = multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;
const identity = libp2p.identity;
const keys = @import("peer_id").keys;
const Allocator = std.mem.Allocator;
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
    config: IdentifyConfig,
    shutting_down: bool = false,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
    ) Self {
        return Self.initWithConfig(allocator, network_switch, .{});
    }

    pub fn initWithConfig(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        config: IdentifyConfig,
    ) Self {
        return .{
            .allocator = allocator,
            .network_switch = network_switch,
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
            .response_buffer = .empty,
            .response_received = false,
        };
        errdefer {
            handler.response_buffer.deinit(self.allocator);
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

    /// Gets the public key bytes from the network switch's transport.
    pub fn getPublicKeyBytes(self: *Self, allocator: Allocator) ![]const u8 {
        return try self.network_switch.transport.host_keypair.publicKeyBytes(allocator);
    }

    /// Gets listen addresses from the network switch.
    pub fn getListenMultiaddrs(self: *Self, allocator: Allocator) !std.ArrayList([]u8) {
        return try self.network_switch.listenMultiaddrs(allocator);
    }

    /// Gets supported protocols from the network switch.
    pub fn getSupportedProtocols(self: *Self, allocator: Allocator) !std.ArrayList([]const u8) {
        var protocols_list: std.ArrayList([]const u8) = .empty;
        errdefer protocols_list.deinit(allocator);

        if (self.config.supported_protocols) |config_protocols| {
            try protocols_list.appendSlice(allocator, config_protocols);
        } else {
            var iter = self.network_switch.mss_handler.supported_protocols.iterator();
            while (iter.next()) |entry| {
                try protocols_list.append(allocator, entry.key_ptr.*);
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

    const Self = @This();

    fn onActivated(self: *Self, _: *quic.QuicStream) anyerror!void {
        _ = self;
        // Initiator waits for response, no action needed on activation
    }

    fn onMessage(self: *Self, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        _ = stream;
        if (message.len == 0) return;

        // Accumulate protobuf bytes
        self.response_buffer.appendSlice(self.allocator, message) catch |err| {
            self.fail(err);
            return err;
        };
    }

    fn onClose(self: *Self, stream: *quic.QuicStream) anyerror!void {
        _ = stream;
        if (self.response_received) return;

        self.response_received = true;

        // Parse the accumulated protobuf message
        // Note: After setting response_received = true, we cannot call fail() as it
        // will return early. Handle errors inline with deliverErrorAndDestroy.
        var reader = identify_pb.IdentifyReader.init(self.response_buffer.items) catch |err| {
            self.deliverErrorAndDestroy(err);
            return err;
        };

        // Collect listen addresses from iterator
        var listen_addrs: ?[]const []const u8 = null;
        const listen_addrs_count = reader.listenAddrsCount();
        if (listen_addrs_count > 0) {
            var addrs = self.allocator.alloc([]const u8, listen_addrs_count) catch |err| {
                self.deliverErrorAndDestroy(err);
                return err;
            };
            var i: usize = 0;
            while (reader.listenAddrsNext()) |addr| {
                addrs[i] = addr;
                i += 1;
            }
            listen_addrs = addrs;
        }

        // Collect protocols from iterator
        var protocols_list: ?[]const []const u8 = null;
        const protocols_count = reader.protocolsCount();
        if (protocols_count > 0) {
            var prots = self.allocator.alloc([]const u8, protocols_count) catch |err| {
                if (listen_addrs) |addrs| self.allocator.free(addrs);
                self.deliverErrorAndDestroy(err);
                return err;
            };
            var j: usize = 0;
            while (reader.protocolsNext()) |proto| {
                prots[j] = proto;
                j += 1;
            }
            protocols_list = prots;
        }

        // Create result structure
        const result = IdentifyResult{
            .protocol_version = reader.getProtocolVersion(),
            .agent_version = reader.getAgentVersion(),
            .public_key = reader.getPublicKey(),
            .listen_addrs = listen_addrs,
            .observed_addr = reader.getObservedAddr(),
            .protocols = protocols_list,
        };

        self.response_buffer.deinit(self.allocator);

        // Check if callback_ctx is an IdentifyRequestCtx (from IdentifyService)
        // using the type tag for safe identification
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverResult(result);
            return;
        }

        // Direct protocol usage (not via IdentifyService) - this is an error
        // because the protocol callback signature doesn't support IdentifyResult
        const callback_fn = self.callback;
        const callback_ctx_val = self.callback_ctx;
        self.allocator.destroy(self);
        callback_fn(callback_ctx_val, error.UnexpectedResultType);
    }

    /// Delivers an error and destroys self. Used in onClose after response_received
    /// is set, where calling fail() would return early without cleanup.
    fn deliverErrorAndDestroy(self: *Self, err: anyerror) void {
        self.response_buffer.deinit(self.allocator);

        // Check if callback_ctx is an IdentifyRequestCtx
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverError(err);
            return;
        }

        // Direct protocol usage - use protocol callback
        const alloc = self.allocator;
        const cb = self.callback;
        const cb_ctx = self.callback_ctx;
        alloc.destroy(self);
        cb(cb_ctx, err);
    }

    fn fail(self: *Self, err: anyerror) void {
        if (self.response_received) return;
        self.response_received = true;
        self.response_buffer.deinit(self.allocator);

        // Check if callback_ctx is an IdentifyRequestCtx
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverError(err);
            return;
        }

        // Direct protocol usage - use protocol callback
        const alloc = self.allocator;
        const cb = self.callback;
        const cb_ctx = self.callback_ctx;
        alloc.destroy(self);
        cb(cb_ctx, err);
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
    encoded_message: ?[]const u8 = null, // Stored until async write completes

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

        // TODO: Extract observed_addr from QUIC connection's remote address.
        // The observed address should be the initiator's source address as seen by the responder.
        // For now, we leave it as null - identify_msg.observed_addr defaults to null.
        _ = stream.conn.security_session;

        // Get supported protocols from handler (abstracts away Swarm access)
        var protocols_list = try self.handler.getSupportedProtocols(allocator);
        defer protocols_list.deinit(allocator);

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
        self.encoded_message = encoded; // Store until write completes
        stream.write(encoded, self, writeCompleteCallback);
    }

    fn writeCompleteCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));

        // Free encoded message now that write is complete
        if (self.encoded_message) |msg| {
            self.allocator.free(msg);
            self.encoded_message = null;
        }

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
        // Free encoded message if still pending
        if (self.encoded_message) |msg| {
            self.allocator.free(msg);
        }
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
/// Uses a magic type tag for safe identification during callback dispatch.
const IdentifyRequestCtx = struct {
    /// Magic value to identify this struct type safely
    const TYPE_TAG: u64 = 0x4944454E54494659; // "IDENTIFY" in hex

    type_tag: u64 = TYPE_TAG,
    service: *IdentifyService,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, result: anyerror!IdentifyResult) void,

    /// Check if a pointer points to a valid IdentifyRequestCtx
    fn isValidCtx(ptr: ?*anyopaque) bool {
        if (ptr == null) return false;
        const maybe_ctx: *const IdentifyRequestCtx = @ptrCast(@alignCast(ptr.?));
        return maybe_ctx.type_tag == TYPE_TAG;
    }

    fn streamOpened(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
        const self: *IdentifyRequestCtx = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.callback(self.callback_ctx, err);
            self.service.allocator.destroy(self);
            return;
        };

        // Success: The IdentifyInitiator will handle the response and call our callback.
        // We do NOT destroy self here - IdentifyInitiator.onClose will destroy it after
        // calling the callback. The callback_ctx passed to onInitiatorStart is `self`.
    }

    /// Called by IdentifyInitiator.onClose to deliver the result and cleanup
    fn deliverResult(self: *IdentifyRequestCtx, result: IdentifyResult) void {
        const cb = self.callback;
        const cb_ctx = self.callback_ctx;
        const allocator = self.service.allocator;
        allocator.destroy(self);
        cb(cb_ctx, result);
    }

    /// Called by IdentifyInitiator.fail to deliver an error and cleanup
    fn deliverError(self: *IdentifyRequestCtx, err: anyerror) void {
        const cb = self.callback;
        const cb_ctx = self.callback_ctx;
        const allocator = self.service.allocator;
        allocator.destroy(self);
        cb(cb_ctx, err);
    }
};

/// Identify push protocol handler.
pub const IdentifyPushHandler = struct {
    allocator: std.mem.Allocator,
    network_switch: *swarm.Switch,
    config: IdentifyConfig,
    shutting_down: bool = false,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
    ) Self {
        return Self.initWithConfig(allocator, network_switch, .{});
    }

    pub fn initWithConfig(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        config: IdentifyConfig,
    ) Self {
        return .{
            .allocator = allocator,
            .network_switch = network_switch,
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
            .response_buffer = .empty,
            .response_received = false,
        };
        errdefer {
            handler.response_buffer.deinit(self.allocator);
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

    /// Gets the public key bytes from the network switch's transport.
    pub fn getPublicKeyBytes(self: *Self, allocator: Allocator) ![]const u8 {
        return try self.network_switch.transport.host_keypair.publicKeyBytes(allocator);
    }

    /// Gets listen addresses from the network switch.
    pub fn getListenMultiaddrs(self: *Self, allocator: Allocator) !std.ArrayList([]u8) {
        return try self.network_switch.listenMultiaddrs(allocator);
    }

    /// Gets supported protocols from the network switch.
    pub fn getSupportedProtocols(self: *Self, allocator: Allocator) !std.ArrayList([]const u8) {
        var protocols_list: std.ArrayList([]const u8) = .empty;
        errdefer protocols_list.deinit(allocator);

        if (self.config.supported_protocols) |config_protocols| {
            try protocols_list.appendSlice(allocator, config_protocols);
        } else {
            var iter = self.network_switch.mss_handler.supported_protocols.iterator();
            while (iter.next()) |entry| {
                try protocols_list.append(allocator, entry.key_ptr.*);
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
    encoded_message: ?[]const u8 = null, // Stored until async write completes

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

        // TODO: Extract observed_addr from QUIC connection's remote address.
        // The observed address should be the initiator's source address as seen by the responder.
        // For now, we leave it as null - identify_msg.observed_addr defaults to null.
        _ = stream.conn.security_session;

        // Get supported protocols from handler (abstracts away Swarm access)
        var protocols_list = try self.handler.getSupportedProtocols(allocator);
        defer protocols_list.deinit(allocator);

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
        self.encoded_message = encoded; // Store until write completes
        stream.write(encoded, self, writeCompleteCallback);
    }

    fn writeCompleteCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));

        // Free encoded message now that write is complete
        if (self.encoded_message) |msg| {
            self.allocator.free(msg);
            self.encoded_message = null;
        }

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
        // Free encoded message if still pending
        if (self.encoded_message) |msg| {
            self.allocator.free(msg);
        }
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
        self.response_buffer.appendSlice(self.allocator, message) catch |err| {
            self.fail(err);
            return err;
        };
    }

    fn onClose(self: *Self, stream: *quic.QuicStream) anyerror!void {
        _ = stream;
        if (self.response_received) return;

        self.response_received = true;

        // Parse the accumulated protobuf message
        // Note: After setting response_received = true, we cannot call fail() as it
        // will return early. Handle errors inline with deliverErrorAndDestroy.
        var reader = identify_pb.IdentifyReader.init(self.response_buffer.items) catch |err| {
            self.deliverErrorAndDestroy(err);
            return err;
        };

        // Collect listen addresses from iterator
        var listen_addrs: ?[]const []const u8 = null;
        const listen_addrs_count = reader.listenAddrsCount();
        if (listen_addrs_count > 0) {
            var addrs = self.allocator.alloc([]const u8, listen_addrs_count) catch |err| {
                self.deliverErrorAndDestroy(err);
                return err;
            };
            var i: usize = 0;
            while (reader.listenAddrsNext()) |addr| {
                addrs[i] = addr;
                i += 1;
            }
            listen_addrs = addrs;
        }

        // Collect protocols from iterator
        var protocols_list: ?[]const []const u8 = null;
        const protocols_count = reader.protocolsCount();
        if (protocols_count > 0) {
            var prots = self.allocator.alloc([]const u8, protocols_count) catch |err| {
                if (listen_addrs) |addrs| self.allocator.free(addrs);
                self.deliverErrorAndDestroy(err);
                return err;
            };
            var j: usize = 0;
            while (reader.protocolsNext()) |proto| {
                prots[j] = proto;
                j += 1;
            }
            protocols_list = prots;
        }

        // Update local peer store with the received information
        // For now, we just parse it - the caller can handle the update
        const result = IdentifyResult{
            .protocol_version = reader.getProtocolVersion(),
            .agent_version = reader.getAgentVersion(),
            .public_key = reader.getPublicKey(),
            .listen_addrs = listen_addrs,
            .observed_addr = reader.getObservedAddr(),
            .protocols = protocols_list,
        };

        self.response_buffer.deinit(self.allocator);

        // Check if callback_ctx is an IdentifyRequestCtx (from IdentifyService)
        // using the type tag for safe identification
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverResult(result);
            return;
        }

        // Otherwise, use the protocol callback (expects controller, not result)
        const callback_fn = self.callback;
        const callback_ctx_val = self.callback_ctx;
        self.allocator.destroy(self);
        callback_fn(callback_ctx_val, error.UnexpectedResultType);
    }

    /// Delivers an error and destroys self. Used in onClose after response_received
    /// is set, where calling fail() would return early without cleanup.
    fn deliverErrorAndDestroy(self: *Self, err: anyerror) void {
        self.response_buffer.deinit(self.allocator);

        // Check if callback_ctx is an IdentifyRequestCtx
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverError(err);
            return;
        }

        // Direct protocol usage - use protocol callback
        const alloc = self.allocator;
        const cb = self.callback;
        const cb_ctx = self.callback_ctx;
        alloc.destroy(self);
        cb(cb_ctx, err);
    }

    fn fail(self: *Self, err: anyerror) void {
        if (self.response_received) return;
        self.response_received = true;
        self.response_buffer.deinit(self.allocator);

        // Check if callback_ctx is an IdentifyRequestCtx
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverError(err);
            return;
        }

        // Direct protocol usage - use protocol callback
        const alloc = self.allocator;
        const cb = self.callback;
        const cb_ctx = self.callback_ctx;
        alloc.destroy(self);
        cb(cb_ctx, err);
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

// ============================================================================
// Tests
// ============================================================================

const io_loop = libp2p.thread_event_loop;

/// Shared test context for setting up server/client pairs (integration tests).
const TestContext = struct {
    allocator: Allocator,
    server_loop: io_loop.ThreadEventLoop,
    server_key: identity.KeyPair,
    server_transport: libp2p.transport.quic.QuicTransport,
    server_switch: swarm.Switch,
    server_peer_id: PeerId,
    client_loop: io_loop.ThreadEventLoop,
    client_key: identity.KeyPair,
    client_transport: libp2p.transport.quic.QuicTransport,
    client_switch: swarm.Switch,

    const Self = @This();

    fn init(allocator: Allocator) !Self {
        var ctx: Self = undefined;
        ctx.allocator = allocator;

        // Server setup
        ctx.server_loop = undefined;
        try ctx.server_loop.init(allocator);
        errdefer ctx.server_loop.deinit();

        ctx.server_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
        errdefer ctx.server_key.deinit();

        ctx.server_transport = undefined;
        try ctx.server_transport.init(&ctx.server_loop, &ctx.server_key, keys.KeyType.ED25519, allocator);

        ctx.server_switch = undefined;
        ctx.server_switch.init(allocator, &ctx.server_transport);

        var server_pubkey = try ctx.server_key.publicKey(allocator);
        defer allocator.free(server_pubkey.data.?);
        ctx.server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

        // Client setup
        ctx.client_loop = undefined;
        try ctx.client_loop.init(allocator);
        errdefer ctx.client_loop.deinit();

        ctx.client_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
        errdefer ctx.client_key.deinit();

        ctx.client_transport = undefined;
        try ctx.client_transport.init(&ctx.client_loop, &ctx.client_key, keys.KeyType.ED25519, allocator);

        ctx.client_switch = undefined;
        ctx.client_switch.init(allocator, &ctx.client_transport);

        return ctx;
    }

    fn deinit(self: *Self) void {
        self.client_switch.stop();
        self.client_switch.deinit();
        self.client_key.deinit();
        self.client_loop.close();
        self.client_loop.deinit();

        self.server_switch.stop();
        self.server_switch.deinit();
        self.server_key.deinit();
        self.server_loop.close();
        self.server_loop.deinit();
    }

    /// Start server listening. Socket bind is synchronous, so listener is ready
    /// immediately after this returns without error.
    fn listenServer(self: *Self, port: u16) !Multiaddr {
        var addr_buf: [64]u8 = undefined;
        const addr_str = std.fmt.bufPrint(&addr_buf, "/ip4/0.0.0.0/udp/{d}", .{port}) catch unreachable;
        const listen_addr = try Multiaddr.fromString(self.allocator, addr_str);
        errdefer listen_addr.deinit();

        try self.server_switch.listen(listen_addr, null, struct {
            fn onStream(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}
        }.onStream);

        return listen_addr;
    }

    /// Create dial address for the server.
    fn dialAddr(self: *Self, port: u16) !Multiaddr {
        var addr_buf: [64]u8 = undefined;
        const addr_str = std.fmt.bufPrint(&addr_buf, "/ip4/127.0.0.1/udp/{d}", .{port}) catch unreachable;
        var addr = try Multiaddr.fromString(self.allocator, addr_str);
        errdefer addr.deinit();
        try addr.push(.{ .P2P = self.server_peer_id });
        return addr;
    }
};

/// Callback context for identify results with timeout support.
const IdentifyResultCtx = struct {
    event: std.Thread.ResetEvent = .{},
    result: ?IdentifyResult = null,
    err: ?anyerror = null,
    allocator: Allocator,

    const Self = @This();

    fn callback(ctx: ?*anyopaque, res: anyerror!IdentifyResult) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        self.result = res catch |err| {
            self.err = err;
            self.event.set();
            return;
        };
        self.err = null;
        self.event.set();
    }

    /// Wait for result with timeout. Returns true if signaled, false if timed out.
    fn timedWait(self: *Self, timeout_ms: u64) bool {
        self.event.timedWait(timeout_ms * std.time.ns_per_ms) catch return false;
        return true;
    }

    fn freeResult(self: *Self) void {
        if (self.result) |result| {
            if (result.listen_addrs) |addrs| self.allocator.free(addrs);
            if (result.protocols) |prots| self.allocator.free(prots);
        }
    }
};

test "identify protocol exchanges peer info with config values and protocols" {
    const allocator = std.testing.allocator;

    var ctx = try TestContext.init(allocator);
    defer ctx.deinit();

    const expected_protocols = &[_][]const u8{ "/ipfs/ping/1.0.0", "/ipfs/id/1.0.0", "/custom/test/1.0" };

    // Server: configure with all identify fields including supported protocols
    var server_identify = IdentifyProtocolHandler.initWithConfig(allocator, &ctx.server_switch, .{
        .protocol_version = "libp2p/1.0.0",
        .agent_version = "zig-libp2p/0.1.0",
        .supported_protocols = expected_protocols,
    });
    defer server_identify.deinit();
    try ctx.server_switch.addProtocolHandler(protocol_id, server_identify.any());

    // Client: minimal config
    var client_identify = IdentifyProtocolHandler.init(allocator, &ctx.client_switch);
    defer client_identify.deinit();
    try ctx.client_switch.addProtocolHandler(protocol_id, client_identify.any());

    // Listen - socket bind is synchronous
    const listen_addr = try ctx.listenServer(9600);
    defer listen_addr.deinit();

    var dial_addr = try ctx.dialAddr(9600);
    defer dial_addr.deinit();

    // Execute identify with timeout
    var identify_service = IdentifyService.init(allocator, &ctx.client_switch);
    var result_ctx = IdentifyResultCtx{ .allocator = allocator };
    defer result_ctx.freeResult();

    try identify_service.identify(dial_addr, &result_ctx, IdentifyResultCtx.callback);

    // Wait with timeout instead of indefinite wait
    const signaled = result_ctx.timedWait(5000); // 5 second timeout
    try std.testing.expect(signaled); // Fail if timed out

    // Assertions
    try std.testing.expect(result_ctx.err == null);
    const result = result_ctx.result orelse return error.IdentifyFailed;

    // Verify protocol_version
    try std.testing.expect(result.protocol_version != null);
    try std.testing.expectEqualStrings("libp2p/1.0.0", result.protocol_version.?);

    // Verify agent_version
    try std.testing.expect(result.agent_version != null);
    try std.testing.expectEqualStrings("zig-libp2p/0.1.0", result.agent_version.?);

    // Verify public_key is present and non-empty
    try std.testing.expect(result.public_key != null);
    try std.testing.expect(result.public_key.?.len > 0);

    // Verify protocols list - count AND content (#16 fix)
    try std.testing.expect(result.protocols != null);
    const protocols_result = result.protocols.?;
    try std.testing.expectEqual(@as(usize, 3), protocols_result.len);

    // Verify each protocol string matches expected
    for (expected_protocols, 0..) |expected, i| {
        try std.testing.expectEqualStrings(expected, protocols_result[i]);
    }
}

test "identify handler accesses transport through switch" {
    // This test validates the architectural change: handlers access transport
    // through network_switch.transport instead of storing transport directly.
    // Requires minimal infrastructure since we only test sync methods.

    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    // Minimal setup - event loop needed for transport init but not started
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    var host_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer host_key.deinit();

    var transport: quic_transport.QuicTransport = undefined;
    try transport.init(&loop, &host_key, keys.KeyType.ED25519, allocator);

    var network_switch: swarm.Switch = undefined;
    network_switch.init(allocator, &transport);
    defer {
        network_switch.stop();
        network_switch.deinit();
    }

    // Create both handler types - validates init works without transport param
    var identify_handler = IdentifyProtocolHandler.init(allocator, &network_switch);
    defer identify_handler.deinit();

    var push_handler = IdentifyPushHandler.init(allocator, &network_switch);
    defer push_handler.deinit();

    // Both should access the same public key through switch.transport
    const id_pub_key = try identify_handler.getPublicKeyBytes(allocator);
    defer allocator.free(id_pub_key);

    const push_pub_key = try push_handler.getPublicKeyBytes(allocator);
    defer allocator.free(push_pub_key);

    try std.testing.expect(id_pub_key.len > 0);
    try std.testing.expectEqualSlices(u8, id_pub_key, push_pub_key);

    // Verify matches direct transport access
    const direct_pub_key = try transport.host_keypair.publicKeyBytes(allocator);
    defer allocator.free(direct_pub_key);
    try std.testing.expectEqualSlices(u8, id_pub_key, direct_pub_key);
}

test "identify handler getSupportedProtocols uses config when provided" {
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    // Minimal setup for sync method test
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    var host_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer host_key.deinit();

    var transport: quic_transport.QuicTransport = undefined;
    try transport.init(&loop, &host_key, keys.KeyType.ED25519, allocator);

    var network_switch: swarm.Switch = undefined;
    network_switch.init(allocator, &transport);
    defer {
        network_switch.stop();
        network_switch.deinit();
    }

    // Handler with explicit protocols in config
    const expected_protocols = &[_][]const u8{ "/test/proto/1.0", "/test/proto/2.0" };
    var handler = IdentifyProtocolHandler.initWithConfig(allocator, &network_switch, .{
        .supported_protocols = expected_protocols,
    });
    defer handler.deinit();

    var protocols_list = try handler.getSupportedProtocols(allocator);
    defer protocols_list.deinit(allocator);

    // Verify count AND content
    try std.testing.expectEqual(@as(usize, 2), protocols_list.items.len);
    try std.testing.expectEqualStrings("/test/proto/1.0", protocols_list.items[0]);
    try std.testing.expectEqualStrings("/test/proto/2.0", protocols_list.items[1]);
}

test "identify push handler initialization and config" {
    // Tests IdentifyPushHandler can be initialized and configured correctly.
    // This is a unit test - no network traffic involved.
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    // Minimal setup for unit test
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    var host_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer host_key.deinit();

    var transport: quic_transport.QuicTransport = undefined;
    try transport.init(&loop, &host_key, keys.KeyType.ED25519, allocator);

    var network_switch: swarm.Switch = undefined;
    network_switch.init(allocator, &transport);
    defer {
        network_switch.stop();
        network_switch.deinit();
    }

    // Verify protocol ID constant is correct
    try std.testing.expectEqualStrings("/ipfs/id/push/1.0.0", protocol_push_id);

    // Test handler with config
    const expected_protocols = &[_][]const u8{ "/push/proto/1.0", "/push/proto/2.0" };
    var handler = IdentifyPushHandler.initWithConfig(allocator, &network_switch, .{
        .protocol_version = "push/1.0.0",
        .agent_version = "zig-push/0.1.0",
        .supported_protocols = expected_protocols,
    });
    defer handler.deinit();

    // Verify handler can access public key through switch
    const pub_key = try handler.getPublicKeyBytes(allocator);
    defer allocator.free(pub_key);
    try std.testing.expect(pub_key.len > 0);

    // Verify config protocols are returned
    var protocols_list = try handler.getSupportedProtocols(allocator);
    defer protocols_list.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), protocols_list.items.len);
    try std.testing.expectEqualStrings("/push/proto/1.0", protocols_list.items[0]);
    try std.testing.expectEqualStrings("/push/proto/2.0", protocols_list.items[1]);
}
