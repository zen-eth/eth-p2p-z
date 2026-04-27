const std = @import("std");
const libp2p = @import("../root.zig");
const protocols = libp2p.protocols;
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
/// Identity information is pre-built at construction time (following jvm-libp2p pattern).
pub const IdentifyConfig = struct {
    protocol_version: ?[]const u8 = null,
    agent_version: ?[]const u8 = null,
    /// Pre-built public key bytes (required for responder)
    public_key: ?[]const u8 = null,
    /// Pre-built listen addresses
    listen_addrs: ?[]const []const u8 = null,
    /// Pre-built supported protocols
    supported_protocols: ?[]const []const u8 = null,
};

/// Shared builder for constructing Identify protocol messages.
/// Eliminates duplication between IdentifyProtocolHandler and IdentifyPushHandler.
pub const IdentifyMessageBuilder = struct {
    config: IdentifyConfig,

    const Self = @This();

    /// Returns the pre-built public key bytes (borrowed, no allocation).
    pub fn getPublicKey(self: *const Self) ?[]const u8 {
        return self.config.public_key;
    }

    /// Returns the pre-built listen addresses (borrowed, no allocation).
    pub fn getListenAddrs(self: *const Self) []const []const u8 {
        return self.config.listen_addrs orelse &.{};
    }

    /// Returns the pre-built supported protocols (borrowed, no allocation).
    pub fn getSupportedProtocols(self: *const Self) []const []const u8 {
        return self.config.supported_protocols orelse &.{};
    }

    /// Builds an Identify message. Caller owns all allocated memory in the returned message.
    /// Use freeIdentifyMessage() to clean up.
    pub fn build(self: *const Self, allocator: Allocator) !identify_pb.Identify {
        var identify_msg = identify_pb.Identify{};
        errdefer self.freeMessage(allocator, &identify_msg);

        // Set protocol version if configured
        if (self.config.protocol_version) |pv| {
            identify_msg.protocol_version = pv;
        }

        // Set agent version if configured
        if (self.config.agent_version) |av| {
            identify_msg.agent_version = av;
        }

        // Copy public key (single allocation)
        if (self.config.public_key) |pk| {
            identify_msg.public_key = try allocator.dupe(u8, pk);
        }

        // Copy listen addresses (single allocation per address)
        const listen_addrs = self.getListenAddrs();
        if (listen_addrs.len > 0) {
            var addr_bytes_list = try allocator.alloc(?[]const u8, listen_addrs.len);
            errdefer allocator.free(addr_bytes_list);

            for (listen_addrs, 0..) |addr, i| {
                addr_bytes_list[i] = try allocator.dupe(u8, addr);
            }
            identify_msg.listen_addrs = addr_bytes_list;
        }

        // Copy protocols array (protocols are string slices, just copy the pointers)
        const protocols_list = self.getSupportedProtocols();
        if (protocols_list.len > 0) {
            var protocols_array = try allocator.alloc(?[]const u8, protocols_list.len);
            for (protocols_list, 0..) |proto, i| {
                protocols_array[i] = proto;
            }
            identify_msg.protocols = protocols_array;
        }

        return identify_msg;
    }

    /// Frees all memory allocated by build().
    pub fn freeMessage(_: *const Self, allocator: Allocator, msg: *identify_pb.Identify) void {
        if (msg.public_key) |pk| allocator.free(pk);
        if (msg.listen_addrs) |addrs| {
            for (addrs) |maybe_addr| {
                if (maybe_addr) |addr| allocator.free(addr);
            }
            allocator.free(addrs);
        }
        if (msg.protocols) |prots| {
            // Protocols are borrowed slices, only free the array
            allocator.free(prots);
        }
    }
};

/// Identify protocol handler that can be registered with the multistream-select handler.
/// Handlers only use stream abstraction - identity info is pre-built at construction.
pub const IdentifyProtocolHandler = struct {
    allocator: std.mem.Allocator,
    builder: IdentifyMessageBuilder,
    shutting_down: bool = false,

    const Self = @This();

    /// Initialize with pre-built identity configuration.
    pub fn init(allocator: std.mem.Allocator, config: IdentifyConfig) Self {
        return .{
            .allocator = allocator,
            .builder = .{ .config = config },
            .shutting_down = false,
        };
    }

    pub fn deinit(self: *Self) void {
        self.shutting_down = true;
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: protocols.AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyInitiator) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!void) void {}
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
        stream: protocols.AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyResponder) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!void) void {}
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
        stream: protocols.AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInitiatorStart(stream, callback_ctx, callback);
    }

    fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: protocols.AnyStream,
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

    /// Access the message builder for building identify messages.
    pub fn messageBuilder(self: *Self) *const IdentifyMessageBuilder {
        return &self.builder;
    }
};

/// Handles identify messages on the initiator side (querying remote peer).
const IdentifyInitiator = struct {
    stream: protocols.AnyStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    response_buffer: std.ArrayList(u8),
    response_received: bool,

    const Self = @This();

    fn onActivated(self: *Self, _: protocols.AnyStream) anyerror!void {
        _ = self;
        // Initiator waits for response, no action needed on activation
    }

    fn onMessage(self: *Self, stream: protocols.AnyStream, message: []const u8) anyerror!void {
        _ = stream;
        if (message.len == 0) return;

        // Accumulate protobuf bytes
        self.response_buffer.appendSlice(self.allocator, message) catch |err| {
            self.fail(err);
            return err;
        };
    }

    fn onClose(self: *Self, stream: protocols.AnyStream) anyerror!void {
        _ = stream;
        if (self.response_received) return;
        self.response_received = true;

        // Parse and clone all data from the response buffer
        const result = parseIdentifyResult(self.allocator, self.response_buffer.items) catch |err| {
            self.response_buffer.deinit(self.allocator);
            self.deliverErrorAndCleanup(err);
            return err;
        };

        // Deliver result FIRST, then cleanup. This ensures the cloned data in result
        // is fully delivered before we free response_buffer, avoiding any potential
        // race conditions with allocator behavior on different platforms.
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            request_ctx.deliverResult(result);
            // Cleanup after callback completes
            self.response_buffer.deinit(self.allocator);
            self.allocator.destroy(self);
        } else {
            // Direct protocol usage - not supported, free result and report error
            freeIdentifyResult(self.allocator, result);
            self.response_buffer.deinit(self.allocator);
            const cb = self.callback;
            const cb_ctx = self.callback_ctx;
            self.allocator.destroy(self);
            cb(cb_ctx, error.UnexpectedResultType);
        }
    }

    /// Delivers an error and cleans up self.
    fn deliverErrorAndCleanup(self: *Self, err: anyerror) void {
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverError(err);
        } else {
            const cb = self.callback;
            const cb_ctx = self.callback_ctx;
            self.allocator.destroy(self);
            cb(cb_ctx, err);
        }
    }

    fn fail(self: *Self, err: anyerror) void {
        if (self.response_received) return;
        self.response_received = true;
        self.response_buffer.deinit(self.allocator);
        self.deliverErrorAndCleanup(err);
    }

    fn vtableOnActivatedFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: protocols.AnyStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
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
    stream: protocols.AnyStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    handler: *IdentifyProtocolHandler,
    message_sent: bool = false,
    encoded_message: ?[]const u8 = null, // Stored until async write completes

    const Self = @This();

    fn onActivated(self: *Self, _: protocols.AnyStream) anyerror!void {
        // Build identify message using shared builder
        var identify_msg = try self.handler.builder.build(self.allocator);
        errdefer self.handler.builder.freeMessage(self.allocator, &identify_msg);

        const encoded = try identify_msg.encode(self.allocator);
        errdefer self.allocator.free(encoded);

        // Free message after encoding
        self.handler.builder.freeMessage(self.allocator, &identify_msg);

        self.message_sent = true;
        self.encoded_message = encoded; // Store until write completes
        self.stream.write(encoded, self, writeCompleteCallback);
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

    fn closeCallback(ctx: ?*anyopaque, res: anyerror!void) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.fail(err);
            return;
        };

        self.callback(self.callback_ctx, null);
        self.allocator.destroy(self);
    }

    fn onMessage(self: *Self, _: protocols.AnyStream, _: []const u8) anyerror!void {
        _ = self;
        // Responder should not receive messages, but we'll ignore them
    }

    fn onClose(self: *Self, stream: protocols.AnyStream) anyerror!void {
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

    fn vtableOnActivatedFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: protocols.AnyStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
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
/// **Ownership**: All fields are allocated by the parser and owned by the caller.
/// Use `freeIdentifyResult` to release all memory.
pub const IdentifyResult = struct {
    protocol_version: ?[]const u8,
    agent_version: ?[]const u8,
    public_key: ?[]const u8,
    listen_addrs: ?[]const []const u8,
    observed_addr: ?[]const u8,
    protocols: ?[]const []const u8,
};

/// Frees all memory owned by an IdentifyResult.
pub fn freeIdentifyResult(allocator: Allocator, result: IdentifyResult) void {
    if (result.protocol_version) |pv| allocator.free(pv);
    if (result.agent_version) |av| allocator.free(av);
    if (result.public_key) |pk| allocator.free(pk);
    if (result.observed_addr) |oa| allocator.free(oa);
    if (result.listen_addrs) |addrs| {
        for (addrs) |a| allocator.free(a);
        allocator.free(addrs);
    }
    if (result.protocols) |prots| {
        for (prots) |p| allocator.free(p);
        allocator.free(prots);
    }
}

/// Parses an Identify protobuf message and returns an owned IdentifyResult.
/// All strings are cloned from the buffer, so the buffer can be freed after this returns.
/// On error, no memory is leaked. On success, caller must call freeIdentifyResult.
fn parseIdentifyResult(allocator: Allocator, buffer: []const u8) anyerror!IdentifyResult {
    var reader = try identify_pb.IdentifyReader.init(buffer);

    // Clone simple string fields
    const pv_raw = reader.getProtocolVersion();
    const protocol_version: ?[]const u8 = if (pv_raw.len > 0) try allocator.dupe(u8, pv_raw) else null;
    errdefer if (protocol_version) |pv| allocator.free(pv);

    const av_raw = reader.getAgentVersion();
    const agent_version: ?[]const u8 = if (av_raw.len > 0) try allocator.dupe(u8, av_raw) else null;
    errdefer if (agent_version) |av| allocator.free(av);

    const pk_raw = reader.getPublicKey();
    const public_key: ?[]const u8 = if (pk_raw.len > 0) try allocator.dupe(u8, pk_raw) else null;
    errdefer if (public_key) |pk| allocator.free(pk);

    const oa_raw = reader.getObservedAddr();
    const observed_addr: ?[]const u8 = if (oa_raw.len > 0) try allocator.dupe(u8, oa_raw) else null;
    errdefer if (observed_addr) |oa| allocator.free(oa);

    // Clone listen addresses
    var listen_addrs: ?[][]const u8 = null;
    const listen_addrs_count = reader.listenAddrsCount();
    if (listen_addrs_count > 0) {
        const addrs = try allocator.alloc([]const u8, listen_addrs_count);
        errdefer allocator.free(addrs);
        var i: usize = 0;
        errdefer for (addrs[0..i]) |a| allocator.free(a);
        while (reader.listenAddrsNext()) |addr| {
            addrs[i] = try allocator.dupe(u8, addr);
            i += 1;
        }
        listen_addrs = addrs;
    }
    errdefer if (listen_addrs) |addrs| {
        for (addrs) |a| allocator.free(a);
        allocator.free(addrs);
    };

    // Clone protocols
    var protocols_list: ?[][]const u8 = null;
    const protocols_count = reader.protocolsCount();
    if (protocols_count > 0) {
        const prots = try allocator.alloc([]const u8, protocols_count);
        errdefer allocator.free(prots);
        var j: usize = 0;
        errdefer for (prots[0..j]) |p| allocator.free(p);
        while (reader.protocolsNext()) |proto| {
            prots[j] = try allocator.dupe(u8, proto);
            j += 1;
        }
        protocols_list = prots;
    }

    return IdentifyResult{
        .protocol_version = protocol_version,
        .agent_version = agent_version,
        .public_key = public_key,
        .listen_addrs = listen_addrs,
        .observed_addr = observed_addr,
        .protocols = protocols_list,
    };
}

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
/// Handlers only use stream abstraction - identity info is pre-built at construction.
pub const IdentifyPushHandler = struct {
    allocator: std.mem.Allocator,
    builder: IdentifyMessageBuilder,
    shutting_down: bool = false,

    const Self = @This();

    /// Initialize with pre-built identity configuration.
    pub fn init(allocator: std.mem.Allocator, config: IdentifyConfig) Self {
        return .{
            .allocator = allocator,
            .builder = .{ .config = config },
            .shutting_down = false,
        };
    }

    pub fn deinit(self: *Self) void {
        self.shutting_down = true;
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: protocols.AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyPushInitiator) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!void) void {}
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
        stream: protocols.AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(IdentifyPushResponder) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!void) void {}
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
        stream: protocols.AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInitiatorStart(stream, callback_ctx, callback);
    }

    fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: protocols.AnyStream,
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

    /// Access the message builder for building identify messages.
    pub fn messageBuilder(self: *Self) *const IdentifyMessageBuilder {
        return &self.builder;
    }
};

/// Handles identify push messages on the initiator side (sending push update).
const IdentifyPushInitiator = struct {
    stream: protocols.AnyStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    handler: *IdentifyPushHandler,
    message_sent: bool = false,
    encoded_message: ?[]const u8 = null, // Stored until async write completes

    const Self = @This();

    fn onActivated(self: *Self, _: protocols.AnyStream) anyerror!void {
        // Build identify message using shared builder
        var identify_msg = try self.handler.builder.build(self.allocator);
        errdefer self.handler.builder.freeMessage(self.allocator, &identify_msg);

        const encoded = try identify_msg.encode(self.allocator);
        errdefer self.allocator.free(encoded);

        // Free message after encoding
        self.handler.builder.freeMessage(self.allocator, &identify_msg);

        self.message_sent = true;
        self.encoded_message = encoded; // Store until write completes
        self.stream.write(encoded, self, writeCompleteCallback);
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

    fn closeCallback(ctx: ?*anyopaque, res: anyerror!void) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        _ = res catch |err| {
            self.fail(err);
            return;
        };

        self.callback(self.callback_ctx, null);
        self.allocator.destroy(self);
    }

    fn onMessage(self: *Self, _: protocols.AnyStream, _: []const u8) anyerror!void {
        _ = self;
        // Initiator should not receive messages
    }

    fn onClose(self: *Self, stream: protocols.AnyStream) anyerror!void {
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

    fn vtableOnActivatedFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: protocols.AnyStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
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
    stream: protocols.AnyStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    response_buffer: std.ArrayList(u8),
    response_received: bool,

    const Self = @This();

    fn onActivated(self: *Self, _: protocols.AnyStream) anyerror!void {
        _ = self;
        // Responder waits for push message
    }

    fn onMessage(self: *Self, stream: protocols.AnyStream, message: []const u8) anyerror!void {
        _ = stream;
        if (message.len == 0) return;

        // Accumulate protobuf bytes
        self.response_buffer.appendSlice(self.allocator, message) catch |err| {
            self.fail(err);
            return err;
        };
    }

    fn onClose(self: *Self, stream: protocols.AnyStream) anyerror!void {
        _ = stream;
        if (self.response_received) return;
        self.response_received = true;

        // Parse and clone all data from the response buffer
        const result = parseIdentifyResult(self.allocator, self.response_buffer.items) catch |err| {
            self.response_buffer.deinit(self.allocator);
            self.deliverErrorAndCleanup(err);
            return err;
        };

        // Deliver result FIRST, then cleanup. This ensures the cloned data in result
        // is fully delivered before we free response_buffer, avoiding any potential
        // race conditions with allocator behavior on different platforms.
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            request_ctx.deliverResult(result);
            // Cleanup after callback completes
            self.response_buffer.deinit(self.allocator);
            self.allocator.destroy(self);
        } else {
            // Direct protocol usage - not supported, free result and report error
            freeIdentifyResult(self.allocator, result);
            self.response_buffer.deinit(self.allocator);
            const cb = self.callback;
            const cb_ctx = self.callback_ctx;
            self.allocator.destroy(self);
            cb(cb_ctx, error.UnexpectedResultType);
        }
    }

    /// Delivers an error and cleans up self.
    fn deliverErrorAndCleanup(self: *Self, err: anyerror) void {
        if (IdentifyRequestCtx.isValidCtx(self.callback_ctx)) {
            const request_ctx: *IdentifyRequestCtx = @ptrCast(@alignCast(self.callback_ctx.?));
            self.allocator.destroy(self);
            request_ctx.deliverError(err);
        } else {
            const cb = self.callback;
            const cb_ctx = self.callback_ctx;
            self.allocator.destroy(self);
            cb(cb_ctx, err);
        }
    }

    fn fail(self: *Self, err: anyerror) void {
        if (self.response_received) return;
        self.response_received = true;
        self.response_buffer.deinit(self.allocator);
        self.deliverErrorAndCleanup(err);
    }

    fn vtableOnActivatedFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    fn vtableOnMessageFn(instance: *anyopaque, stream: protocols.AnyStream, message: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    fn vtableOnCloseFn(instance: *anyopaque, stream: protocols.AnyStream) anyerror!void {
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

    /// Build identify config with pre-built identity info from server transport.
    fn serverIdentifyConfig(self: *Self) !IdentifyConfig {
        const public_key = try self.server_transport.host_keypair.publicKeyBytes(self.allocator);
        return .{
            .public_key = public_key,
        };
    }

    /// Build identify config with pre-built identity info from client transport.
    fn clientIdentifyConfig(self: *Self) !IdentifyConfig {
        const public_key = try self.client_transport.host_keypair.publicKeyBytes(self.allocator);
        return .{
            .public_key = public_key,
        };
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
            freeIdentifyResult(self.allocator, result);
        }
    }
};

test "identify protocol exchanges peer info with config values and protocols" {
    const allocator = std.testing.allocator;

    var ctx = try TestContext.init(allocator);
    defer ctx.deinit();

    const expected_protocols = &[_][]const u8{ "/ipfs/ping/1.0.0", "/ipfs/id/1.0.0", "/custom/test/1.0" };

    // Build pre-built identity config from transport (like service-level code would do)
    const server_public_key = try ctx.server_transport.host_keypair.publicKeyBytes(allocator);
    defer allocator.free(server_public_key);

    // Server: configure with all identify fields including supported protocols
    var server_identify = IdentifyProtocolHandler.init(allocator, .{
        .protocol_version = "libp2p/1.0.0",
        .agent_version = "zig-libp2p/0.1.0",
        .public_key = server_public_key,
        .supported_protocols = expected_protocols,
    });
    defer server_identify.deinit();
    try ctx.server_switch.addProtocolHandler(protocol_id, server_identify.any());

    // Client: minimal config (client doesn't need public key for initiator role)
    var client_identify = IdentifyProtocolHandler.init(allocator, .{});
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

test "identify handler uses pre-built identity config" {
    // This test validates the architectural change: handlers use pre-built
    // identity config instead of accessing transport/switch directly.
    // Identity info is injected at construction time.

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

    // Pre-build identity info from transport (service-level code would do this)
    const public_key = try transport.host_keypair.publicKeyBytes(allocator);
    defer allocator.free(public_key);

    // Create handlers with pre-built config
    var identify_handler = IdentifyProtocolHandler.init(allocator, .{
        .public_key = public_key,
    });
    defer identify_handler.deinit();

    var push_handler = IdentifyPushHandler.init(allocator, .{
        .public_key = public_key,
    });
    defer push_handler.deinit();

    // Both should return the same public key from config
    const id_pub_key = identify_handler.builder.getPublicKey();
    const push_pub_key = push_handler.builder.getPublicKey();

    try std.testing.expect(id_pub_key != null);
    try std.testing.expect(id_pub_key.?.len > 0);
    try std.testing.expectEqualSlices(u8, id_pub_key.?, push_pub_key.?);

    // Verify matches the original public key
    try std.testing.expectEqualSlices(u8, id_pub_key.?, public_key);
}

test "identify handler getSupportedProtocols uses config" {
    const allocator = std.testing.allocator;

    // Handler with explicit protocols in config (no transport/switch needed)
    const expected_protocols = &[_][]const u8{ "/test/proto/1.0", "/test/proto/2.0" };
    var handler = IdentifyProtocolHandler.init(allocator, .{
        .supported_protocols = expected_protocols,
    });
    defer handler.deinit();

    const protocols_list = handler.builder.getSupportedProtocols();

    // Verify count AND content
    try std.testing.expectEqual(@as(usize, 2), protocols_list.len);
    try std.testing.expectEqualStrings("/test/proto/1.0", protocols_list[0]);
    try std.testing.expectEqualStrings("/test/proto/2.0", protocols_list[1]);
}

test "identify push handler initialization and config" {
    // Tests IdentifyPushHandler can be initialized and configured correctly.
    // This is a unit test - no network traffic involved.
    const allocator = std.testing.allocator;

    // Verify protocol ID constant is correct
    try std.testing.expectEqualStrings("/ipfs/id/push/1.0.0", protocol_push_id);

    // Test handler with pre-built config (no transport/switch needed)
    const expected_protocols = &[_][]const u8{ "/push/proto/1.0", "/push/proto/2.0" };
    const test_public_key = "test-public-key-bytes";
    var handler = IdentifyPushHandler.init(allocator, .{
        .protocol_version = "push/1.0.0",
        .agent_version = "zig-push/0.1.0",
        .public_key = test_public_key,
        .supported_protocols = expected_protocols,
    });
    defer handler.deinit();

    // Verify handler returns public key from config
    const pub_key = handler.builder.getPublicKey();
    try std.testing.expect(pub_key != null);
    try std.testing.expect(pub_key.?.len > 0);
    try std.testing.expectEqualSlices(u8, test_public_key, pub_key.?);

    // Verify config protocols are returned
    const protocols_list = handler.builder.getSupportedProtocols();
    try std.testing.expectEqual(@as(usize, 2), protocols_list.len);
    try std.testing.expectEqualStrings("/push/proto/1.0", protocols_list[0]);
    try std.testing.expectEqualStrings("/push/proto/2.0", protocols_list[1]);
}
