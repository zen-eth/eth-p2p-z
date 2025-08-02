const std = @import("std");
const quic = @import("../transport/quic/root.zig").lsquic_transport;
const proto_handler = @import("../proto_handler.zig");
const @"switch" = @import("../switch.zig");
const io_loop = @import("../thread_event_loop.zig");
const ssl = @import("ssl");
const keys_proto = @import("../proto/keys.proto.zig");

pub const DiscardProtocolHandler = struct {
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const handler = stream.engine.allocator.create(DiscardInitiator) catch unreachable;
        handler.* = .{
            .stream = stream,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        _: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const handler = stream.engine.allocator.create(DiscardResponder) catch unreachable;
        handler.* = .{};
        stream.proto_msg_handler = handler.any();
        callback(callback_ctx, null);
    }

    pub fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.onResponderStart(stream, callback_ctx, callback);
    }

    pub fn vtableOnInitiatorStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.onInitiatorStart(stream, callback_ctx, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = proto_handler.ProtocolHandlerVTable{
        .onInitiatorStartFn = vtableOnInitiatorStartFn,
        .onResponderStartFn = vtableOnResponderStartFn,
    };

    pub fn any(self: *Self) proto_handler.AnyProtocolHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub const DiscardInitiator = struct {
    stream: *quic.QuicStream,

    callback_ctx: ?*anyopaque,

    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        self.stream = stream;
        const sender = DiscardSender.init(stream, self.allocator);
        self.callback(self.callback_ctx, sender);
    }

    pub fn onMessage(self: *Self, _: *quic.QuicStream, msg: []const u8) anyerror!void {
        std.log.warn("Discard protocol received a message: {s}", .{msg});
        self.callback(self.callback_ctx, error.InvalidMessage);
        return error.InvalidMessage;
    }

    pub fn onClose(_: *Self, _: *quic.QuicStream) anyerror!void {
        // No operation for discard protocol.
    }

    pub fn send(self: *Self, message: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void) void {
        self.stream.write(message, callback_ctx, callback);
    }

    pub fn vtableOnActivatedFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    pub fn vtableOnMessageFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        message: []const u8,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    pub fn vtableOnCloseFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onClose(stream);
    }

    // --- Static VTable Instance ---
    const vtable_instance = proto_handler.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    pub fn any(self: *Self) proto_handler.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub const DiscardResponder = struct {
    const Self = @This();

    pub fn onActivated(_: *Self, _: *quic.QuicStream) anyerror!void {}

    pub fn onMessage(_: *Self, _: *quic.QuicStream, msg: []const u8) anyerror!void {
        std.log.debug("Discard protocol received a message: {s}", .{msg});
    }

    pub fn onClose(_: *Self, _: *quic.QuicStream) anyerror!void {
        // No operation for discard protocol.
    }

    pub fn vtableOnActivatedFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    pub fn vtableOnMessageFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        message: []const u8,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    pub fn vtableOnCloseFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onClose(stream);
    }

    // --- Static VTable Instance ---
    const vtable_instance = proto_handler.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    pub fn any(self: *Self) proto_handler.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub const DiscardSender = struct {
    stream: *quic.QuicStream,

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(stream: *quic.QuicStream, allocator: std.mem.Allocator) *Self {
        const self = allocator.create(Self) catch unreachable;
        self.* = .{ .stream = stream, .allocator = allocator };
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self);
    }

    pub fn send(self: *Self, message: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void) void {
        self.stream.write(message, callback_ctx, callback);
    }
};

test "discard protocol using switch" {
    const allocator = std.testing.allocator;
    const switch1_listen_address = try std.net.Address.parseIp4("127.0.0.1", 8767);

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
    if (ssl.EVP_PKEY_keygen_init(pctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_host_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(pctx, &maybe_host_key) == 0) {
        return error.OpenSSLFailed;
    }
    const host_key = maybe_host_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(host_key);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);
    defer transport.deinit();

    var switch1: @"switch".Switch = undefined;
    switch1.init(allocator, &transport);
    defer switch1.deinit();

    var discard_handler = DiscardProtocolHandler{
        .allocator = allocator,
    };
    switch1.proto_handlers.append(discard_handler.any()) catch unreachable;

    try switch1.listen(switch1_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {
            // Handle the callback
        }
    }.callback);

    std.time.sleep(200 * std.time.ns_per_ms);

    var cl_loop: io_loop.ThreadEventLoop = undefined;
    try cl_loop.init(allocator);
    defer {
        cl_loop.close();
        cl_loop.deinit();
    }
    const cl_pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
    if (ssl.EVP_PKEY_keygen_init(cl_pctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_cl_host_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(cl_pctx, &maybe_cl_host_key) == 0) {
        return error.OpenSSLFailed;
    }
    const cl_host_key = maybe_cl_host_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(cl_host_key);

    var cl_transport: quic.QuicTransport = undefined;
    try cl_transport.init(&cl_loop, cl_host_key, keys_proto.KeyType.ED25519, allocator);
    defer cl_transport.deinit();

    var switch2: @"switch".Switch = undefined;
    switch2.init(allocator, &cl_transport);
    defer switch2.deinit();

    var discard_handler2 = DiscardProtocolHandler{
        .allocator = allocator,
    };
    switch2.proto_handlers.append(discard_handler2.any()) catch unreachable;

    std.time.sleep(200 * std.time.ns_per_ms);

    switch2.newStream(
        switch1_listen_address,
        &.{"discard"},
        null,
        struct {
            pub fn callback(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
                _ = res catch |err| {
                    std.log.warn("Failed to start stream: {}", .{err});
                    return;
                };
                std.log.info("Stream started successfully", .{});
            }
        }.callback,
    );

    std.debug.print("Switch 2 is trying to connect to Switch 1 at address: {}\n", .{switch1_listen_address});
    std.time.sleep(1000 * std.time.ns_per_ms);
}
