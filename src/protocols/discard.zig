const std = @import("std");
const quic = @import("../transport/quic/root.zig").lsquic_transport;
const proto_handler = @import("../proto_handler.zig");
const @"switch" = @import("../switch.zig");
const io_loop = @import("../thread_event_loop.zig");
const ssl = @import("ssl");
const keys_proto = @import("../proto/keys.proto.zig");
const tls = @import("../security/tls.zig");

pub const DiscardProtocolHandler = struct {
    allocator: std.mem.Allocator,

    initiator: ?*DiscardInitiator,

    responder: ?*DiscardResponder,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .initiator = null,
            .responder = null,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.initiator) |initiator| {
            self.allocator.destroy(initiator);
        }
        if (self.responder) |responder| {
            self.allocator.destroy(responder);
        }
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const handler = stream.conn.engine.allocator.create(DiscardInitiator) catch unreachable;
        handler.* = .{
            .sender = undefined,
            .stream = stream,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
        };
        self.initiator = handler;
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) void {
        const handler = stream.conn.engine.allocator.create(DiscardResponder) catch unreachable;
        handler.* = .{
            .callback_ctx = callback_ctx,
            .callback = callback,
        };
        self.responder = handler;
        stream.setProtoMsgHandler(handler.any());
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

    sender: *DiscardSender,

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        self.stream = stream;
        const sender = self.allocator.create(DiscardSender) catch unreachable;
        sender.* = DiscardSender.init(stream);
        self.sender = sender;
        self.callback(self.callback_ctx, sender);
    }

    pub fn onMessage(self: *Self, _: *quic.QuicStream, msg: []const u8) anyerror!void {
        std.log.warn("Discard protocol received a message: {s}", .{msg});
        self.callback(self.callback_ctx, error.InvalidMessage);
        return error.InvalidMessage;
    }

    pub fn onClose(self: *Self, _: *quic.QuicStream) anyerror!void {
        std.debug.print("Discard protocol stream closed\n", .{});
        self.allocator.destroy(self.sender);
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
    callback_ctx: ?*anyopaque,

    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

    const Self = @This();

    pub fn onActivated(_: *Self, _: *quic.QuicStream) anyerror!void {}

    pub fn onMessage(_: *Self, _: *quic.QuicStream, msg: []const u8) anyerror!void {
        std.debug.print("Discard protocol responder received a message: {s}\n", .{msg});
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

    const Self = @This();

    pub fn init(stream: *quic.QuicStream) Self {
        return Self{
            .stream = stream,
        };
    }

    pub fn deinit(_: *Self) void {}

    pub fn send(self: *Self, message: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void) void {
        self.stream.write(message, callback_ctx, callback);
    }
};

test "discard protocol using switch" {
    const allocator = std.testing.allocator;
    const switch1_listen_address = try std.net.Address.parseIp4("127.0.0.1", 8767);

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer loop.deinit();

    const host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);
    // defer transport.deinit();

    var switch1: @"switch".Switch = undefined;
    switch1.init(allocator, &transport);
    defer switch1.deinit();

    var discard_handler = DiscardProtocolHandler.init(allocator);
    defer discard_handler.deinit();
    switch1.proto_handlers.append(discard_handler.any()) catch unreachable;

    try switch1.listen(switch1_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {
            // Handle the callback
        }
    }.callback);

    // Wait for the switch to start listening.
    std.time.sleep(200 * std.time.ns_per_ms);

    var cl_loop: io_loop.ThreadEventLoop = undefined;
    try cl_loop.init(allocator);
    defer cl_loop.deinit();

    const cl_host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(cl_host_key);

    var cl_transport: quic.QuicTransport = undefined;
    try cl_transport.init(&cl_loop, cl_host_key, keys_proto.KeyType.ED25519, allocator);
    // defer cl_transport.deinit();

    var switch2: @"switch".Switch = undefined;
    switch2.init(allocator, &cl_transport);
    defer switch2.deinit();

    var discard_handler2 = DiscardProtocolHandler.init(allocator);
    defer discard_handler2.deinit();
    switch2.proto_handlers.append(discard_handler2.any()) catch unreachable;

    const TestNewStreamCallback = struct {
        mutex: std.Thread.ResetEvent,

        sender: *DiscardSender,

        const Self = @This();
        pub fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const sender_ptr = res catch |err| {
                std.log.warn("Failed to start stream: {}", .{err});
                self.mutex.set();
                return;
            };
            self.sender = @ptrCast(@alignCast(sender_ptr.?));
            std.log.info("Stream started successfully", .{});
            self.mutex.set();
        }
    };
    var callback: TestNewStreamCallback = .{
        .mutex = .{},
        .sender = undefined,
    };
    switch2.newStream(
        switch1_listen_address,
        &.{"discard"},
        &callback,
        TestNewStreamCallback.callback,
    );

    callback.mutex.wait();
    callback.sender.send("Hello from Switch 2", null, struct {
        pub fn callback_(_: ?*anyopaque, res: anyerror!usize) void {
            if (res) |size| {
                std.debug.print("Message sent successfully, size: {}\n", .{size});
            } else |err| {
                std.debug.print("Failed to send message: {}\n", .{err});
            }
        }
    }.callback_);

    std.time.sleep(200 * std.time.ns_per_ms);

    callback.sender.stream.close(null, struct {
        pub fn callback_(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
    }.callback_);
}
