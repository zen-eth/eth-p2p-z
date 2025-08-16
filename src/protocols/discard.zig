const std = @import("std");
const libp2p = @import("../root.zig");
const quic = libp2p.transport.quic;
const protocols = libp2p.protocols;
const swarm = libp2p.swarm;
const io_loop = libp2p.thread_event_loop;
const ssl = @import("ssl");
const keys_proto = libp2p.protobuf.keys;
const tls = libp2p.security.tls;
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;

pub const DiscardProtocolHandler = struct {
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(DiscardInitiator) catch unreachable;
        handler.* = .{
            .sender = undefined,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(DiscardResponder) catch unreachable;
        handler.* = .{
            .total_received = 0,
            .message_count = 0,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onResponderStart(stream, callback_ctx, callback);
    }

    pub fn vtableOnInitiatorStartFn(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInitiatorStart(stream, callback_ctx, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = protocols.ProtocolHandlerVTable{
        .onInitiatorStartFn = vtableOnInitiatorStartFn,
        .onResponderStartFn = vtableOnResponderStartFn,
    };

    pub fn any(self: *Self) protocols.AnyProtocolHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub const DiscardInitiator = struct {
    callback_ctx: ?*anyopaque,

    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

    allocator: std.mem.Allocator,

    sender: *DiscardSender,

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
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
        self.allocator.destroy(self.sender);

        const allocator = self.allocator;
        allocator.destroy(self);
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
    const vtable_instance = protocols.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    pub fn any(self: *Self) protocols.AnyProtocolMessageHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub const DiscardResponder = struct {
    callback_ctx: ?*anyopaque,

    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

    total_received: usize,

    message_count: usize,

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn onActivated(_: *Self, _: *quic.QuicStream) anyerror!void {}

    pub fn onMessage(self: *Self, _: *quic.QuicStream, msg: []const u8) anyerror!void {
        self.total_received += msg.len;
        self.message_count += 1;
        std.debug.print("DiscardResponder received message {}: {}\n", .{ self.message_count, msg.len });
    }

    pub fn onClose(self: *Self, _: *quic.QuicStream) anyerror!void {
        const allocator = self.allocator;
        allocator.destroy(self);
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
    const vtable_instance = protocols.ProtocolMessageHandlerVTable{
        .onActivatedFn = vtableOnActivatedFn,
        .onMessageFn = vtableOnMessageFn,
        .onCloseFn = vtableOnCloseFn,
    };

    pub fn any(self: *Self) protocols.AnyProtocolMessageHandler {
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
    const switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/8767");
    defer switch1_listen_address.deinit();

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.deinit();
    }

    const host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);

    var pubkey = try tls.createProtobufEncodedPublicKey1(allocator, host_key);
    defer allocator.free(pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &pubkey);

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport);
    defer {
        switch1.deinit();
    }

    var discard_handler = DiscardProtocolHandler.init(allocator);
    defer discard_handler.deinit();
    try switch1.addProtocolHandler("discard", discard_handler.any());

    try switch1.listen(switch1_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {
            // Handle the callback
        }
    }.callback);

    // Wait for the switch to start listening.
    std.time.sleep(200 * std.time.ns_per_ms);

    var cl_loop: io_loop.ThreadEventLoop = undefined;
    try cl_loop.init(allocator);
    defer {
        cl_loop.deinit();
    }

    const cl_host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(cl_host_key);

    var cl_transport: quic.QuicTransport = undefined;
    try cl_transport.init(&cl_loop, cl_host_key, keys_proto.KeyType.ED25519, allocator);

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &cl_transport);
    defer {
        switch2.deinit();
    }

    var discard_handler2 = DiscardProtocolHandler.init(allocator);
    defer discard_handler2.deinit();
    try switch2.addProtocolHandler("discard", discard_handler2.any());

    const TestNewStreamCallback = struct {
        mutex: std.Thread.ResetEvent,

        sender: *DiscardSender,

        const Self = @This();
        pub fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const sender_ptr = res catch {
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

    var dial_ma = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/8767");
    try dial_ma.push(.{ .P2P = server_peer_id });
    defer dial_ma.deinit();
    switch2.newStream(
        dial_ma,
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

    var callback1: TestNewStreamCallback = .{
        .mutex = .{},
        .sender = undefined,
    };
    switch2.newStream(
        dial_ma,
        &.{"discard"},
        &callback1,
        TestNewStreamCallback.callback,
    );

    callback1.mutex.wait();

    callback1.sender.send("Hello from Switch 2 (second message)", null, struct {
        pub fn callback_(_: ?*anyopaque, res: anyerror!usize) void {
            if (res) |size| {
                std.debug.print("Second message sent successfully, size: {}\n", .{size});
            } else |err| {
                std.debug.print("Failed to send second message: {}\n", .{err});
            }
        }
    }.callback_);

    std.time.sleep(2000 * std.time.ns_per_ms); // Wait for the stream to be established

}

test "discard protocol using switch with 1MB data" {
    const allocator = std.testing.allocator;
    const switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/8777");
    defer switch1_listen_address.deinit();

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer loop.deinit();

    const host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    var pubkey = try tls.createProtobufEncodedPublicKey1(allocator, host_key);
    defer allocator.free(pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &pubkey);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport);
    defer switch1.deinit();

    var discard_handler = DiscardProtocolHandler.init(allocator);
    defer discard_handler.deinit();
    try switch1.addProtocolHandler("discard", discard_handler.any());

    try switch1.listen(switch1_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}
    }.callback);

    std.time.sleep(200 * std.time.ns_per_ms);

    var cl_loop: io_loop.ThreadEventLoop = undefined;
    try cl_loop.init(allocator);
    defer cl_loop.deinit();

    const cl_host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(cl_host_key);

    var cl_transport: quic.QuicTransport = undefined;
    try cl_transport.init(&cl_loop, cl_host_key, keys_proto.KeyType.ED25519, allocator);

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &cl_transport);
    defer switch2.deinit();

    var discard_handler2 = DiscardProtocolHandler.init(allocator);
    defer discard_handler2.deinit();
    try switch2.addProtocolHandler("discard", discard_handler2.any());

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
            self.mutex.set();
        }
    };

    var callback: TestNewStreamCallback = .{ .mutex = .{}, .sender = undefined };
    var dial_ma = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/8777");
    try dial_ma.push(.{ .P2P = server_peer_id });
    defer dial_ma.deinit();
    switch2.newStream(dial_ma, &.{"discard"}, &callback, TestNewStreamCallback.callback);
    callback.mutex.wait();
    const sender = callback.sender;

    const BlockingSendCallback = struct {
        mutex: std.Thread.ResetEvent,
        result: anyerror!usize,

        const Self = @This();
        pub fn callback_(ctx: ?*anyopaque, res: anyerror!usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.result = res;
            self.mutex.set();
        }
    };

    const MESSAGE_SIZE = 1024; // 1KB per message
    const TARGET_TOTAL_SIZE = 1024 * 1024; // 1MB total
    const TOTAL_MESSAGES = TARGET_TOTAL_SIZE / MESSAGE_SIZE;

    var message_buffer: [MESSAGE_SIZE]u8 = undefined;
    for (&message_buffer, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    var total_sent: usize = 0;
    std.debug.print("Starting to send {} messages of {} bytes each in a blocking loop...\n", .{ TOTAL_MESSAGES, MESSAGE_SIZE });

    for (0..TOTAL_MESSAGES) |i| {
        var send_callback = BlockingSendCallback{
            .mutex = .{},
            .result = undefined,
        };

        sender.send(&message_buffer, &send_callback, BlockingSendCallback.callback_);

        send_callback.mutex.wait();

        const size = send_callback.result catch |err| {
            std.debug.print("Failed to send message {d}: {s}\n", .{ i, @errorName(err) });
            return err; // Propagate error to test framework
        };
        total_sent += size;
    }

    std.debug.print("Successfully sent all messages! Total bytes: {}\n", .{total_sent});
    try std.testing.expectEqual(TARGET_TOTAL_SIZE, total_sent);

    // Give some time for the responder to process all messages
    std.time.sleep(2000 * std.time.ns_per_ms);
}

test "no supported protocols error" {
    const allocator = std.testing.allocator;
    const switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/8867");
    defer switch1_listen_address.deinit();

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.deinit();
    }

    const host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    var pubkey = try tls.createProtobufEncodedPublicKey1(allocator, host_key);
    defer allocator.free(pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &pubkey);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport);
    defer {
        switch1.deinit();
    }

    var discard_handler = DiscardProtocolHandler.init(allocator);
    defer discard_handler.deinit();

    try switch1.listen(switch1_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {
            // Handle the callback
        }
    }.callback);

    // Wait for the switch to start listening.
    std.time.sleep(200 * std.time.ns_per_ms);

    var cl_loop: io_loop.ThreadEventLoop = undefined;
    try cl_loop.init(allocator);
    defer {
        cl_loop.deinit();
    }

    const cl_host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(cl_host_key);

    var cl_transport: quic.QuicTransport = undefined;
    try cl_transport.init(&cl_loop, cl_host_key, keys_proto.KeyType.ED25519, allocator);

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &cl_transport);
    defer {
        switch2.deinit();
    }

    var discard_handler2 = DiscardProtocolHandler.init(allocator);
    defer discard_handler2.deinit();
    // try switch2.addProtocolHandler("discard", discard_handler2.any());

    const TestNewStreamCallback = struct {
        mutex: std.Thread.ResetEvent,

        sender: *DiscardSender,

        const Self = @This();
        pub fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const sender_ptr = res catch |err| {
                std.testing.expectEqual(error.NoSupportedProtocols, err) catch unreachable;
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
    var dial_ma = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/8867");
    try dial_ma.push(.{ .P2P = server_peer_id });
    defer dial_ma.deinit();

    switch2.newStream(
        dial_ma,
        &.{"discard"},
        &callback,
        TestNewStreamCallback.callback,
    );

    callback.mutex.wait();

    std.time.sleep(2000 * std.time.ns_per_ms); // Wait for the stream to be established

}
