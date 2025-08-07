const std = @import("std");
const libp2p = @import("../root.zig");
const quic = libp2p.transport.quic;
const protocols = libp2p.protocols;
const @"switch" = @import("../switch.zig");
const io_loop = @import("../thread_event_loop.zig");
const ssl = @import("ssl");
const keys_proto = @import("../proto/keys.proto.zig");
const tls = @import("../security/tls.zig");

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
    ) void {
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
    ) void {
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
    const switch1_listen_address = try std.net.Address.parseIp4("127.0.0.1", 8767);

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.deinit();
    }

    const host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);

    var switch1: @"switch".Switch = undefined;
    switch1.init(allocator, &transport);
    defer {
        switch1.deinit();
    }

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
    defer {
        cl_loop.deinit();
    }

    const cl_host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(cl_host_key);

    var cl_transport: quic.QuicTransport = undefined;
    try cl_transport.init(&cl_loop, cl_host_key, keys_proto.KeyType.ED25519, allocator);

    var switch2: @"switch".Switch = undefined;
    switch2.init(allocator, &cl_transport);
    defer {
        switch2.deinit();
    }

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

    var callback1: TestNewStreamCallback = .{
        .mutex = .{},
        .sender = undefined,
    };
    switch2.newStream(
        switch1_listen_address,
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
    const switch1_listen_address = try std.net.Address.parseIp4("127.0.0.1", 8767);

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer loop.deinit();

    const host_key = try tls.generateKeyPair(keys_proto.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);

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

    // Loop sending logic to send approximately 1MB of data ---
    const SendContext = struct {
        total_sent: std.atomic.Value(usize),
        total_messages: usize,
        completed_sends: std.atomic.Value(usize),
        completion_event: std.Thread.ResetEvent,
        failed: std.atomic.Value(bool),

        const Self = @This();

        pub fn sendCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));

            if (res) |size| {
                _ = self.total_sent.fetchAdd(size, .monotonic);
                std.debug.print("Message sent successfully, size: {}, total sent: {}\n", .{ size, self.total_sent.load(.monotonic) });
            } else |err| {
                std.debug.print("Failed to send message: {s}\n", .{@errorName(err)});
                self.failed.store(true, .monotonic);
            }

            // Check if this was the last message
            const completed = self.completed_sends.fetchAdd(1, .monotonic) + 1;
            if (completed >= self.total_messages) {
                self.completion_event.set();
            }
        }
    };

    // Configuration for the data sending
    const MESSAGE_SIZE = 1024; // 1KB per message
    const TARGET_TOTAL_SIZE = 1024 * 1024; // 1MB total
    const TOTAL_MESSAGES = TARGET_TOTAL_SIZE / MESSAGE_SIZE;

    // Create the message payload (1KB of data)
    var message_buffer: [MESSAGE_SIZE]u8 = undefined;
    for (&message_buffer, 0..) |*byte, i| {
        byte.* = @intCast(i % 256); // Fill with repeating pattern
    }

    var send_context = SendContext{
        .total_sent = std.atomic.Value(usize).init(0),
        .total_messages = TOTAL_MESSAGES,
        .completed_sends = std.atomic.Value(usize).init(0),
        .completion_event = .{},
        .failed = std.atomic.Value(bool).init(false),
    };

    std.debug.print("Starting to send {} messages of {} bytes each (total: {} bytes)\n", .{ TOTAL_MESSAGES, MESSAGE_SIZE, TARGET_TOTAL_SIZE });

    // Send all messages in a loop
    for (0..TOTAL_MESSAGES) |i| {
        // Add a sequence number to each message for debugging
        const sequence_bytes = std.mem.asBytes(&i);
        @memcpy(message_buffer[0..@sizeOf(usize)], sequence_bytes);

        callback.sender.send(&message_buffer, &send_context, SendContext.sendCallback);

        // Optional: Add a small delay to avoid overwhelming the system
        if (i % 100 == 0) {
            std.time.sleep(1 * std.time.ns_per_ms); // 1ms delay every 100 messages
        }
    }

    std.debug.print("All {} messages queued for sending, waiting for completion...\n", .{TOTAL_MESSAGES});

    // Wait for all sends to complete
    send_context.completion_event.wait();

    if (send_context.failed.load(.monotonic)) {
        std.debug.print("Some messages failed to send!\n", .{});
    } else {
        std.debug.print("Successfully sent all messages! Total bytes: {}\n", .{send_context.total_sent.load(.monotonic)});
    }

    // Give some time for the responder to process all messages
    std.time.sleep(2000 * std.time.ns_per_ms);
}
