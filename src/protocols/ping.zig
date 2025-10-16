//! libp2p ping protocol implementation
//!
//! This module implements the libp2p ping protocol as specified in:
//! https://github.com/libp2p/specs/blob/master/ping/ping.md
//!
//! Protocol ID: `/ipfs/ping/1.0.0`
//!
//! The ping protocol allows peers to measure latency and verify connectivity.
//! The initiator sends 32 random bytes, and the responder echoes them back.
//! The initiator then validates that the received response matches the sent payload.
//!
//! Usage example:
//! ```zig
//! var ping_handler = PingProtocolHandler.init(allocator);
//! defer ping_handler.deinit();
//! try switch.addProtocolHandler("/ipfs/ping/1.0.0", ping_handler.any());
//!
//! // On initiator side, after newStream callback:
//! var payload: [PING_SIZE]u8 = undefined;
//! std.crypto.random.bytes(&payload);
//! ping_sender.ping(&payload, callback_ctx, callback);
//! ```

const std = @import("std");
const libp2p = @import("../root.zig");
const quic = libp2p.transport.quic;
const protocols = libp2p.protocols;
const swarm = libp2p.swarm;
const io_loop = libp2p.thread_event_loop;
const ssl = @import("ssl");
const tls = libp2p.security.tls;
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;
const keys = @import("peer_id").keys;

/// Size of ping payload in bytes according to libp2p ping spec
pub const PING_SIZE: usize = 32;

pub const PingProtocolHandler = struct {
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
        const handler = self.allocator.create(PingInitiator) catch unreachable;
        handler.* = .{
            .sender = undefined,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
            .expected_payload = undefined,
            .received_response = false,
            .ping_in_flight = false,
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(PingResponder) catch unreachable;
        handler.* = .{
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

pub const PingInitiator = struct {
    callback_ctx: ?*anyopaque,
    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    allocator: std.mem.Allocator,
    sender: *PingSender,
    expected_payload: [PING_SIZE]u8,
    received_response: bool,
    ping_in_flight: bool,

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        const sender = self.allocator.create(PingSender) catch unreachable;
        sender.* = PingSender.init(stream, &self.expected_payload, &self.ping_in_flight);
        self.sender = sender;

        // Generate random payload for ping using cryptographically secure random
        std.crypto.random.bytes(&self.expected_payload);

        self.callback(self.callback_ctx, sender);
    }

    pub fn onMessage(self: *Self, _: *quic.QuicStream, msg: []const u8) anyerror!void {
        // Validate response matches expected payload
        if (msg.len != PING_SIZE) {
            std.log.warn("Ping protocol received invalid response size: {} (expected {})", .{ msg.len, PING_SIZE });
            self.callback(self.callback_ctx, error.InvalidPingResponse);
            return error.InvalidPingResponse;
        }

        if (!std.mem.eql(u8, msg, &self.expected_payload)) {
            std.log.warn("Ping protocol received mismatched response", .{});
            self.callback(self.callback_ctx, error.InvalidPingResponse);
            return error.InvalidPingResponse;
        }

        self.received_response = true;
        self.ping_in_flight = false;
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

pub const PingResponder = struct {
    callback_ctx: ?*anyopaque,
    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn onActivated(_: *Self, _: *quic.QuicStream) anyerror!void {}

    pub fn onMessage(_: *Self, stream: *quic.QuicStream, msg: []const u8) anyerror!void {
        // Echo back the received payload
        if (msg.len != PING_SIZE) {
            std.log.warn("Ping responder received invalid payload size: {} (expected {})", .{ msg.len, PING_SIZE });
            return error.InvalidPingPayload;
        }

        // Echo the message back
        const EchoCallback = struct {
            fn callback(_: ?*anyopaque, res: anyerror!usize) void {
                if (res) |_| {
                    std.debug.print("Ping responder echoed back payload\n", .{});
                } else |err| {
                    std.debug.print("Ping responder failed to echo: {}\n", .{err});
                }
            }
        };

        stream.write(msg, null, EchoCallback.callback);
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

pub const PingSender = struct {
    stream: *quic.QuicStream,
    expected_payload_ptr: *[PING_SIZE]u8,
    ping_in_flight_ptr: *bool,

    const Self = @This();

    pub fn init(stream: *quic.QuicStream, expected_payload_ptr: *[PING_SIZE]u8, ping_in_flight_ptr: *bool) Self {
        return Self{
            .stream = stream,
            .expected_payload_ptr = expected_payload_ptr,
            .ping_in_flight_ptr = ping_in_flight_ptr,
        };
    }

    pub fn deinit(_: *Self) void {}

    /// Send a ping with the given 32-byte payload.
    /// Returns error.PingInFlight if a previous ping has not yet received a response.
    /// Concurrent pings are not supported - wait for the previous ping to complete before sending another.
    pub fn ping(self: *Self, payload: *const [PING_SIZE]u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void) !void {
        // Check if a ping is already in flight to prevent race conditions
        if (self.ping_in_flight_ptr.*) {
            return error.PingInFlight;
        }

        // Mark ping as in-flight and update the expected payload
        self.ping_in_flight_ptr.* = true;
        @memcpy(self.expected_payload_ptr, payload);
        self.stream.write(payload, callback_ctx, callback);
    }
};

const TestNewStreamCallback = struct {
    mutex: std.Thread.ResetEvent,
    sender: *PingSender,

    const Self = @This();
    pub fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        const sender_ptr = res catch {
            self.mutex.set();
            return;
        };
        self.sender = @ptrCast(@alignCast(sender_ptr.?));
        self.mutex.set();
    }
};

test "ping protocol basic functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.log.warn("Memory leak detected in test!", .{});
        }
    }
    const allocator = gpa.allocator();

    const server_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9200");
    defer server_listen_address.deinit();

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer server_loop.deinit();

    const server_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(server_key);

    var server_transport: quic.QuicTransport = undefined;
    try server_transport.init(&server_loop, server_key, keys.KeyType.ED25519, allocator);

    var server_pubkey = try tls.createProtobufEncodedPublicKey(allocator, server_key);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer server_switch.deinit();

    var ping_handler = PingProtocolHandler.init(allocator);
    defer ping_handler.deinit();
    try server_switch.addProtocolHandler("/ipfs/ping/1.0.0", ping_handler.any());

    try server_switch.listen(server_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}
    }.callback);

    std.time.sleep(200 * std.time.ns_per_ms);

    // Client setup
    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer client_loop.deinit();

    const client_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(client_key);

    var client_transport: quic.QuicTransport = undefined;
    try client_transport.init(&client_loop, client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer client_switch.deinit();

    var client_ping_handler = PingProtocolHandler.init(allocator);
    defer client_ping_handler.deinit();
    try client_switch.addProtocolHandler("/ipfs/ping/1.0.0", client_ping_handler.any());

    var callback: TestNewStreamCallback = .{ .mutex = .{}, .sender = undefined };

    var dial_ma = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9200");
    defer dial_ma.deinit();
    try dial_ma.push(.{ .P2P = server_peer_id });

    client_switch.newStream(
        dial_ma,
        &.{"/ipfs/ping/1.0.0"},
        &callback,
        TestNewStreamCallback.callback,
    );

    callback.mutex.wait();

    // Send ping
    var payload: [PING_SIZE]u8 = undefined;
    std.crypto.random.bytes(&payload);

    const PingSendCallback = struct {
        mutex: std.Thread.ResetEvent,
        result: anyerror!usize,

        const Self = @This();
        pub fn callback_(ctx: ?*anyopaque, res: anyerror!usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.result = res;
            self.mutex.set();
        }
    };

    var send_callback = PingSendCallback{
        .mutex = .{},
        .result = undefined,
    };

    try callback.sender.ping(&payload, &send_callback, PingSendCallback.callback_);
    send_callback.mutex.wait();

    const size = try send_callback.result;
    try std.testing.expectEqual(PING_SIZE, size);

    // Give time for response to arrive
    std.time.sleep(1000 * std.time.ns_per_ms);
}

test "ping protocol periodic ping/pong" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.log.warn("Memory leak detected in test!", .{});
        }
    }
    const allocator = gpa.allocator();

    const server_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9201");
    defer server_listen_address.deinit();

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer server_loop.deinit();

    const server_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(server_key);

    var server_transport: quic.QuicTransport = undefined;
    try server_transport.init(&server_loop, server_key, keys.KeyType.ED25519, allocator);

    var server_pubkey = try tls.createProtobufEncodedPublicKey(allocator, server_key);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer server_switch.deinit();

    var ping_handler = PingProtocolHandler.init(allocator);
    defer ping_handler.deinit();
    try server_switch.addProtocolHandler("/ipfs/ping/1.0.0", ping_handler.any());

    try server_switch.listen(server_listen_address, null, struct {
        pub fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}
    }.callback);

    std.time.sleep(200 * std.time.ns_per_ms);

    // Client setup
    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer client_loop.deinit();

    const client_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(client_key);

    var client_transport: quic.QuicTransport = undefined;
    try client_transport.init(&client_loop, client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer client_switch.deinit();

    var client_ping_handler = PingProtocolHandler.init(allocator);
    defer client_ping_handler.deinit();
    try client_switch.addProtocolHandler("/ipfs/ping/1.0.0", client_ping_handler.any());

    var callback: TestNewStreamCallback = .{ .mutex = .{}, .sender = undefined };

    var dial_ma = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9201");
    defer dial_ma.deinit();
    try dial_ma.push(.{ .P2P = server_peer_id });

    client_switch.newStream(
        dial_ma,
        &.{"/ipfs/ping/1.0.0"},
        &callback,
        TestNewStreamCallback.callback,
    );

    callback.mutex.wait();

    // Send multiple pings in a loop
    const NUM_PINGS = 10;
    const PingSendCallback = struct {
        mutex: std.Thread.ResetEvent,
        result: anyerror!usize,

        const Self = @This();
        pub fn callback_(ctx: ?*anyopaque, res: anyerror!usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.result = res;
            self.mutex.set();
        }
    };

    std.debug.print("Starting {} periodic ping/pong cycles...\n", .{NUM_PINGS});

    for (0..NUM_PINGS) |i| {
        var payload: [PING_SIZE]u8 = undefined;
        std.crypto.random.bytes(&payload);

        var send_callback = PingSendCallback{
            .mutex = .{},
            .result = undefined,
        };

        try callback.sender.ping(&payload, &send_callback, PingSendCallback.callback_);
        send_callback.mutex.wait();

        const size = send_callback.result catch |err| {
            std.debug.print("Failed to send ping {d}: {s}\n", .{ i, @errorName(err) });
            return err;
        };

        try std.testing.expectEqual(PING_SIZE, size);
        std.debug.print("Ping/pong cycle {} completed successfully\n", .{i + 1});

        // Small delay between pings to simulate periodic behavior
        std.time.sleep(100 * std.time.ns_per_ms);
    }

    std.debug.print("Successfully completed {} ping/pong cycles!\n", .{NUM_PINGS});

    // Give time for final responses to arrive
    std.time.sleep(500 * std.time.ns_per_ms);
}
