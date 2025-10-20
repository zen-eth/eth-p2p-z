const std = @import("std");
const libp2p = @import("../root.zig");
const protocols = libp2p.protocols;
const quic = libp2p.transport.quic;
const swarm = libp2p.swarm;
const PeerId = @import("peer_id").PeerId;
const io_loop = libp2p.thread_event_loop;
const xev = @import("xev");

/// Protocol identifier for libp2p ping.
pub const protocol_id = "/ipfs/ping/1.0.0";

/// Length of a ping payload in bytes.
pub const payload_length: usize = 32;

/// Default timeout for a ping round trip (10 seconds).
pub const default_timeout_ns: u64 = 10 * std.time.ns_per_s;

/// Ping protocol handler that can be registered with the multistream-select handler.
pub const PingProtocolHandler = struct {
    allocator: std.mem.Allocator,
    outbound_streams: std.StringHashMap(usize),
    inbound_streams: std.StringHashMap(usize),
    shutting_down: bool = false,
    network_switch: *swarm.Switch,
    responder_behavior: ResponderBehavior,

    const Self = @This();

    pub const ResponderBehavior = struct {
        drop_responses: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, network_switch: *swarm.Switch) Self {
        return Self.initWithBehavior(allocator, network_switch, .{});
    }

    pub fn initWithBehavior(
        allocator: std.mem.Allocator,
        network_switch: *swarm.Switch,
        behavior: ResponderBehavior,
    ) Self {
        return .{
            .allocator = allocator,
            .outbound_streams = std.StringHashMap(usize).init(allocator),
            .inbound_streams = std.StringHashMap(usize).init(allocator),
            .shutting_down = false,
            .network_switch = network_switch,
            .responder_behavior = behavior,
        };
    }

    pub fn deinit(self: *Self) void {
        self.shutting_down = true;
        var out_iter = self.outbound_streams.iterator();
        while (out_iter.next()) |entry| {
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.outbound_streams.deinit();

        var in_iter = self.inbound_streams.iterator();
        while (in_iter.next()) |entry| {
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.inbound_streams.deinit();
    }

    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const peer_key = self.acquireOutboundSlot(stream) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };

        const handler = self.allocator.create(PingInitiator) catch |err| {
            self.releaseOutboundSlot(peer_key);
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        handler.* = .{
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .stream = stream,
            .sender = null,
            .pending_request = null,
            .handler = self,
            .peer_key = peer_key,
            .timeout_timer = undefined,
            .timeout_completion = .{},
            .timeout_cancel_completion = .{},
            .timeout_active = false,
            .timeout_deadline_ms = 0,
            .recv_len = 0,
            .recv_buffer = undefined,
        };

        handler.timeout_timer = xev.Timer.init() catch |err| {
            self.releaseOutboundSlot(peer_key);
            self.allocator.destroy(handler);
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const peer_key = self.acquireInboundSlot(stream) catch |err| {
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };

        const handler = self.allocator.create(PingResponder) catch |err| {
            self.releaseInboundSlot(peer_key);
            callback(callback_ctx, err);
            stream.close(null, struct {
                fn noop(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.noop);
            return err;
        };
        handler.* = .{
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .stream = stream,
            .handler = self,
            .peer_key = peer_key,
            .pending_len = 0,
            .pending_payload = undefined,
            .behavior = self.responder_behavior,
        };

        stream.setProtoMsgHandler(handler.any());

        // Notify the listener that the responder is ready. We simply pass the handler pointer
        // so the listener can observe lifecycle events if desired.
        handler.callback(handler.callback_ctx, handler);
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

    fn acquireOutboundSlot(self: *Self, stream: *quic.QuicStream) ![]const u8 {
        return self.acquireSlot(&self.outbound_streams, 1, stream) catch |err| switch (err) {
            error.PingStreamLimitExceeded => error.OutboundPingStreamLimitExceeded,
            else => err,
        };
    }

    fn releaseOutboundSlot(self: *Self, key: []const u8) void {
        self.releaseSlot(&self.outbound_streams, key);
    }

    fn acquireInboundSlot(self: *Self, stream: *quic.QuicStream) ![]const u8 {
        return self.acquireSlot(&self.inbound_streams, 2, stream) catch |err| switch (err) {
            error.PingStreamLimitExceeded => error.InboundPingStreamLimitExceeded,
            else => err,
        };
    }

    fn releaseInboundSlot(self: *Self, key: []const u8) void {
        self.releaseSlot(&self.inbound_streams, key);
    }

    fn acquireSlot(self: *Self, map: *std.StringHashMap(usize), limit: usize, stream: *quic.QuicStream) ![]const u8 {
        if (self.shutting_down) return error.HandlerShutdown;

        const session = stream.conn.security_session orelse return error.MissingRemotePeerId;
        const key_buf = try self.peerKeyFromPeerId(&session.remote_id);

        const gop = try map.getOrPut(key_buf);
        if (gop.found_existing) {
            self.allocator.free(key_buf);
            if (gop.value_ptr.* >= limit) {
                return error.PingStreamLimitExceeded;
            }
            gop.value_ptr.* += 1;
            return gop.key_ptr.*;
        }

        // Newly inserted; initialize counter.
        gop.value_ptr.* = 1;
        return gop.key_ptr.*;
    }

    fn releaseSlot(self: *Self, map: *std.StringHashMap(usize), key: []const u8) void {
        if (self.shutting_down) return;
        if (map.getPtr(key)) |count_ptr| {
            if (count_ptr.* <= 1) {
                const removed = map.fetchRemove(key).?;
                self.allocator.free(@constCast(removed.key));
            } else {
                count_ptr.* -= 1;
            }
        }
    }

    fn peerKeyFromPeerId(self: *Self, peer_id: *const PeerId) ![]const u8 {
        const len = peer_id.toBase58Len();
        const buf = try self.allocator.alloc(u8, len);
        defer self.allocator.free(buf);

        const encoded = try peer_id.toBase58(buf);
        return try self.allocator.dupe(u8, encoded);
    }

    fn eventLoop(self: *Self) *io_loop.ThreadEventLoop {
        return self.network_switch.transport.io_event_loop;
    }
};

/// Result callback signature for ping requests. The callback is invoked with the RTT (in nanoseconds)
/// or an error describing why the ping failed.
pub const PingResultCallback = *const fn (ctx: ?*anyopaque, res: anyerror!u64) void;

/// Controller returned to initiators that allows issuing ping requests over an established stream.
pub const PingSender = struct {
    initiator: *PingInitiator,

    const Self = @This();

    pub fn ping(self: *Self, timeout_ns: u64, callback_ctx: ?*anyopaque, callback: PingResultCallback) !void {
        try self.initiator.beginPing(timeout_ns, callback_ctx, callback);
    }

    pub fn close(self: *Self, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void) void {
        self.initiator.stream.close(callback_ctx, callback);
    }
};

const PingRequest = struct {
    payload: [payload_length]u8,
    timer: std.time.Timer,
    timeout_ns: u64,
    callback_ctx: ?*anyopaque,
    callback: PingResultCallback,
};

/// Handles ping messages on the initiator side.
const PingInitiator = struct {
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    stream: *quic.QuicStream,
    sender: ?*PingSender,
    pending_request: ?*PingRequest,
    handler: *PingProtocolHandler,
    peer_key: []const u8,
    timeout_timer: xev.Timer,
    timeout_completion: xev.Completion,
    timeout_cancel_completion: xev.Completion,
    timeout_active: bool = false,
    timeout_deadline_ms: u64 = 0,
    recv_buffer: [payload_length]u8 = undefined,
    recv_len: usize = 0,

    const Self = @This();

    fn beginPing(self: *Self, timeout_ns: u64, callback_ctx: ?*anyopaque, callback: PingResultCallback) !void {
        if (self.pending_request != null) {
            return error.PingInProgress;
        }

        var request = try self.allocator.create(PingRequest);
        errdefer self.allocator.destroy(request);

        request.timeout_ns = if (timeout_ns == 0) default_timeout_ns else timeout_ns;
        request.callback_ctx = callback_ctx;
        request.callback = callback;
        request.timer = try std.time.Timer.start();

        std.crypto.random.bytes(&request.payload);

        self.pending_request = request;
        self.recv_len = 0;
        errdefer {
            self.pending_request = null;
            self.recv_len = 0;
        }

        try self.startTimeout(request.timeout_ns);

        self.stream.write(request.payload[0..], self, writeCompleteCallback);
    }

    fn startTimeout(self: *Self, timeout_ns: u64) !void {
        const timeout_ms = std.math.divCeil(u64, timeout_ns, std.time.ns_per_ms) catch unreachable;
        self.timeout_deadline_ms = if (timeout_ms == 0) 1 else timeout_ms;
        self.timeout_active = true;
        errdefer {
            self.timeout_active = false;
            self.timeout_deadline_ms = 0;
        }

        const loop = self.handler.eventLoop();
        if (loop.inEventLoopThread()) {
            self.armTimeout(loop);
        } else {
            try loop.queueCall(Self, self, startTimeoutOnLoop);
        }
    }

    fn startTimeoutOnLoop(loop: *io_loop.ThreadEventLoop, self: *Self) void {
        self.armTimeout(loop);
    }

    fn armTimeout(self: *Self, loop: *io_loop.ThreadEventLoop) void {
        self.timeout_timer.reset(
            &loop.loop,
            &self.timeout_completion,
            &self.timeout_cancel_completion,
            self.timeout_deadline_ms,
            Self,
            self,
            timeoutTimerCallback,
        );
    }

    fn writeCompleteCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        const written = res catch |err| {
            self.failPending(err);
            return;
        };

        if (written != payload_length) {
            self.failPending(error.IncompletePingWrite);
        }
    }

    fn failPending(self: *Self, err: anyerror) void {
        self.cancelTimeout();
        if (self.pending_request) |request| {
            const cb = request.callback;
            const cb_ctx = request.callback_ctx;
            self.pending_request = null;
            self.recv_len = 0;
            self.allocator.destroy(request);
            cb(cb_ctx, err);
        }
    }

    fn succeedPending(self: *Self, elapsed_ns: u64) void {
        self.cancelTimeout();
        if (self.pending_request) |request| {
            const cb = request.callback;
            const cb_ctx = request.callback_ctx;
            self.pending_request = null;
            self.allocator.destroy(request);
            cb(cb_ctx, elapsed_ns);
        }
    }

    fn cancelTimeout(self: *Self) void {
        if (!self.timeout_active) return;

        self.timeout_active = false;
        self.timeout_deadline_ms = 0;

        const loop = self.handler.eventLoop();
        if (loop.inEventLoopThread()) {
            self.timeout_timer.cancel(
                &loop.loop,
                &self.timeout_completion,
                &self.timeout_cancel_completion,
                Self,
                self,
                timeoutCancelCallback,
            );
        } else {
            loop.queueCall(Self, self, cancelTimeoutOnLoop) catch |err| {
                std.log.warn("failed to queue ping timeout cancel: {any}", .{err});
            };
        }
    }

    fn cancelTimeoutOnLoop(loop: *io_loop.ThreadEventLoop, self: *Self) void {
        self.timeout_timer.cancel(
            &loop.loop,
            &self.timeout_completion,
            &self.timeout_cancel_completion,
            Self,
            self,
            timeoutCancelCallback,
        );
    }

    fn timeoutTimerCallback(
        ctx: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        if (loop.stopped()) return .disarm;

        const self = ctx.?;

        _ = r catch |err| {
            if (err == error.Canceled) return .disarm;
            std.log.warn("ping timeout timer failed: {}", .{err});
            return .disarm;
        };

        if (!self.timeout_active) return .disarm;

        self.timeout_active = false;
        self.timeout_deadline_ms = 0;
        self.failPending(error.PingTimeout);
        return .disarm;
    }

    fn timeoutCancelCallback(
        _: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        _: xev.Timer.CancelError!void,
    ) xev.CallbackAction {
        if (loop.stopped()) return .disarm;
        return .disarm;
    }

    fn onActivated(self: *Self, _: *quic.QuicStream) anyerror!void {
        const sender = self.allocator.create(PingSender) catch |err| {
            self.callback(self.callback_ctx, err);
            return err;
        };
        sender.* = .{ .initiator = self };
        self.sender = sender;

        self.callback(self.callback_ctx, sender);
    }

    fn onMessage(self: *Self, _: *quic.QuicStream, message: []const u8) anyerror!void {
        if (self.pending_request == null) {
            return error.UnexpectedPingResponse;
        }

        if (message.len == 0) return;

        if (self.recv_len + message.len > payload_length) {
            self.failPending(error.InvalidPingResponse);
            return error.InvalidPingResponse;
        }

        std.mem.copyForwards(u8, self.recv_buffer[self.recv_len .. self.recv_len + message.len], message);
        self.recv_len += message.len;

        if (self.recv_len < payload_length) {
            return;
        }

        const request = self.pending_request.?;
        if (!std.mem.eql(u8, &request.payload, self.recv_buffer[0..payload_length])) {
            self.failPending(error.InvalidPingResponse);
            return error.InvalidPingResponse;
        }

        const elapsed_ns = request.timer.read();
        self.recv_len = 0;

        self.succeedPending(elapsed_ns);
    }

    fn onClose(self: *Self, _: *quic.QuicStream) anyerror!void {
        self.failPending(error.StreamClosed);

        if (self.sender) |sender| {
            self.allocator.destroy(sender);
            self.sender = null;
        }
        self.handler.releaseOutboundSlot(self.peer_key);
        self.cancelTimeout();
        self.timeout_timer.deinit();
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

/// Handles ping messages on the responder side.
const PingResponder = struct {
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    stream: *quic.QuicStream,
    handler: *PingProtocolHandler,
    peer_key: []const u8,
    pending_payload: [payload_length]u8 = undefined,
    pending_len: usize = 0,
    behavior: PingProtocolHandler.ResponderBehavior,

    const Self = @This();

    fn onActivated(self: *Self, _: *quic.QuicStream) anyerror!void {
        self.callback(self.callback_ctx, self);
    }

    fn onMessage(self: *Self, stream: *quic.QuicStream, message: []const u8) anyerror!void {
        if (message.len == 0) return;

        if (self.pending_len + message.len > payload_length) {
            self.pending_len = 0;
            return error.InvalidPingRequest;
        }

        std.mem.copyForwards(u8, self.pending_payload[self.pending_len .. self.pending_len + message.len], message);
        self.pending_len += message.len;

        if (self.pending_len < payload_length) {
            return;
        }

        if (self.behavior.drop_responses) {
            self.pending_len = 0;
            return;
        }

        stream.write(self.pending_payload[0..payload_length], self, writeCallback);
    }

    fn onClose(self: *Self, _: *quic.QuicStream) anyerror!void {
        self.handler.releaseInboundSlot(self.peer_key);
        self.allocator.destroy(self);
    }

    fn writeCallback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        // Just log the error if the responder fails to echo the payload.
        _ = res catch |err| {
            std.log.warn("Failed to echo ping payload: {any}", .{err});
            self.pending_len = 0;
            return;
        };
        self.pending_len = 0;
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

test "ping protocol round trip" {
    const allocator = std.testing.allocator;
    const tls = libp2p.security.tls;
    const ssl = @import("ssl");
    const keys = @import("peer_id").keys;
    const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer server_loop.deinit();

    const server_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(server_key);

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer server_switch.deinit();

    var server_ping = PingProtocolHandler.init(allocator, &server_switch);
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try tls.createProtobufEncodedPublicKey(allocator, server_key);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    const listen_addr = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9400");
    defer listen_addr.deinit();

    try server_switch.listen(listen_addr, null, struct {
        fn onStream(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
            _ = res catch {};
        }
    }.onStream);

    std.time.sleep(200 * std.time.ns_per_ms);

    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer client_loop.deinit();

    const client_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(client_key);

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer client_switch.deinit();

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9400");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const StreamCtx = struct {
        event: std.Thread.ResetEvent,
        sender: ?*PingSender = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const controller = res catch {
                self.event.set();
                return;
            };
            self.sender = @ptrCast(@alignCast(controller.?));
            self.event.set();
        }
    };

    var stream_ctx = StreamCtx{ .event = .{} };

    client_switch.newStream(dial_addr, &.{protocol_id}, &stream_ctx, StreamCtx.callback);
    stream_ctx.event.wait();

    const sender = stream_ctx.sender orelse return error.UnableToOpenPingStream;

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent,
        result_ns: ?u64 = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.result_ns = res catch |err| {
                self.result_ns = null;
                std.log.warn("ping failed: {any}", .{err});
                self.event.set();
                return;
            };
            self.event.set();
        }
    };

    var ping_ctx = PingResultCtx{ .event = .{} };
    try sender.ping(default_timeout_ns, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();

    const rtt = ping_ctx.result_ns orelse return error.PingFailed;
    try std.testing.expect(rtt > 0);

    sender.close(null, struct {
        fn onClosed(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
    }.onClosed);

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "ping protocol timeout" {
    const allocator = std.testing.allocator;
    const tls = libp2p.security.tls;
    const ssl = @import("ssl");
    const keys = @import("peer_id").keys;
    const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer server_loop.deinit();

    const server_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(server_key);

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer server_switch.deinit();

    var server_ping = PingProtocolHandler.initWithBehavior(allocator, &server_switch, .{ .drop_responses = true });
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try tls.createProtobufEncodedPublicKey(allocator, server_key);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    const listen_addr = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9405");
    defer listen_addr.deinit();

    try server_switch.listen(listen_addr, null, struct {
        fn onStream(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
            _ = res catch {};
        }
    }.onStream);

    std.time.sleep(200 * std.time.ns_per_ms);

    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer client_loop.deinit();

    const client_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(client_key);

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer client_switch.deinit();

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9405");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const StreamCtx = struct {
        event: std.Thread.ResetEvent,
        sender: ?*PingSender = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const controller = res catch {
                self.event.set();
                return;
            };
            self.sender = @ptrCast(@alignCast(controller.?));
            self.event.set();
        }
    };

    var stream_ctx = StreamCtx{ .event = .{} };

    client_switch.newStream(dial_addr, &.{protocol_id}, &stream_ctx, StreamCtx.callback);
    stream_ctx.event.wait();

    const sender = stream_ctx.sender orelse return error.UnableToOpenPingStream;

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent,
        result_ns: ?u64 = null,
        err: ?anyerror = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.result_ns = res catch |err| {
                self.result_ns = null;
                self.err = err;
                self.event.set();
                return;
            };
            self.err = null;
            self.event.set();
        }
    };

    var ping_ctx = PingResultCtx{ .event = .{} };
    const short_timeout_ns = 200 * std.time.ns_per_ms;
    try sender.ping(short_timeout_ns, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();

    try std.testing.expectEqual(@as(?u64, null), ping_ctx.result_ns);
    try std.testing.expect(ping_ctx.err != null);
    try std.testing.expectEqual(error.PingTimeout, ping_ctx.err.?);

    sender.close(null, struct {
        fn onClosed(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
    }.onClosed);

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "ping protocol periodic pings" {
    const allocator = std.testing.allocator;
    const tls = libp2p.security.tls;
    const ssl = @import("ssl");
    const keys = @import("peer_id").keys;
    const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer server_loop.deinit();

    const server_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(server_key);

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer server_switch.deinit();

    var server_ping = PingProtocolHandler.init(allocator, &server_switch);
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try tls.createProtobufEncodedPublicKey(allocator, server_key);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    const listen_addr = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9410");
    defer listen_addr.deinit();

    try server_switch.listen(listen_addr, null, struct {
        fn onStream(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
            _ = res catch {};
        }
    }.onStream);

    std.time.sleep(200 * std.time.ns_per_ms);

    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer client_loop.deinit();

    const client_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(client_key);

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer client_switch.deinit();

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9410");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const StreamCtx = struct {
        event: std.Thread.ResetEvent,
        sender: ?*PingSender = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const controller = res catch {
                self.event.set();
                return;
            };
            self.sender = @ptrCast(@alignCast(controller.?));
            self.event.set();
        }
    };

    var stream_ctx = StreamCtx{ .event = .{} };

    client_switch.newStream(dial_addr, &.{protocol_id}, &stream_ctx, StreamCtx.callback);
    stream_ctx.event.wait();

    const sender = stream_ctx.sender orelse return error.UnableToOpenPingStream;

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent,
        result_ns: ?u64 = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.result_ns = res catch |err| {
                self.result_ns = null;
                std.log.warn("ping failed: {any}", .{err});
                self.event.set();
                return;
            };
            self.event.set();
        }
    };

    const iterations = 5;
    for (0..iterations) |_| {
        var ping_ctx = PingResultCtx{ .event = .{} };
        try sender.ping(default_timeout_ns, &ping_ctx, PingResultCtx.callback);
        ping_ctx.event.wait();

        const rtt = ping_ctx.result_ns orelse return error.PingFailed;
        try std.testing.expect(rtt > 0);

        std.time.sleep(100 * std.time.ns_per_ms);
    }

    sender.close(null, struct {
        fn onClosed(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
    }.onClosed);

    std.time.sleep(200 * std.time.ns_per_ms);
}
