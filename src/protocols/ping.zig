const std = @import("std");
const libp2p = @import("../root.zig");
const protocols = libp2p.protocols;
const quic = libp2p.transport.quic;
const swarm = libp2p.swarm;
const multiaddr = @import("multiformats").multiaddr;
const Multiaddr = multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;
const io_loop = libp2p.thread_event_loop;
const xev = libp2p.xev;
const identity = libp2p.identity;
const keys = @import("peer_id").keys;
const Allocator = std.mem.Allocator;
const Atomic = std.atomic;

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
            .stream = stream,
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
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
            .pending_loop_calls = Atomic.Value(usize).init(0),
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
            .controller = undefined,
            .stream = stream,
            .allocator = self.allocator,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .handler = self,
            .peer_key = peer_key,
            .pending_len = 0,
            .pending_payload = undefined,
            .behavior = self.responder_behavior,
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

/// Controller for a single ping stream. Callers receive this handle and are responsible for
/// closing the underlying QUIC stream when finished.
pub const PingStream = struct {
    controller: protocols.ProtocolStreamController = undefined,
    stream: ?*quic.QuicStream = null,
    initiator: ?*PingInitiator = null,
    allocator: Allocator,

    const Self = @This();

    pub const ResultCallback = *const fn (ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void;

    const stream_controller_vtable = protocols.ProtocolStreamControllerVTable{
        .getStreamFn = controllerGetStream,
    };

    fn controllerGetStream(instance: *anyopaque) *quic.QuicStream {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.stream.?;
    }

    pub fn init(stream: *quic.QuicStream, initiator: *PingInitiator, allocator: Allocator) Self {
        return .{
            .controller = undefined,
            .stream = stream,
            .initiator = initiator,
            .allocator = allocator,
        };
    }

    pub fn setup(self: *Self) void {
        const instance: *anyopaque = @ptrCast(self);
        self.controller = protocols.initStreamController(instance, &stream_controller_vtable);
    }

    pub fn close(
        self: *Self,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void,
    ) void {
        if (self.stream) |stream_| {
            stream_.close(callback_ctx, callback);
        } else {
            callback(callback_ctx, error.NoActiveStream);
        }
    }
};

/// Service facade responsible for dialing peers and starting ping streams.
pub const PingService = struct {
    allocator: Allocator,
    network_switch: *swarm.Switch,

    const Self = @This();

    pub fn init(allocator: Allocator, network_switch: *swarm.Switch) Self {
        return .{
            .allocator = allocator,
            .network_switch = network_switch,
        };
    }

    pub fn ping(
        self: *Self,
        address: Multiaddr,
        options: PingOptions,
        callback_ctx: ?*anyopaque,
        callback: PingStream.ResultCallback,
    ) !void {
        const addr_buf: ?[]u8 = try address.toString(self.allocator);
        defer if (addr_buf) |buf| self.allocator.free(buf);

        var check_addr = address;
        try requirePeerComponent(&check_addr);

        const ctx = try self.allocator.create(ServicePingRequestCtx);
        ctx.* = .{
            .service = self,
            .timeout_ns = if (options.timeout_ns == 0) default_timeout_ns else options.timeout_ns,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .stream_sender = null,
            .completed = false,
            .result_ns = 0,
            .result_err = null,
            .result_ready = false,
            .result_delivered = false,
        };

        self.network_switch.newStream(address, &.{protocol_id}, ctx, ServicePingRequestCtx.streamOpened);
    }

    fn requirePeerComponent(address: *Multiaddr) !void {
        var iter = address.iterator();
        while (try iter.next()) |protocol| {
            switch (protocol) {
                .P2P => return,
                else => continue,
            }
        }

        return error.MissingPeerComponent;
    }

    const ServicePingRequestCtx = struct {
        service: *Self,
        timeout_ns: u64,
        callback_ctx: ?*anyopaque,
        callback: PingStream.ResultCallback,
        stream_sender: ?*PingStream,
        completed: bool,
        result_ns: u64,
        result_err: ?anyerror,
        result_ready: bool,
        result_delivered: bool,

        fn streamOpened(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *ServicePingRequestCtx = @ptrCast(@alignCast(ctx.?));
            const controller = res catch |err| {
                self.fail(err);
                return;
            };

            if (controller == null) {
                self.fail(error.MissingPingStream);
                return;
            }

            const sender: *PingStream = @ptrCast(@alignCast(controller.?));
            self.stream_sender = sender;

            self.beginPing() catch |err| {
                self.fail(err);
                return;
            };
        }

        fn beginPing(self: *ServicePingRequestCtx) !void {
            const sender = self.stream_sender orelse return error.MissingPingStream;
            const initiator = sender.initiator orelse return error.MissingPingInitiator;
            try initiator.beginPing(self.timeout_ns, self, pingCompleted);
        }

        fn pingCompleted(ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void {
            const self: *ServicePingRequestCtx = @ptrCast(@alignCast(ctx.?));
            if (sender) |sender_ref| {
                self.stream_sender = sender_ref;
            }
            if (!self.completed) {
                self.completed = true;
            }

            const rtt = res catch |err| {
                self.result_err = err;
                self.result_ready = true;
                self.finish();
                return;
            };

            self.result_ns = rtt;
            self.result_err = null;
            self.result_ready = true;
            self.finish();
        }

        fn fail(self: *ServicePingRequestCtx, err: anyerror) void {
            if (!self.completed) {
                self.completed = true;
            }

            if (!self.result_ready) {
                self.result_err = err;
                self.result_ready = true;
            }
            self.finish();
        }

        fn finish(self: *ServicePingRequestCtx) void {
            self.deliverResult();
            self.cleanup();
        }

        fn deliverResult(self: *ServicePingRequestCtx) void {
            if (self.result_delivered) return;
            self.result_delivered = true;

            const sender = self.stream_sender;

            if (!self.result_ready) {
                self.callback(self.callback_ctx, sender, error.StreamClosed);
                return;
            }

            if (self.result_err) |err| {
                self.callback(self.callback_ctx, sender, err);
            } else {
                self.callback(self.callback_ctx, sender, self.result_ns);
            }
        }

        fn cleanup(self: *ServicePingRequestCtx) void {
            self.service.allocator.destroy(self);
        }
    };
};

/// Result callback signature for ping requests. The callback is invoked with the RTT (in nanoseconds)
/// or an error describing why the ping failed.
pub const PingResultCallback = PingStream.ResultCallback;

pub const PingOptions = struct {
    timeout_ns: u64 = default_timeout_ns,
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
    stream: *quic.QuicStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    sender: ?*PingStream,
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
    pending_loop_calls: Atomic.Value(usize),
    destroy_requested: bool = false,
    destroyed: bool = false,

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
            self.incrementLoopCall();
            loop.queueCall(Self, self, startTimeoutOnLoop) catch |err| {
                self.completeLoopCall();
                return err;
            };
        }
    }

    fn startTimeoutOnLoop(loop: *io_loop.ThreadEventLoop, self: *Self) void {
        self.armTimeout(loop);
        self.completeLoopCall();
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
            cb(cb_ctx, self.sender, err);
        }
    }

    fn succeedPending(self: *Self, elapsed_ns: u64) void {
        self.cancelTimeout();
        if (self.pending_request) |request| {
            const cb = request.callback;
            const cb_ctx = request.callback_ctx;
            self.pending_request = null;
            self.allocator.destroy(request);
            cb(cb_ctx, self.sender, elapsed_ns);
        }
    }

    fn incrementLoopCall(self: *Self) void {
        _ = self.pending_loop_calls.fetchAdd(1, .seq_cst);
    }

    fn completeLoopCall(self: *Self) void {
        const prev = self.pending_loop_calls.fetchSub(1, .seq_cst);
        std.debug.assert(prev > 0);
        self.maybeFinalizeDestroy();
    }

    fn maybeFinalizeDestroy(self: *Self) void {
        if (!self.destroy_requested or self.destroyed) return;
        if (self.pending_loop_calls.load(.seq_cst) != 0) return;

        self.destroyed = true;
        self.allocator.destroy(self);
    }

    fn scheduleDestroy(self: *Self, loop: *io_loop.ThreadEventLoop) void {
        self.incrementLoopCall();
        loop.queueCall(Self, self, destroyOnLoop) catch |err| {
            std.log.warn("failed to queue ping destroy: {any}", .{err});
            self.completeLoopCall();
        };
    }

    fn destroyOnLoop(_: *io_loop.ThreadEventLoop, self: *Self) void {
        self.completeLoopCall();
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
            self.incrementLoopCall();
            loop.queueCall(Self, self, cancelTimeoutOnLoop) catch |err| {
                std.log.warn("failed to queue ping timeout cancel: {any}", .{err});
                self.completeLoopCall();
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
        self.completeLoopCall();
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

    fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        self.stream = stream;
        const sender = self.allocator.create(PingStream) catch |err| {
            self.callback(self.callback_ctx, err);
            return err;
        };
        sender.* = PingStream.init(stream, self, self.allocator);
        sender.setup();
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
        const loop = self.handler.eventLoop();
        self.destroy_requested = true;
        self.scheduleDestroy(loop);
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
    controller: protocols.ProtocolStreamController,
    stream: *quic.QuicStream,
    allocator: std.mem.Allocator,
    callback_ctx: ?*anyopaque,
    callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    handler: *PingProtocolHandler,
    peer_key: []const u8,
    pending_payload: [payload_length]u8 = undefined,
    pending_len: usize = 0,
    behavior: PingProtocolHandler.ResponderBehavior,

    const Self = @This();

    const stream_controller_vtable = protocols.ProtocolStreamControllerVTable{
        .getStreamFn = controllerGetStream,
    };

    fn controllerGetStream(instance: *anyopaque) *quic.QuicStream {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.stream;
    }

    fn onActivated(self: *Self, stream: *quic.QuicStream) anyerror!void {
        self.stream = stream;
        const instance: *anyopaque = @ptrCast(self);
        self.controller = protocols.initStreamController(instance, &stream_controller_vtable);
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

test "ping listen callback exposes negotiated protocol" {
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer {
        server_loop.close();
        server_loop.deinit();
    }

    var server_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer server_key.deinit();

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, &server_key, keys.KeyType.ED25519, allocator);
    defer server_transport.deinit();

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer {
        server_switch.stop();
        server_switch.deinit();
    }

    var server_ping = PingProtocolHandler.init(allocator, &server_switch);
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9425");
    defer listen_addr.deinit();

    const ListenCtx = struct {
        event: std.Thread.ResetEvent = .{},
        protocol: ?[]const u8 = null,
        stream: ?*quic.QuicStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            defer self.event.set();

            const controller = res catch return;
            const stream = libp2p.protocols.getStream(controller) orelse return;
            self.protocol = stream.negotiated_protocol;
            self.stream = stream;
        }
    };

    var listen_ctx = ListenCtx{};
    try server_switch.listen(listen_addr, &listen_ctx, ListenCtx.callback);

    std.time.sleep(200 * std.time.ns_per_ms);

    var server_pubkey = try server_key.publicKey(allocator);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer {
        client_loop.close();
        client_loop.deinit();
    }

    var client_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer client_key.deinit();

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, &client_key, keys.KeyType.ED25519, allocator);
    defer client_transport.deinit();

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer {
        client_switch.stop();
        client_switch.deinit();
    }

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9425");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent = .{},
        result_ns: ?u64 = null,
        err: ?anyerror = null,
        sender: ?*PingStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.sender = sender;
            self.result_ns = res catch |err| {
                self.err = err;
                self.result_ns = null;
                self.event.set();
                return;
            };
            self.err = null;
            self.event.set();
        }
    };

    var ping_service = PingService.init(allocator, &client_switch);
    var ping_ctx = PingResultCtx{};
    try ping_service.ping(dial_addr, .{}, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();
    try std.testing.expect(ping_ctx.err == null);
    try std.testing.expect(ping_ctx.result_ns != null);

    if (ping_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            const Self = @This();

            fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                const self: *Self = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };

        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    listen_ctx.event.wait();
    try std.testing.expect(listen_ctx.protocol != null);
    try std.testing.expect(std.mem.eql(u8, listen_ctx.protocol.?, protocol_id));

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "ping protocol round trip" {
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer {
        server_loop.close();
        server_loop.deinit();
    }

    var server_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer server_key.deinit();

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, &server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer {
        server_switch.stop();
        server_switch.deinit();
    }

    var server_ping = PingProtocolHandler.init(allocator, &server_switch);
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try server_key.publicKey(allocator);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    const listen_addr = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9400");
    defer listen_addr.deinit();

    try server_switch.listen(listen_addr, null, struct {
        fn onStream(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const controller = res catch |err| {
                std.log.err("failed to accept incoming ping stream: {any}", .{err});
                return;
            };
            if (protocols.getStream(controller) == null) {
                std.log.err("controller did not expose a stream", .{});
                return;
            }
        }
    }.onStream);

    std.time.sleep(200 * std.time.ns_per_ms);

    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer {
        client_loop.close();
        client_loop.deinit();
    }

    var client_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer client_key.deinit();

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, &client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer {
        client_switch.stop();
        client_switch.deinit();
    }

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9400");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent = .{},
        result_ns: ?u64 = null,
        err: ?anyerror = null,
        sender: ?*PingStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.sender = sender;
            self.result_ns = res catch |err| {
                self.err = err;
                self.result_ns = null;
                self.event.set();
                return;
            };
            self.err = null;
            self.event.set();
        }
    };

    var ping_service = PingService.init(allocator, &client_switch);
    var ping_ctx = PingResultCtx{};
    try ping_service.ping(dial_addr, .{}, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();

    try std.testing.expect(ping_ctx.err == null);
    const rtt = ping_ctx.result_ns orelse return error.PingFailed;
    try std.testing.expect(rtt > 0);

    if (ping_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            const Self = @This();

            fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                const self: *Self = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };

        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "ping multiaddr round trip" {
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer {
        server_loop.close();
        server_loop.deinit();
    }

    var server_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer server_key.deinit();

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, &server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer {
        server_switch.stop();
        server_switch.deinit();
    }

    var server_ping = PingProtocolHandler.init(allocator, &server_switch);
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try server_key.publicKey(allocator);
    defer allocator.free(server_pubkey.data.?);
    const server_peer_id = try PeerId.fromPublicKey(allocator, &server_pubkey);

    const listen_addr = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9412");
    defer listen_addr.deinit();

    try server_switch.listen(listen_addr, null, struct {
        fn onStream(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
            _ = res catch {};
        }
    }.onStream);

    std.time.sleep(200 * std.time.ns_per_ms);

    var client_loop: io_loop.ThreadEventLoop = undefined;
    try client_loop.init(allocator);
    defer {
        client_loop.close();
        client_loop.deinit();
    }

    var client_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer client_key.deinit();

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, &client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer {
        client_switch.stop();
        client_switch.deinit();
    }

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9412");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent,
        result_ns: ?u64 = null,
        sender: ?*PingStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.sender = sender;
            self.result_ns = res catch |err| {
                std.log.warn("ping multiaddr failed: {any}", .{err});
                self.result_ns = null;
                self.event.set();
                return;
            };
            self.event.set();
        }
    };

    var ping_ctx = PingResultCtx{ .event = .{} };
    var ping_service = PingService.init(allocator, &client_switch);
    try ping_service.ping(dial_addr, .{}, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();

    const rtt = ping_ctx.result_ns orelse return error.PingFailed;
    try std.testing.expect(rtt > 0);

    if (ping_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            const Self = @This();

            fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                const self: *Self = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };

        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "ping protocol timeout" {
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer {
        server_loop.close();
        server_loop.deinit();
    }

    var server_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer server_key.deinit();

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, &server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer {
        server_switch.stop();
        server_switch.deinit();
    }

    var server_ping = PingProtocolHandler.initWithBehavior(allocator, &server_switch, .{ .drop_responses = true });
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try server_key.publicKey(allocator);
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
    defer {
        client_loop.close();
        client_loop.deinit();
    }

    var client_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer client_key.deinit();

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, &client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer {
        client_switch.stop();
        client_switch.deinit();
    }

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9405");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent = .{},
        result_ns: ?u64 = null,
        err: ?anyerror = null,
        sender: ?*PingStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.sender = sender;
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

    var ping_service = PingService.init(allocator, &client_switch);
    var ping_ctx = PingResultCtx{};
    const short_timeout_ns = 200 * std.time.ns_per_ms;
    try ping_service.ping(dial_addr, .{ .timeout_ns = short_timeout_ns }, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();

    try std.testing.expectEqual(@as(?u64, null), ping_ctx.result_ns);
    try std.testing.expect(ping_ctx.err != null);
    try std.testing.expectEqual(error.PingTimeout, ping_ctx.err.?);

    if (ping_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            const Self = @This();

            fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                const self: *Self = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };

        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "ping protocol periodic pings" {
    const allocator = std.testing.allocator;
    const quic_transport = libp2p.transport.quic;

    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(allocator);
    defer {
        server_loop.close();
        server_loop.deinit();
    }

    var server_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer server_key.deinit();

    var server_transport: quic_transport.QuicTransport = undefined;
    try server_transport.init(&server_loop, &server_key, keys.KeyType.ED25519, allocator);

    var server_switch: swarm.Switch = undefined;
    server_switch.init(allocator, &server_transport);
    defer {
        server_switch.stop();
        server_switch.deinit();
    }

    var server_ping = PingProtocolHandler.init(allocator, &server_switch);
    defer server_ping.deinit();

    try server_switch.addProtocolHandler(protocol_id, server_ping.any());

    var server_pubkey = try server_key.publicKey(allocator);
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
    defer {
        client_loop.close();
        client_loop.deinit();
    }

    var client_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer client_key.deinit();

    var client_transport: quic_transport.QuicTransport = undefined;
    try client_transport.init(&client_loop, &client_key, keys.KeyType.ED25519, allocator);

    var client_switch: swarm.Switch = undefined;
    client_switch.init(allocator, &client_transport);
    defer {
        client_switch.stop();
        client_switch.deinit();
    }

    var client_ping = PingProtocolHandler.init(allocator, &client_switch);
    defer client_ping.deinit();

    try client_switch.addProtocolHandler(protocol_id, client_ping.any());

    var dial_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9410");
    defer dial_addr.deinit();
    try dial_addr.push(.{ .P2P = server_peer_id });

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent,
        result_ns: ?u64 = null,
        sender: ?*PingStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, sender: ?*PingStream, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.sender = sender;
            self.result_ns = res catch |err| {
                self.result_ns = null;
                std.log.warn("ping failed: {any}", .{err});
                self.event.set();
                return;
            };
            self.event.set();
        }
    };

    var ping_service = PingService.init(allocator, &client_switch);
    const CloseCtx = struct {
        event: std.Thread.ResetEvent = .{},

        const Self = @This();

        fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.event.set();
        }
    };
    const iterations = 5;
    for (0..iterations) |_| {
        var ping_ctx = PingResultCtx{ .event = .{} };
        try ping_service.ping(dial_addr, .{}, &ping_ctx, PingResultCtx.callback);
        ping_ctx.event.wait();

        const rtt = ping_ctx.result_ns orelse return error.PingFailed;
        try std.testing.expect(rtt > 0);

        if (ping_ctx.sender) |sender| {
            var close_ctx = CloseCtx{};
            sender.close(&close_ctx, CloseCtx.callback);
            close_ctx.event.wait();
        }

        std.time.sleep(100 * std.time.ns_per_ms);
    }

    std.time.sleep(200 * std.time.ns_per_ms);
}
