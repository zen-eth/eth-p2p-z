const std = @import("std");
const libp2p = @import("../../root.zig");
const protocols = libp2p.protocols;
const PeerId = @import("peer-id").PeerId;
const quic = libp2p.transport.quic;
const rpc = libp2p.protobuf.rpc;
const PubSub = @import("pubsub.zig").PubSub;
const uvarint = @import("multiformats").uvarint;

const max_message_size = 1024 * 1024;
const u64_max_uvarint_bytes = 10;

/// The Semiduplex struct represents a bidirectional communication channel
/// between two peers in a PubSub network. It consists of two halves:
/// the initiator (read half) and the responder (write half). Each half is
/// represented by a PubSubPeerInitiator and PubSubPeerResponder respectively.
/// The Semiduplex struct manages the lifecycle of these two halves and provides
/// methods to close the connection gracefully.
pub const Semiduplex = struct {
    /// The read half of the semiduplex stream.
    initiator: ?*PubSubPeerInitiator,
    /// The write half of the semiduplex stream.
    responder: ?*PubSubPeerResponder,

    allocator: std.mem.Allocator,
    /// Indicates whether the close operation is initiated by the application layer.
    active_close: bool = false,

    replace_stream: bool = false,

    const Self = @This();

    pub fn close(self: *Self, s_callback_ctx: ?*anyopaque, s_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*Semiduplex) void) void {
        if (self.active_close) {
            s_callback(s_callback_ctx, self);
            return;
        }

        self.active_close = true;

        const current_initiator = self.initiator;
        const current_responder = self.responder;

        if (current_initiator) |init| {
            init.stream.close(null, struct {
                fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.callback);
        }

        if (current_responder) |resp| {
            resp.stream.close(null, struct {
                fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.callback);
        }
        s_callback(s_callback_ctx, self);
    }
};

pub const PubSubPeerProtocolHandler = struct {
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
        stream: *libp2p.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(PubSubPeerInitiator) catch unreachable;
        handler.* = .{
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
            .stream = stream,
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *libp2p.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(PubSubPeerResponder) catch unreachable;
        handler.* = .{
            .pubsub = undefined,
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
            .stream = stream,
            .received_buffer = std.fifo.LinearFifo(u8, .Dynamic).init(self.allocator),
        };
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn vtableOnResponderStartFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onResponderStart(stream, callback_ctx, callback);
    }

    pub fn vtableOnInitiatorStartFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
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

pub const PubSubPeerInitiator = struct {
    callback_ctx: ?*anyopaque,

    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

    allocator: std.mem.Allocator,

    stream: *libp2p.QuicStream,

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *libp2p.QuicStream) anyerror!void {
        self.stream = stream;
        self.callback(self.callback_ctx, self);
    }

    pub fn onMessage(_: *Self, _: *libp2p.QuicStream, msg: []const u8) anyerror!void {
        std.log.warn("Write stream received a message with size: {d}", .{msg.len});
    }

    pub fn onClose(self: *Self, _: *libp2p.QuicStream) anyerror!void {
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn vtableOnActivatedFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    pub fn vtableOnMessageFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
        message: []const u8,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    pub fn vtableOnCloseFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
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

pub const PubSubPeerResponder = struct {
    callback_ctx: ?*anyopaque,

    callback: *const fn (ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

    allocator: std.mem.Allocator,

    stream: *libp2p.QuicStream,

    pubsub: *PubSub,

    received_buffer: std.fifo.LinearFifo(u8, .Dynamic),

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *libp2p.QuicStream) anyerror!void {
        self.stream = stream;
        self.callback(self.callback_ctx, self);
    }

    pub fn onMessage(self: *Self, _: *libp2p.QuicStream, message: []const u8) anyerror!void {
        try self.received_buffer.write(message);

        while (true) {
            const readable_bytes = self.received_buffer.readableSlice(0);
            const result = uvarint.decode(usize, readable_bytes) catch |err| {
                switch (err) {
                    uvarint.VarintParseError.Insufficient => return,
                    else => return err,
                }
            };
            const msg_len = result.value;
            const remaining = result.remaining;

            if (msg_len > max_message_size) {
                return error.MessageTooLarge;
            }

            const msg_len_size = readable_bytes.len - remaining.len;
            const total_need = msg_len_size + msg_len;

            if (self.received_buffer.readableLength() < total_need) {
                return;
            }

            const copied_message = try self.pubsub.allocator.alloc(u8, msg_len);
            defer self.pubsub.allocator.free(copied_message);
            self.received_buffer.discard(msg_len_size);
            const bytes_read = self.received_buffer.read(copied_message);
            std.debug.assert(bytes_read == msg_len);

            const rpc_reader = try rpc.RPCReader.init(self.pubsub.allocator, copied_message);
            try self.pubsub.incoming_rpc.append(self.pubsub.allocator, rpc_reader);
        }
    }

    pub fn onClose(self: *Self, _: *libp2p.QuicStream) anyerror!void {
        const allocator = self.allocator;
        self.received_buffer.deinit();
        allocator.destroy(self);
    }

    pub fn vtableOnActivatedFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActivated(stream);
    }

    pub fn vtableOnMessageFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
        message: []const u8,
    ) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onMessage(stream, message);
    }

    pub fn vtableOnCloseFn(
        instance: *anyopaque,
        stream: *libp2p.QuicStream,
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
