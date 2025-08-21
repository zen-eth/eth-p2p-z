const std = @import("std");
const libp2p = @import("../../root.zig");
const protocols = libp2p.protocols;
const PeerId = @import("peer-id").PeerId;

pub const Semiduplex = struct {
    /// The read half of the semiduplex stream.
    initiator: ?*PubSubPeerInitiator,
    /// The write half of the semiduplex stream.
    responder: ?*PubSubPeerResponder,

    allocator: std.mem.Allocator,

    close_ctx: ?*anyopaque,

    close_callback: ?*const fn (ctx: ?*anyopaque, res: anyerror!*Semiduplex) void,

    const Self = @This();

    pub fn close(self: *Self, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!*Semiduplex) void) void {
        self.close_ctx = callback_ctx;
        self.close_callback = callback;

        if (self.initiator) |init| {
            init.stream.close(self, onOutgoingStreamClose);
        } else {
            if (self.responder) |resp| {
                resp.stream.close(self, onIncomingStreamClose);
            } else {
                std.log.warn("Both initiator and responder are null", .{});
            }
        }
    }

    fn onOutgoingStreamClose(ctx: ?*anyopaque, res: anyerror!*libp2p.QuicStream) void {
        const self: *Semiduplex = @ptrCast(@alignCast(ctx.?));
        _ = res catch unreachable;

        if (self.responder) |resp| {
            resp.stream.close(self, onIncomingStreamClose);
        } else {
            self.close_callback.?(self.close_ctx, self);
        }
    }

    fn onIncomingStreamClose(ctx: ?*anyopaque, res: anyerror!*libp2p.QuicStream) void {
        const self: *Semiduplex = @ptrCast(@alignCast(ctx.?));
        _ = res catch unreachable;

        self.close_callback.?(self.close_ctx, self);
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
            .callback_ctx = callback_ctx,
            .callback = callback,
            .allocator = self.allocator,
            .stream = stream,
            .received = std.ArrayList(libp2p.PubSubMessage).init(self.allocator),
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

    received: std.ArrayList(libp2p.PubSubMessage),

    stream: *libp2p.QuicStream,

    const Self = @This();

    pub fn onActivated(self: *Self, stream: *libp2p.QuicStream) anyerror!void {
        self.stream = stream;
        self.callback(self.callback_ctx, self);
    }

    pub fn onMessage(_: *Self, _: *libp2p.QuicStream, _: []const u8) anyerror!void {
        // Decode the message and add to received
    }

    pub fn onClose(self: *Self, _: *libp2p.QuicStream) anyerror!void {
        const allocator = self.allocator;
        self.received.deinit();
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
