const std = @import("std");
const libp2p = @import("root.zig");
const quic = libp2p.transport.quic;

pub const discard = @import("protocols/discard.zig");
pub const mss = @import("protocols/mss.zig");
pub const pubsub = @import("protocols/pubsub/pubsub.zig");
pub const ping = @import("protocols/ping.zig");
pub const identify = @import("protocols/identify.zig");

pub const ProtocolId = []const u8;

pub const ProtocolStreamControllerVTable = struct {
    getStreamFn: *const fn (instance: *anyopaque) *quic.QuicStream,
};

pub const ProtocolStreamController = struct {
    instance: *anyopaque,
    vtable: *const ProtocolStreamControllerVTable,

    pub fn getStream(self: *ProtocolStreamController) *quic.QuicStream {
        return self.vtable.getStreamFn(self.instance);
    }
};

pub fn initStreamController(instance: *anyopaque, vtable: *const ProtocolStreamControllerVTable) ProtocolStreamController {
    return .{ .instance = instance, .vtable = vtable };
}

pub fn asStreamController(controller: ?*anyopaque) ?*ProtocolStreamController {
    if (controller == null) return null;
    return @ptrCast(@alignCast(controller.?));
}

pub fn getStream(controller: ?*anyopaque) ?*quic.QuicStream {
    const base = asStreamController(controller) orelse return null;
    return base.getStream();
}

/// This is the protocol binding interface for QUIC protocol message handlers.
/// It registers the protocol handler with the QUIC transport and provides
/// methods to handle protocol-specific messages.
pub const ProtocolHandlerVTable = struct {
    onInitiatorStartFn: *const fn (
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void,

    onResponderStartFn: *const fn (
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void,
};

/// This struct represents a protocol handler that can be used with the QUIC transport.
/// It contains an instance of the protocol handler and a vtable that defines the
/// methods to be called when the protocol is started by the initiator or responder.
pub const AnyProtocolHandler = struct {
    instance: *anyopaque,
    vtable: *const ProtocolHandlerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onInitiatorStart(self: *Self, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void) !void {
        return self.vtable.onInitiatorStartFn(self.instance, stream, callback_ctx, callback);
    }

    pub fn onResponderStart(self: *Self, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void) !void {
        return self.vtable.onResponderStartFn(self.instance, stream, callback_ctx, callback);
    }
};

/// This is the protocol message handler interface for QUIC protocol messages.
/// It defines the methods that need to be implemented by any protocol message handler.
/// The methods are called when the protocol is activated, when a message is received,
/// and when the stream is closed.
pub const ProtocolMessageHandlerVTable = struct {
    onActivatedFn: *const fn (
        instance: *anyopaque,
        stream: *quic.QuicStream,
    ) anyerror!void,

    onMessageFn: *const fn (
        instance: *anyopaque,
        stream: *quic.QuicStream,
        message: []const u8,
    ) anyerror!void,

    onCloseFn: *const fn (
        instance: *anyopaque,
        stream: *quic.QuicStream,
    ) anyerror!void,
};

/// This struct represents a protocol message handler that can be used with the QUIC transport.
/// It contains an instance of the protocol message handler and a vtable that defines
/// the methods to be called when the protocol is activated, when a message is received,
/// and when the stream is closed.
pub const AnyProtocolMessageHandler = struct {
    instance: *anyopaque,
    vtable: *const ProtocolMessageHandlerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onActivated(self: *Self, stream: *quic.QuicStream) !void {
        try self.vtable.onActivatedFn(self.instance, stream);
    }

    pub fn onMessage(self: *Self, stream: *quic.QuicStream, message: []const u8) !void {
        try self.vtable.onMessageFn(self.instance, stream, message);
    }

    pub fn onClose(self: *Self, stream: *quic.QuicStream) !void {
        try self.vtable.onCloseFn(self.instance, stream);
    }
};
