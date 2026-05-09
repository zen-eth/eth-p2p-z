const std = @import("std");
const PeerId = @import("peer_id").PeerId;

pub const discard = @import("protocols/discard.zig");
pub const mss = @import("protocols/mss.zig");
pub const pubsub = @import("protocols/pubsub/pubsub.zig");
pub const ping = @import("protocols/ping.zig");
pub const identify = @import("protocols/identify.zig");

pub const ProtocolId = []const u8;

/// Transport-agnostic stream abstraction used by all protocol handlers.
/// Replaces *quic.QuicStream throughout the protocol layer, enabling both
/// QUIC streams and yamux streams to be used with the same handlers.
pub const AnyStream = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        writeFn: *const fn (*anyopaque, []const u8, ?*anyopaque, *const fn (?*anyopaque, anyerror!usize) void) void,
        closeFn: *const fn (*anyopaque, ?*anyopaque, *const fn (?*anyopaque, anyerror!void) void) void,
        setProtoMsgHandlerFn: *const fn (*anyopaque, AnyProtocolMessageHandler) void,
        getProtoMsgHandlerFn: *const fn (*anyopaque) ?AnyProtocolMessageHandler,
        setNegotiatedProtocolFn: *const fn (*anyopaque, ?ProtocolId) void,
        getNegotiatedProtocolFn: *const fn (*anyopaque) ?ProtocolId,
        getProposedProtocolsFn: *const fn (*anyopaque) ?[]const ProtocolId,
        getRemotePeerIdFn: *const fn (*anyopaque) ?PeerId,
    };

    pub fn write(
        self: AnyStream,
        data: []const u8,
        ctx: ?*anyopaque,
        cb: *const fn (?*anyopaque, anyerror!usize) void,
    ) void {
        self.vtable.writeFn(self.ptr, data, ctx, cb);
    }

    pub fn close(
        self: AnyStream,
        ctx: ?*anyopaque,
        cb: *const fn (?*anyopaque, anyerror!void) void,
    ) void {
        self.vtable.closeFn(self.ptr, ctx, cb);
    }

    pub fn setProtoMsgHandler(self: AnyStream, handler: AnyProtocolMessageHandler) void {
        self.vtable.setProtoMsgHandlerFn(self.ptr, handler);
    }

    pub fn getProtoMsgHandler(self: AnyStream) ?AnyProtocolMessageHandler {
        return self.vtable.getProtoMsgHandlerFn(self.ptr);
    }

    pub fn setNegotiatedProtocol(self: AnyStream, proto: ?ProtocolId) void {
        self.vtable.setNegotiatedProtocolFn(self.ptr, proto);
    }

    pub fn getNegotiatedProtocol(self: AnyStream) ?ProtocolId {
        return self.vtable.getNegotiatedProtocolFn(self.ptr);
    }

    pub fn getProposedProtocols(self: AnyStream) ?[]const ProtocolId {
        return self.vtable.getProposedProtocolsFn(self.ptr);
    }

    pub fn getRemotePeerId(self: AnyStream) ?PeerId {
        return self.vtable.getRemotePeerIdFn(self.ptr);
    }
};

pub const ProtocolStreamControllerVTable = struct {
    getStreamFn: *const fn (instance: *anyopaque) AnyStream,
};

pub const ProtocolStreamController = struct {
    instance: *anyopaque,
    vtable: *const ProtocolStreamControllerVTable,

    pub fn getStream(self: *ProtocolStreamController) AnyStream {
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

pub fn getStream(controller: ?*anyopaque) ?AnyStream {
    const base = asStreamController(controller) orelse return null;
    return base.getStream();
}

pub const ProtocolHandlerVTable = struct {
    onInitiatorStartFn: *const fn (
        instance: *anyopaque,
        stream: AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void,

    onResponderStartFn: *const fn (
        instance: *anyopaque,
        stream: AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) anyerror!void,
};

pub const AnyProtocolHandler = struct {
    instance: *anyopaque,
    vtable: *const ProtocolHandlerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onInitiatorStart(
        self: *Self,
        stream: AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        return self.vtable.onInitiatorStartFn(self.instance, stream, callback_ctx, callback);
    }

    pub fn onResponderStart(
        self: *Self,
        stream: AnyStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        return self.vtable.onResponderStartFn(self.instance, stream, callback_ctx, callback);
    }
};

pub const ProtocolMessageHandlerVTable = struct {
    onActivatedFn: *const fn (
        instance: *anyopaque,
        stream: AnyStream,
    ) anyerror!void,

    onMessageFn: *const fn (
        instance: *anyopaque,
        stream: AnyStream,
        message: []const u8,
    ) anyerror!void,

    onCloseFn: *const fn (
        instance: *anyopaque,
        stream: AnyStream,
    ) anyerror!void,
};

pub const AnyProtocolMessageHandler = struct {
    instance: *anyopaque,
    vtable: *const ProtocolMessageHandlerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onActivated(self: *Self, stream: AnyStream) !void {
        try self.vtable.onActivatedFn(self.instance, stream);
    }

    pub fn onMessage(self: *Self, stream: AnyStream, message: []const u8) !void {
        try self.vtable.onMessageFn(self.instance, stream, message);
    }

    pub fn onClose(self: *Self, stream: AnyStream) !void {
        try self.vtable.onCloseFn(self.instance, stream);
    }
};
