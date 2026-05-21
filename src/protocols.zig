const std = @import("std");
const PeerId = @import("peer_id").PeerId;
const Stream = @import("quic.zig").Stream;

pub const ProtocolId = []const u8;

pub const InboundProtocolContext = struct {
    protocol_id: ProtocolId,
    peer_id: PeerId,
    remote_addr: std.Io.net.IpAddress,
};

pub const AnyProtocolStreamHandler = struct {
    instance: *anyopaque,
    runFn: *const fn (*anyopaque, std.Io, *Stream) anyerror!void,
    deinitFn: *const fn (*anyopaque, std.mem.Allocator) void,

    pub fn run(self: AnyProtocolStreamHandler, io: std.Io, stream: *Stream) !void {
        return self.runFn(self.instance, io, stream);
    }

    pub fn deinit(self: AnyProtocolStreamHandler, allocator: std.mem.Allocator) void {
        self.deinitFn(self.instance, allocator);
    }
};

pub const AnyProtocolService = struct {
    instance: *anyopaque,
    openInboundFn: *const fn (*anyopaque, std.mem.Allocator, InboundProtocolContext) anyerror!AnyProtocolStreamHandler,
    deinitFn: ?*const fn (*anyopaque) void = null,

    pub fn openInbound(
        self: AnyProtocolService,
        allocator: std.mem.Allocator,
        ctx: InboundProtocolContext,
    ) !AnyProtocolStreamHandler {
        return self.openInboundFn(self.instance, allocator, ctx);
    }

    pub fn deinit(self: AnyProtocolService) void {
        if (self.deinitFn) |func| func(self.instance);
    }
};

pub fn protocolStreamHandler(
    comptime T: type,
    comptime run_method: fn (*T, std.Io, *Stream) anyerror!void,
    instance: *T,
) AnyProtocolStreamHandler {
    const Wrap = struct {
        fn invoke(erased: *anyopaque, io: std.Io, stream: *Stream) anyerror!void {
            return run_method(@ptrCast(@alignCast(erased)), io, stream);
        }

        fn deinit(_: *anyopaque, _: std.mem.Allocator) void {}
    };
    return .{ .instance = instance, .runFn = Wrap.invoke, .deinitFn = Wrap.deinit };
}

pub fn ownedProtocolStreamHandler(
    comptime T: type,
    comptime run_method: fn (*T, std.Io, *Stream) anyerror!void,
    instance: *T,
) AnyProtocolStreamHandler {
    const Wrap = struct {
        fn invoke(erased: *anyopaque, io: std.Io, stream: *Stream) anyerror!void {
            return run_method(@ptrCast(@alignCast(erased)), io, stream);
        }

        fn deinit(erased: *anyopaque, allocator: std.mem.Allocator) void {
            allocator.destroy(@as(*T, @ptrCast(@alignCast(erased))));
        }
    };
    return .{ .instance = instance, .runFn = Wrap.invoke, .deinitFn = Wrap.deinit };
}

pub fn protocolService(
    comptime T: type,
    comptime open_inbound_method: fn (*T, std.mem.Allocator, InboundProtocolContext) anyerror!AnyProtocolStreamHandler,
    instance: *T,
) AnyProtocolService {
    const Wrap = struct {
        fn openInbound(
            erased: *anyopaque,
            allocator: std.mem.Allocator,
            ctx: InboundProtocolContext,
        ) anyerror!AnyProtocolStreamHandler {
            return open_inbound_method(@ptrCast(@alignCast(erased)), allocator, ctx);
        }
    };
    return .{ .instance = instance, .openInboundFn = Wrap.openInbound };
}

pub fn ownedProtocolService(
    comptime T: type,
    comptime open_inbound_method: fn (*T, std.mem.Allocator, InboundProtocolContext) anyerror!AnyProtocolStreamHandler,
    comptime deinit_method: fn (*T) void,
    instance: *T,
) AnyProtocolService {
    const Wrap = struct {
        fn openInbound(
            erased: *anyopaque,
            allocator: std.mem.Allocator,
            ctx: InboundProtocolContext,
        ) anyerror!AnyProtocolStreamHandler {
            return open_inbound_method(@ptrCast(@alignCast(erased)), allocator, ctx);
        }

        fn deinit(erased: *anyopaque) void {
            deinit_method(@ptrCast(@alignCast(erased)));
        }
    };
    return .{ .instance = instance, .openInboundFn = Wrap.openInbound, .deinitFn = Wrap.deinit };
}

pub fn streamHandlerService(
    comptime T: type,
    comptime run_method: fn (*T, std.Io, *Stream) anyerror!void,
    instance: *T,
) AnyProtocolService {
    const Wrap = struct {
        fn openInbound(
            erased: *anyopaque,
            _: std.mem.Allocator,
            _: InboundProtocolContext,
        ) anyerror!AnyProtocolStreamHandler {
            return protocolStreamHandler(T, run_method, @ptrCast(@alignCast(erased)));
        }
    };
    return .{ .instance = instance, .openInboundFn = Wrap.openInbound };
}

pub fn ownedStreamHandlerService(
    comptime T: type,
    comptime run_method: fn (*T, std.Io, *Stream) anyerror!void,
    comptime deinit_method: fn (*T) void,
    instance: *T,
) AnyProtocolService {
    const Wrap = struct {
        fn openInbound(
            erased: *anyopaque,
            _: std.mem.Allocator,
            _: InboundProtocolContext,
        ) anyerror!AnyProtocolStreamHandler {
            return protocolStreamHandler(T, run_method, @ptrCast(@alignCast(erased)));
        }

        fn deinit(erased: *anyopaque) void {
            deinit_method(@ptrCast(@alignCast(erased)));
        }
    };
    return .{ .instance = instance, .openInboundFn = Wrap.openInbound, .deinitFn = Wrap.deinit };
}

pub const multistream = @import("protocols/multistream.zig");
pub const ping = @import("protocols/ping.zig");
pub const identify = @import("protocols/identify.zig");
pub const discard = @import("protocols/discard.zig");
pub const pubsub = @import("protocols/pubsub/pubsub.zig");
