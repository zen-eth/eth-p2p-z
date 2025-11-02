const std = @import("std");
const libp2p = @import("../../root.zig");
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;

const pubsub = libp2p.protocols.pubsub;

pub fn extend(comptime Loop: type) type {
    return struct {
        const PubsubAddPeerTask = struct {
            ps: pubsub.PubSub,
            peer: Multiaddr,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        };

        fn runPubsubAddPeer(_: *Loop, ctx: *PubsubAddPeerTask) void {
            ctx.ps.addPeer(ctx.peer, ctx.callback_ctx, ctx.callback);
        }

        pub fn queuePubsubAddPeer(
            self: *Loop,
            ps: pubsub.PubSub,
            peer: Multiaddr,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        ) !void {
            const task = try self.allocator.create(PubsubAddPeerTask);
            task.* = .{
                .ps = ps,
                .peer = peer,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubAddPeerTask, task, runPubsubAddPeer, Loop.makeDestroyTask(PubsubAddPeerTask));
        }

        const PubsubRemovePeerTask = struct {
            ps: pubsub.PubSub,
            peer: PeerId,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        };

        fn runPubsubRemovePeer(_: *Loop, ctx: *PubsubRemovePeerTask) void {
            ctx.ps.removePeer(ctx.peer, ctx.callback_ctx, ctx.callback);
        }

        pub fn queuePubsubRemovePeer(
            self: *Loop,
            ps: pubsub.PubSub,
            peer: PeerId,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        ) !void {
            const task = try self.allocator.create(PubsubRemovePeerTask);
            task.* = .{
                .ps = ps,
                .peer = peer,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubRemovePeerTask, task, runPubsubRemovePeer, Loop.makeDestroyTask(PubsubRemovePeerTask));
        }

        const PubsubStartHeartbeatTask = struct {
            ps: pubsub.PubSub,
        };

        fn runPubsubStartHeartbeat(_: *Loop, ctx: *PubsubStartHeartbeatTask) void {
            ctx.ps.startHeartbeat();
        }

        pub fn queuePubsubStartHeartbeat(self: *Loop, ps: pubsub.PubSub) !void {
            const task = try self.allocator.create(PubsubStartHeartbeatTask);
            task.* = .{
                .ps = ps,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubStartHeartbeatTask, task, runPubsubStartHeartbeat, Loop.makeDestroyTask(PubsubStartHeartbeatTask));
        }

        const PubsubStopHeartbeatTask = struct {
            ps: pubsub.PubSub,
        };

        fn runPubsubStopHeartbeat(_: *Loop, ctx: *PubsubStopHeartbeatTask) void {
            ctx.ps.stopHeartbeat();
        }

        pub fn queuePubsubStopHeartbeat(self: *Loop, ps: pubsub.PubSub) !void {
            const task = try self.allocator.create(PubsubStopHeartbeatTask);
            task.* = .{
                .ps = ps,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubStopHeartbeatTask, task, runPubsubStopHeartbeat, Loop.makeDestroyTask(PubsubStopHeartbeatTask));
        }

        const PubsubSubscribeTask = struct {
            ps: pubsub.PubSub,
            topic: []const u8,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        };

        fn runPubsubSubscribe(_: *Loop, ctx: *PubsubSubscribeTask) void {
            ctx.ps.subscribe(ctx.topic, ctx.callback_ctx, ctx.callback);
        }

        pub fn queuePubsubSubscribe(
            self: *Loop,
            ps: pubsub.PubSub,
            topic: []const u8,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        ) !void {
            const task = try self.allocator.create(PubsubSubscribeTask);
            task.* = .{
                .ps = ps,
                .topic = topic,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubSubscribeTask, task, runPubsubSubscribe, Loop.makeDestroyTask(PubsubSubscribeTask));
        }

        const PubsubUnsubscribeTask = struct {
            ps: pubsub.PubSub,
            topic: []const u8,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        };

        fn runPubsubUnsubscribe(_: *Loop, ctx: *PubsubUnsubscribeTask) void {
            ctx.ps.unsubscribe(ctx.topic, ctx.callback_ctx, ctx.callback);
        }

        pub fn queuePubsubUnsubscribe(
            self: *Loop,
            ps: pubsub.PubSub,
            topic: []const u8,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,
        ) !void {
            const task = try self.allocator.create(PubsubUnsubscribeTask);
            task.* = .{
                .ps = ps,
                .topic = topic,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubUnsubscribeTask, task, runPubsubUnsubscribe, Loop.makeDestroyTask(PubsubUnsubscribeTask));
        }

        const PubsubPublishTask = struct {
            ps: pubsub.PubSub,
            topic: []u8,
            message: []u8,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void,
            consumed: bool = false,
        };

        fn runPubsubPublish(_: *Loop, ctx: *PubsubPublishTask) void {
            ctx.ps.vtable.publishOwnedFn(ctx.ps.instance, ctx.topic, ctx.message, ctx.callback_ctx, ctx.callback);
            ctx.consumed = true;
        }

        fn destroyPubsubPublish(loop: *Loop, ctx: *PubsubPublishTask) void {
            if (!ctx.consumed) {
                const ps_allocator = ctx.ps.allocator();
                if (ctx.topic.len > 0) ps_allocator.free(ctx.topic);
                if (ctx.message.len > 0) ps_allocator.free(ctx.message);
            }
            loop.allocator.destroy(ctx);
        }

        pub fn queuePubsubPublish(
            self: *Loop,
            ps: pubsub.PubSub,
            topic: []u8,
            message: []u8,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void,
        ) !void {
            const allocator = ps.allocator();
            errdefer allocator.free(topic);
            errdefer allocator.free(message);

            const task = try self.allocator.create(PubsubPublishTask);
            task.* = .{
                .ps = ps,
                .topic = topic,
                .message = message,
                .callback_ctx = callback_ctx,
                .callback = callback,
                .consumed = false,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(PubsubPublishTask, task, runPubsubPublish, destroyPubsubPublish);
        }
    };
}

test "queuePubsubPublish transfers ownership without duplicating buffers" {
    const allocator = std.testing.allocator;

    const MockLoop = struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        queue_calls: usize = 0,

        pub fn queueCallWithDeinit(
            self: *Self,
            comptime T: type,
            ctx: *T,
            comptime func: *const fn (loop: *Self, ctx: *T) void,
            comptime destroy_fn: ?*const fn (loop: *Self, ctx: *T) void,
        ) !void {
            self.queue_calls += 1;
            func(self, ctx);
            if (destroy_fn) |df| {
                df(self, ctx);
            }
        }

        pub fn makeDestroyTask(comptime T: type) *const fn (loop: *Self, ctx: *T) void {
            return struct {
                fn destroy(loop: *Self, ctx: *T) void {
                    loop.allocator.destroy(ctx);
                }
            }.destroy;
        }
    };

    const MockRouter = struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        publish_calls: usize = 0,
        last_topic: ?[]const u8 = null,
        last_message: ?[]const u8 = null,

        fn publishOwnedFn(
            instance: *anyopaque,
            topic: []const u8,
            data: []const u8,
            _: ?*anyopaque,
            _: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void,
        ) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.publish_calls += 1;
            self.last_topic = topic;
            self.last_message = data;
        }

        fn getAllocatorFn(instance: *anyopaque) std.mem.Allocator {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.allocator;
        }

        fn unexpectedHandle(_: *anyopaque, _: std.mem.Allocator, _: *const pubsub.RPC) anyerror!void {
            return error.UnexpectedCall;
        }

        fn unexpectedAddPeer(_: *anyopaque, _: Multiaddr, _: ?*anyopaque, _: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
            @panic("unexpected addPeer call");
        }

        fn unexpectedRemovePeer(_: *anyopaque, _: PeerId, _: ?*anyopaque, _: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
            @panic("unexpected removePeer call");
        }

        fn unexpectedSubscribe(_: *anyopaque, _: []const u8, _: ?*anyopaque, _: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
            @panic("unexpected subscribe call");
        }

        fn unexpectedUnsubscribe(_: *anyopaque, _: []const u8, _: ?*anyopaque, _: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
            @panic("unexpected unsubscribe call");
        }

        fn unexpectedPublish(_: *anyopaque, _: []const u8, _: []const u8, _: ?*anyopaque, _: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
            @panic("unexpected publish call");
        }

        fn unexpectedStartHeartbeat(_: *anyopaque) void {
            @panic("unexpected startHeartbeat call");
        }

        fn unexpectedStopHeartbeat(_: *anyopaque) void {
            @panic("unexpected stopHeartbeat call");
        }
    };

    const PubsubTasks = extend(MockLoop);

    var loop = MockLoop{ .allocator = allocator };
    var router = MockRouter{ .allocator = allocator };

    const vtable = pubsub.PubSubVTable{
        .handleRPCFn = MockRouter.unexpectedHandle,
        .addPeerFn = MockRouter.unexpectedAddPeer,
        .removePeerFn = MockRouter.unexpectedRemovePeer,
        .subscribeFn = MockRouter.unexpectedSubscribe,
        .unsubscribeFn = MockRouter.unexpectedUnsubscribe,
        .publishFn = MockRouter.unexpectedPublish,
        .publishOwnedFn = MockRouter.publishOwnedFn,
        .startHeartbeatFn = MockRouter.unexpectedStartHeartbeat,
        .stopHeartbeatFn = MockRouter.unexpectedStopHeartbeat,
        .getAllocatorFn = MockRouter.getAllocatorFn,
    };

    const iface = pubsub.PubSub{ .instance = &router, .vtable = &vtable };

    const topic_bytes = "regression-topic";
    const message_bytes = "regression-message";

    const topic = try allocator.dupe(u8, topic_bytes);
    defer if (router.last_topic == null) allocator.free(topic);
    const original_topic_ptr = @intFromPtr(topic.ptr);

    const message = try allocator.dupe(u8, message_bytes);
    defer if (router.last_message == null) allocator.free(message);
    const original_message_ptr = @intFromPtr(message.ptr);

    const PublishCallback = struct {
        fn callback(_: ?*anyopaque, _: anyerror![]PeerId) void {}
    };

    try PubsubTasks.queuePubsubPublish(
        &loop,
        iface,
        topic,
        message,
        null,
        PublishCallback.callback,
    );

    try std.testing.expectEqual(@as(usize, 1), router.publish_calls);
    try std.testing.expect(router.last_topic != null);
    try std.testing.expect(router.last_message != null);
    try std.testing.expectEqual(original_topic_ptr, @intFromPtr(router.last_topic.?.ptr));
    try std.testing.expectEqual(original_message_ptr, @intFromPtr(router.last_message.?.ptr));

    const captured_topic = router.last_topic.?;
    const captured_topic_mut = @constCast(captured_topic.ptr)[0..captured_topic.len];
    const captured_message = router.last_message.?;
    const captured_message_mut = @constCast(captured_message.ptr)[0..captured_message.len];

    allocator.free(captured_topic_mut);
    allocator.free(captured_message_mut);
}
