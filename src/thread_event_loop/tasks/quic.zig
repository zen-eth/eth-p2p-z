const std = @import("std");
const libp2p = @import("../../root.zig");
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;

const quic = libp2p.transport.quic;

pub fn extend(comptime Loop: type) type {
    return struct {
        const QuicEngineStopTask = struct {
            engine: *quic.QuicEngine,
        };

        fn runQuicEngineStop(_: *Loop, ctx: *QuicEngineStopTask) void {
            ctx.engine.doStop();
        }

        pub fn queueQuicEngineStop(self: *Loop, engine: *quic.QuicEngine) !void {
            const task = try self.allocator.create(QuicEngineStopTask);
            task.* = .{ .engine = engine };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(QuicEngineStopTask, task, runQuicEngineStop, Loop.makeDestroyTask(QuicEngineStopTask));
        }

        const QuicEngineStartTask = struct {
            engine: *quic.QuicEngine,
        };

        fn runQuicEngineStart(_: *Loop, ctx: *QuicEngineStartTask) void {
            ctx.engine.doStart();
        }

        pub fn queueQuicEngineStart(self: *Loop, engine: *quic.QuicEngine) !void {
            const task = try self.allocator.create(QuicEngineStartTask);
            task.* = .{ .engine = engine };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(QuicEngineStartTask, task, runQuicEngineStart, Loop.makeDestroyTask(QuicEngineStartTask));
        }

        const QuicConnectTask = struct {
            engine: *quic.QuicEngine,
            peer_address: Multiaddr,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void,
        };

        fn runQuicConnect(_: *Loop, ctx: *QuicConnectTask) void {
            ctx.engine.doConnect(ctx.peer_address, ctx.callback_ctx, ctx.callback);
        }

        pub fn queueQuicConnect(
            self: *Loop,
            engine: *quic.QuicEngine,
            peer_address: Multiaddr,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void,
        ) !void {
            const task = try self.allocator.create(QuicConnectTask);
            task.* = .{
                .engine = engine,
                .peer_address = peer_address,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(QuicConnectTask, task, runQuicConnect, Loop.makeDestroyTask(QuicConnectTask));
        }

        const QuicCloseConnectionTask = struct {
            conn: *quic.QuicConnection,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void,
        };

        fn runQuicCloseConnection(_: *Loop, ctx: *QuicCloseConnectionTask) void {
            ctx.conn.doClose(ctx.callback_ctx, ctx.callback);
        }

        pub fn queueQuicCloseConnection(
            self: *Loop,
            quic_conn: *quic.QuicConnection,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicConnection) void,
        ) !void {
            const task = try self.allocator.create(QuicCloseConnectionTask);
            task.* = .{
                .conn = quic_conn,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(QuicCloseConnectionTask, task, runQuicCloseConnection, Loop.makeDestroyTask(QuicCloseConnectionTask));
        }

        const QuicNewStreamTask = struct {
            conn: *quic.QuicConnection,
            new_stream_ctx: ?*anyopaque,
            new_stream_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void,
        };

        fn runQuicNewStream(_: *Loop, ctx: *QuicNewStreamTask) void {
            ctx.conn.doNewStream(ctx.new_stream_ctx, ctx.new_stream_callback);
        }

        pub fn queueQuicNewStream(
            self: *Loop,
            quic_conn: *quic.QuicConnection,
            new_stream_ctx: ?*anyopaque,
            new_stream_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void,
        ) !void {
            const task = try self.allocator.create(QuicNewStreamTask);
            task.* = .{
                .conn = quic_conn,
                .new_stream_ctx = new_stream_ctx,
                .new_stream_callback = new_stream_callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(QuicNewStreamTask, task, runQuicNewStream, Loop.makeDestroyTask(QuicNewStreamTask));
        }

        const QuicWriteStreamTask = struct {
            stream: *quic.QuicStream,
            data: std.ArrayList(u8),
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void,
        };

        fn runQuicWriteStream(_: *Loop, ctx: *QuicWriteStreamTask) void {
            ctx.stream.doWrite(ctx.data, ctx.callback_ctx, ctx.callback);
        }

        pub fn queueQuicWriteStream(
            self: *Loop,
            stream: *quic.QuicStream,
            data: std.ArrayList(u8),
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void,
        ) !void {
            const task = try self.allocator.create(QuicWriteStreamTask);
            task.* = .{
                .stream = stream,
                .data = data,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            errdefer task.data.deinit();
            try self.queueCallWithDeinit(QuicWriteStreamTask, task, runQuicWriteStream, Loop.makeDestroyTask(QuicWriteStreamTask));
        }

        const QuicCloseStreamTask = struct {
            stream: *quic.QuicStream,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void,
        };

        fn runQuicCloseStream(_: *Loop, ctx: *QuicCloseStreamTask) void {
            ctx.stream.doClose(ctx.callback_ctx, ctx.callback);
        }

        pub fn queueQuicCloseStream(
            self: *Loop,
            stream: *quic.QuicStream,
            callback_ctx: ?*anyopaque,
            callback: *const fn (ctx: ?*anyopaque, res: anyerror!*quic.QuicStream) void,
        ) !void {
            const task = try self.allocator.create(QuicCloseStreamTask);
            task.* = .{
                .stream = stream,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(QuicCloseStreamTask, task, runQuicCloseStream, Loop.makeDestroyTask(QuicCloseStreamTask));
        }
    };
}
