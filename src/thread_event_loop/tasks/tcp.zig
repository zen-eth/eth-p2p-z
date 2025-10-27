const std = @import("std");
const libp2p = @import("../../root.zig");
const conn = libp2p.conn;
const xev = libp2p.xev;

const xev_tcp = libp2p.transport.tcp;

pub fn extend(
    comptime Loop: type,
    comptime ConnectCtx: type,
    comptime AcceptCtx: type,
    comptime WriteCtx: type,
    comptime CloseCtx: type,
) type {
    return struct {
        const TcpConnectTask = struct {
            address: std.net.Address,
            transport: *xev_tcp.XevTransport,
            timeout_ms: u64,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void,
            callback_instance: ?*anyopaque,
        };

        fn runTcpConnect(loop: *Loop, ctx: *TcpConnectTask) void {
            const address = ctx.address;
            var socket = xev.TCP.init(address) catch unreachable;
            const c = loop.completion_pool.create() catch unreachable;
            const connect_ctx = loop.connect_ctx_pool.create() catch unreachable;
            connect_ctx.* = .{
                .transport = ctx.transport,
                .callback = ctx.callback,
                .callback_instance = ctx.callback_instance,
            };
            socket.connect(&loop.loop, c, address, ConnectCtx, connect_ctx, xev_tcp.XevTransport.connectCB);
        }

        pub fn queueTcpConnect(
            self: *Loop,
            address: std.net.Address,
            transport: *xev_tcp.XevTransport,
            timeout_ms: u64,
            callback_instance: ?*anyopaque,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void,
        ) !void {
            const task = try self.allocator.create(TcpConnectTask);
            task.* = .{
                .address = address,
                .transport = transport,
                .timeout_ms = timeout_ms,
                .callback = callback,
                .callback_instance = callback_instance,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(TcpConnectTask, task, runTcpConnect, Loop.makeDestroyTask(TcpConnectTask));
        }

        const TcpAcceptTask = struct {
            server: xev.TCP,
            transport: *xev_tcp.XevTransport,
            timeout_ms: u64,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void,
            callback_instance: ?*anyopaque,
        };

        fn runTcpAccept(loop: *Loop, ctx: *TcpAcceptTask) void {
            const server = ctx.server;
            const c = loop.completion_pool.create() catch unreachable;
            const accept_ctx = loop.accept_ctx_pool.create() catch unreachable;
            accept_ctx.* = .{
                .callback = ctx.callback,
                .callback_instance = ctx.callback_instance,
                .transport = ctx.transport,
            };
            server.accept(&loop.loop, c, AcceptCtx, accept_ctx, xev_tcp.XevListener.acceptCB);
        }

        pub fn queueTcpAccept(
            self: *Loop,
            server: xev.TCP,
            transport: *xev_tcp.XevTransport,
            timeout_ms: u64,
            callback_instance: ?*anyopaque,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void,
        ) !void {
            const task = try self.allocator.create(TcpAcceptTask);
            task.* = .{
                .server = server,
                .transport = transport,
                .timeout_ms = timeout_ms,
                .callback = callback,
                .callback_instance = callback_instance,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(TcpAcceptTask, task, runTcpAccept, Loop.makeDestroyTask(TcpAcceptTask));
        }

        const TcpWriteTask = struct {
            buffer: []const u8,
            channel: *xev_tcp.XevSocketChannel,
            timeout_ms: u64,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void,
            callback_instance: ?*anyopaque,
        };

        fn runTcpWrite(loop: *Loop, ctx: *TcpWriteTask) void {
            const c = loop.completion_pool.create() catch unreachable;
            const write_ctx = loop.write_ctx_pool.create() catch unreachable;
            write_ctx.* = .{
                .channel = ctx.channel,
                .callback_instance = ctx.callback_instance,
                .callback = ctx.callback,
            };
            ctx.channel.socket.write(&loop.loop, c, .{ .slice = ctx.buffer }, WriteCtx, write_ctx, xev_tcp.XevSocketChannel.writeCallback);
        }

        pub fn queueTcpWrite(
            self: *Loop,
            channel: *xev_tcp.XevSocketChannel,
            buffer: []const u8,
            timeout_ms: u64,
            callback_instance: ?*anyopaque,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void,
        ) !void {
            const task = try self.allocator.create(TcpWriteTask);
            task.* = .{
                .buffer = buffer,
                .channel = channel,
                .timeout_ms = timeout_ms,
                .callback = callback,
                .callback_instance = callback_instance,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(TcpWriteTask, task, runTcpWrite, Loop.makeDestroyTask(TcpWriteTask));
        }

        const TcpCloseTask = struct {
            channel: *xev_tcp.XevSocketChannel,
            timeout_ms: u64,
            callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
            callback_instance: ?*anyopaque,
        };

        fn runTcpClose(loop: *Loop, ctx: *TcpCloseTask) void {
            const c = loop.completion_pool.create() catch unreachable;
            const close_ctx = loop.close_ctx_pool.create() catch unreachable;
            close_ctx.* = .{
                .channel = ctx.channel,
                .callback_instance = ctx.callback_instance,
                .callback = ctx.callback,
            };
            ctx.channel.socket.shutdown(&loop.loop, c, CloseCtx, close_ctx, xev_tcp.XevSocketChannel.shutdownCB);
        }

        pub fn queueTcpClose(
            self: *Loop,
            channel: *xev_tcp.XevSocketChannel,
            timeout_ms: u64,
            callback_instance: ?*anyopaque,
            callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
        ) !void {
            const task = try self.allocator.create(TcpCloseTask);
            task.* = .{
                .channel = channel,
                .timeout_ms = timeout_ms,
                .callback = callback,
                .callback_instance = callback_instance,
            };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(TcpCloseTask, task, runTcpClose, Loop.makeDestroyTask(TcpCloseTask));
        }
    };
}
