//! `Stream` handle: the user-facing API for a QUIC stream. Holds only a
//! `*SharedState` (channel + atomic flags + a back-pointer to the
//! connection's `SharedState`). All control operations (close, reset,
//! ...) ride on the connection's inbox as `Command`s with stack-allocated
//! reply slots; bytes flow through the inbound/outbound byte queues
//! directly.
//!
//! The handle never holds a pointer into the connection actor, the
//! stream actor record, or any other actor-private memory.

const std = @import("std");
const conn_commands = @import("../connection/commands.zig");
const io_time = @import("../io/time.zig");
const shared_state_mod = @import("shared_state.zig");

const Allocator = std.mem.Allocator;

pub const SharedState = shared_state_mod.SharedState;
pub const Command = conn_commands.Command;
pub const VoidReply = conn_commands.VoidReply;

const ReadErrorImpl = error{
    EndOfStream,
    ConnectionClosed,
    ResetByPeer,
    StreamShutdown,
} || std.Io.Cancelable || std.Io.Timeout.Error;

const WriteErrorImpl = error{
    ConnectionClosed,
    StreamShutdown,
    ResetByPeer,
} || std.Io.Cancelable || std.Io.Timeout.Error;

const ReadOptionsImpl = struct { timeout: std.Io.Timeout = .none };
const WriteOptionsImpl = struct { timeout: std.Io.Timeout = .none };

const Impl = struct {
    allocator: Allocator,
    state: ?*SharedState,
    handle_lock: std.atomic.Value(u8) = .init(0),

    fn lockHandle(self: *Impl) void {
        while (self.handle_lock.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlockHandle(self: *Impl) void {
        self.handle_lock.store(0, .release);
    }

    fn liveRetained(self: *Impl) ?*SharedState {
        self.lockHandle();
        const state = self.state;
        if (state) |st| st.retain();
        self.unlockHandle();
        return state;
    }

    fn deinit(self: *Impl) void {
        const allocator = self.allocator;
        self.lockHandle();
        const state = self.state;
        self.state = null;
        self.unlockHandle();
        if (state) |st| {
            // Best-effort drop notification: if not already closed, ask the
            // actor to abrupt-close (RESET_STREAM(0)). Fire-and-forget.
            if (!st.isClosed()) {
                st.markClosedLocal();
                postFireAndForget(st, .{ .drop_stream = st.stream_id }, st.io);
            }
            st.release();
        }
        allocator.destroy(self);
    }
};

pub const Stream = opaque {
    pub const Reader = StreamReader;
    pub const Writer = StreamWriter;
    pub const ReadError = ReadErrorImpl;
    pub const WriteError = WriteErrorImpl;
    pub const ShutdownWriteError = std.Io.Cancelable;
    pub const CloseError = std.Io.Cancelable;
    pub const CloseReadError = std.Io.Cancelable;
    pub const ResetError = std.Io.Cancelable;
    pub const ReadOptions = ReadOptionsImpl;
    pub const WriteOptions = WriteOptionsImpl;

    fn impl(self: *Stream) *Impl {
        return @ptrCast(@alignCast(self));
    }

    pub fn deinit(self: *Stream) void {
        self.impl().deinit();
    }

    pub fn streamId(self: *Stream) ?u64 {
        const state = self.impl().liveRetained() orelse return null;
        defer state.release();
        return state.stream_id;
    }

    pub fn read(self: *Stream, io: std.Io, buf: []u8, opts: ReadOptions) ReadError!usize {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        return readShared(state, io, buf, opts);
    }

    pub fn readAll(self: *Stream, io: std.Io, buf: []u8, opts: ReadOptions) ReadError!void {
        var off: usize = 0;
        while (off < buf.len) {
            const n = try self.read(io, buf[off..], opts);
            off += n;
        }
    }

    pub fn write(self: *Stream, io: std.Io, buf: []const u8, opts: WriteOptions) WriteError!usize {
        const state = self.impl().liveRetained() orelse return error.ConnectionClosed;
        defer state.release();
        return writeShared(state, io, buf, opts);
    }

    pub fn writeAll(self: *Stream, io: std.Io, buf: []const u8, opts: WriteOptions) WriteError!void {
        var off: usize = 0;
        while (off < buf.len) {
            const n = try self.write(io, buf[off..], opts);
            if (n == 0) return error.StreamShutdown;
            off += n;
        }
    }

    pub fn reader(self: *Stream, io: std.Io, buffer: []u8) Reader {
        return Reader.init(self, io, buffer);
    }

    pub fn writer(self: *Stream, io: std.Io, buffer: []u8) Writer {
        return Writer.init(self, io, buffer);
    }

    /// Graceful bidirectional close. Schedules FIN on the write side and
    /// STOP_SENDING(0) on the read side. After this returns, subsequent
    /// reads/writes return `error.StreamShutdown`.
    pub fn close(self: *Stream, io: std.Io) CloseError!void {
        const state = self.impl().liveRetained() orelse return;
        defer state.release();
        if (state.isClosed()) return;
        state.markClosedLocal();

        _ = postCommittedCommand(state, .{ .close_stream = .{ .stream_id = state.stream_id } }, io);
    }

    /// Graceful read-side close. Sends STOP_SENDING(0). Writes remain open.
    pub fn closeRead(self: *Stream, io: std.Io) CloseReadError!void {
        const state = self.impl().liveRetained() orelse return;
        defer state.release();
        if (state.isReadShutdown() or state.isClosed()) return;
        state.markReadShutdownLocal();

        var reply: VoidReply = .{};
        if (!postCommittedCommand(state, .{ .close_read_stream = .{ .stream_id = state.stream_id, .reply = &reply } }, io)) return;
        reply.event.waitUncancelable(io);
    }

    /// Graceful write-side close. Buffered writes drain, then FIN goes on
    /// the wire. Reply fires *after* the FIN has been handed to quiche so
    /// callers can sequence "FIN sent before connection close".
    pub fn closeWrite(self: *Stream, io: std.Io) ShutdownWriteError!void {
        const state = self.impl().liveRetained() orelse return;
        defer state.release();
        if (state.isWriteShutdown() or state.isClosed()) return;
        state.markWriteShutdownLocal();

        var reply: VoidReply = .{};
        if (!postCommittedCommand(state, .{ .close_write_stream = .{ .stream_id = state.stream_id, .reply = &reply } }, io)) return;
        reply.event.waitUncancelable(io);
    }

    /// Abrupt bidirectional close. RESET_STREAM(code) on the write side,
    /// STOP_SENDING(code) on the read side. Pending outbound bytes are
    /// discarded.
    pub fn reset(self: *Stream, io: std.Io, app_error_code: u64) ResetError!void {
        const state = self.impl().liveRetained() orelse return;
        defer state.release();
        if (state.isClosed()) return;
        state.markClosedLocal();

        var reply: VoidReply = .{};
        if (!postCommittedCommand(state, .{ .reset_stream = .{
            .stream_id = state.stream_id,
            .code = app_error_code,
            .reply = &reply,
        } }, io)) return;
        reply.event.waitUncancelable(io);
    }
};

pub fn create(allocator: Allocator, state: *SharedState) Allocator.Error!*Stream {
    const impl = try allocator.create(Impl);
    impl.* = .{ .allocator = allocator, .state = state };
    return @ptrCast(impl);
}

fn postCommittedCommand(state: *SharedState, cmd: Command, io: std.Io) bool {
    state.conn.inbox.putOneUncancelable(io, cmd) catch |err| switch (err) {
        error.Closed => return false,
    };
    state.conn.notifyControlCommandsReady();
    return true;
}

fn postFireAndForget(state: *SharedState, cmd: Command, io: std.Io) void {
    const n = state.conn.inbox.putUncancelable(io, &.{cmd}, 0) catch {
        _ = state.conn.control_inbox_drops.fetchAdd(1, .acq_rel);
        return;
    };
    if (n == 1) {
        state.conn.notifyControlCommandsReady();
    } else {
        _ = state.conn.control_inbox_drops.fetchAdd(1, .acq_rel);
    }
}

fn readShared(state: *SharedState, io: std.Io, buf: []u8, opts: ReadOptionsImpl) ReadErrorImpl!usize {
    if (buf.len == 0) return 0;
    if (state.isClosed() or state.isReadShutdown()) {
        if (state.isInboundResetByPeer()) return error.ResetByPeer;
        return error.StreamShutdown;
    }
    var queue = if (state.inbound_queue) |*q| q else return error.ConnectionClosed;

    const deadline = opts.timeout.toDeadline(io);
    while (true) {
        const observed = if (opts.timeout != .none) queue.observeReadable() else 0;
        const n = queue.tryGet(io, buf) catch {
            if (state.isInboundResetByPeer()) return error.ResetByPeer;
            return error.EndOfStream;
        };
        if (n > 0) return n;

        if (opts.timeout == .none) {
            const blocking_n = queue.get(io, buf, 1) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                error.Closed => {
                    if (state.isInboundResetByPeer()) return error.ResetByPeer;
                    return error.EndOfStream;
                },
            };
            return blocking_n;
        }
        if (io_time.timeoutExpired(io, deadline)) return error.Timeout;
        try queue.waitReadable(io, observed, deadline);
    }
}

fn writeShared(state: *SharedState, io: std.Io, buf: []const u8, opts: WriteOptionsImpl) WriteErrorImpl!usize {
    if (buf.len == 0) return 0;
    if (state.isOutboundResetByPeer()) return error.ResetByPeer;
    if (state.isClosed()) return error.ConnectionClosed;
    if (state.isWriteShutdown()) return error.StreamShutdown;
    var queue = if (state.outbound_queue) |*q| q else return error.ConnectionClosed;

    const deadline = opts.timeout.toDeadline(io);
    while (true) {
        if (state.isOutboundResetByPeer()) return error.ResetByPeer;
        if (state.isClosed()) return error.ConnectionClosed;
        if (state.isWriteShutdown()) return error.StreamShutdown;

        const observed = queue.observeWritable();
        const written = queue.tryPutSome(io, buf);
        if (written > 0) {
            state.signalOutboundReady(io);
            return written;
        }

        // Note: surface queue-full pressure through the connection's
        // pending counter so the actor can fold it into stats.
        _ = state.conn.outbound_stream_queue_full.fetchAdd(1, .acq_rel);

        if (opts.timeout != .none and io_time.timeoutExpired(io, deadline)) return error.Timeout;
        try queue.waitWritable(io, observed, deadline);
    }
}

test "closeWrite returns when connection inbox is already closed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const conn_state = try shared_state_mod.ConnSharedState.create(allocator, io, .{
        .inbox_capacity = 1,
        .accept_capacity = 1,
    });
    defer {
        conn_state.release();
        conn_state.release();
    }

    const state = try SharedState.create(allocator, io, conn_state, .{
        .stream_id = 0,
        .inbound_queue_bytes = 1024,
        .outbound_queue_bytes = 1024,
    });
    const stream = try create(allocator, state);
    defer stream.deinit();

    conn_state.inbox.close(io);
    try stream.closeWrite(io);
    try std.testing.expect(state.isWriteShutdown());
}

pub const StreamReader = struct {
    io: std.Io,
    interface: std.Io.Reader,
    stream: *Stream,
    err: ?ReadErrorImpl = null,

    pub fn init(stream: *Stream, io: std.Io, buffer: []u8) StreamReader {
        return .{
            .io = io,
            .interface = .{
                .vtable = &.{
                    .stream = streamImpl,
                    .readVec = readVec,
                },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .stream = stream,
        };
    }

    pub fn deinit(_: *StreamReader) void {}

    fn streamImpl(io_r: *std.Io.Reader, io_w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const dest = limit.slice(try io_w.writableSliceGreedy(1));
        var data: [1][]u8 = .{dest};
        const n = readVec(io_r, &data) catch |err| switch (err) {
            error.EndOfStream => return error.EndOfStream,
            error.ReadFailed => return error.ReadFailed,
        };
        io_w.advance(n);
        return n;
    }

    fn readVec(io_r: *std.Io.Reader, data: [][]u8) std.Io.Reader.Error!usize {
        const r: *StreamReader = @alignCast(@fieldParentPtr("interface", io_r));
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, _ = try io_r.writableVector(&iovecs_buffer, data);
        const dest = iovecs_buffer[0..dest_n];
        std.debug.assert(dest.len > 0);
        std.debug.assert(dest[0].len > 0);

        return r.stream.read(r.io, dest[0], .{}) catch |err| switch (err) {
            error.EndOfStream => error.EndOfStream,
            else => {
                r.err = err;
                return error.ReadFailed;
            },
        };
    }
};

pub const StreamWriter = struct {
    io: std.Io,
    interface: std.Io.Writer,
    stream: *Stream,
    err: ?WriteErrorImpl = null,

    pub fn init(stream: *Stream, io: std.Io, buffer: []u8) StreamWriter {
        return .{
            .io = io,
            .interface = .{
                .vtable = &.{ .drain = drain },
                .buffer = buffer,
            },
            .stream = stream,
        };
    }

    pub fn deinit(_: *StreamWriter) void {}

    fn drain(io_w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const w: *StreamWriter = @alignCast(@fieldParentPtr("interface", io_w));
        const total = writeSplatHeader(w, io_w.buffered(), data, splat) catch |err| {
            w.err = err;
            return error.WriteFailed;
        };
        return io_w.consume(total);
    }

    fn writeSplatHeader(w: *StreamWriter, header: []const u8, data: []const []const u8, splat: usize) WriteErrorImpl!usize {
        var total: usize = 0;
        if (header.len > 0) {
            const n = try w.stream.write(w.io, header, .{});
            total += n;
            if (n < header.len) return total;
        }

        for (data[0 .. data.len - 1]) |bytes| {
            if (bytes.len == 0) continue;
            const n = try w.stream.write(w.io, bytes, .{});
            total += n;
            if (n < bytes.len) return total;
        }

        const pattern = data[data.len - 1];
        var i: usize = 0;
        while (i < splat) : (i += 1) {
            if (pattern.len == 0) continue;
            const n = try w.stream.write(w.io, pattern, .{});
            total += n;
            if (n < pattern.len) return total;
        }
        return total;
    }
};
