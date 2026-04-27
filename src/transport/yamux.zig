/// Yamux multiplexer implementation.
/// Spec: https://github.com/hashicorp/yamux/blob/master/spec.md
///
/// Yamux multiplexes multiple logical streams over a single TCP connection.
/// Each stream is identified by a 32-bit stream ID.
/// - Clients (initiators) use odd stream IDs: 1, 3, 5, ...
/// - Servers (responders) use even stream IDs: 2, 4, 6, ...
///
/// Frame header layout (12 bytes, big-endian):
///   [0]    version  = 0
///   [1]    type     = 0x00 Data / 0x01 WindowUpdate / 0x02 Ping / 0x03 GoAway
///   [2-3]  flags    = SYN=0x1 ACK=0x2 FIN=0x4 RST=0x8
///   [4-7]  stream_id
///   [8-11] length   (payload bytes for Data / increment delta for WindowUpdate)

const std = @import("std");
const Allocator = std.mem.Allocator;
const conn_mod = @import("../conn.zig");
const AnyConn = conn_mod.AnyConn;
const ConnHandlerContext = conn_mod.ConnHandlerContext;
const ConnHandlerVTable = conn_mod.ConnHandlerVTable;
const AnyConnHandler = conn_mod.AnyConnHandler;
const libp2p = @import("../root.zig");
const PeerId = @import("peer_id").PeerId;

/// A lightweight data-delivery interface for YamuxStream.
/// Step 1.2 will bridge this to the full AnyProtocolMessageHandler/AnyStream.
pub const StreamDataHandlerVTable = struct {
    onDataFn: *const fn (instance: *anyopaque, data: []const u8) anyerror!void,
    onCloseFn: *const fn (instance: *anyopaque) void,
};

pub const AnyStreamDataHandler = struct {
    instance: *anyopaque,
    vtable: *const StreamDataHandlerVTable,

    pub fn onData(self: AnyStreamDataHandler, data: []const u8) !void {
        return self.vtable.onDataFn(self.instance, data);
    }
    pub fn onClose(self: AnyStreamDataHandler) void {
        self.vtable.onCloseFn(self.instance);
    }
};

// Frame types
const TYPE_DATA: u8 = 0x00;
const TYPE_WINDOW_UPDATE: u8 = 0x01;
const TYPE_PING: u8 = 0x02;
const TYPE_GO_AWAY: u8 = 0x03;

// Flags
const FLAG_SYN: u16 = 0x0001;
const FLAG_ACK: u16 = 0x0002;
const FLAG_FIN: u16 = 0x0004;
const FLAG_RST: u16 = 0x0008;

pub const HEADER_LEN: usize = 12;
const INITIAL_WINDOW: u32 = 256 * 1024; // 256 KiB per stream
const WINDOW_UPDATE_THRESHOLD: u32 = INITIAL_WINDOW / 2;

const StreamState = enum {
    syn_sent, // We opened; waiting for ACK
    syn_recv, // Peer opened; we ACKed
    established,
    local_close, // We sent FIN; peer has not yet
    remote_close, // Peer sent FIN; we have not yet
    closed,
};

/// A pending write waiting for enough send_window.
const PendingWrite = struct {
    framed_buf: []u8, // Allocated [header|data]; freed by write callback
    data_len: usize, // Original data length for window accounting
    callback_ctx: ?*anyopaque,
    callback: *const fn (?*anyopaque, anyerror!usize) void,
};

pub const YamuxFrame = struct {
    version: u8 = 0,
    frame_type: u8,
    flags: u16,
    stream_id: u32,
    length: u32,

    /// Encode into a 12-byte slice.
    pub fn encode(self: YamuxFrame, buf: []u8) void {
        std.debug.assert(buf.len >= HEADER_LEN);
        buf[0] = self.version;
        buf[1] = self.frame_type;
        std.mem.writeInt(u16, buf[2..4], self.flags, .big);
        std.mem.writeInt(u32, buf[4..8], self.stream_id, .big);
        std.mem.writeInt(u32, buf[8..12], self.length, .big);
    }

    pub fn decode(buf: []const u8) YamuxFrame {
        std.debug.assert(buf.len >= HEADER_LEN);
        return .{
            .version = buf[0],
            .frame_type = buf[1],
            .flags = std.mem.readInt(u16, buf[2..4], .big),
            .stream_id = std.mem.readInt(u32, buf[4..8], .big),
            .length = std.mem.readInt(u32, buf[8..12], .big),
        };
    }
};

// Context carried through AnyConn.write for a framed yamux write.
const FramedWriteCtx = struct {
    allocator: Allocator,
    framed_buf: []u8,
    data_len: usize,
    user_ctx: ?*anyopaque,
    user_callback: *const fn (?*anyopaque, anyerror!usize) void,

    fn writeCallback(instance: ?*anyopaque, res: anyerror!usize) void {
        const self: *FramedWriteCtx = @ptrCast(@alignCast(instance.?));
        const allocator = self.allocator;
        const data_len = self.data_len;
        const user_ctx = self.user_ctx;
        const user_callback = self.user_callback;
        allocator.free(self.framed_buf);
        allocator.destroy(self);
        user_callback(user_ctx, if (res) |_| data_len else |e| e);
    }
};

/// A logical yamux stream.
pub const YamuxStream = struct {
    allocator: Allocator,
    stream_id: u32,
    session: *YamuxSession,
    state: StreamState,

    // Receive side
    recv_buf: std.ArrayList(u8),
    recv_window_consumed: u32,

    // Send side
    send_window: u32,
    pending_writes: std.ArrayList(PendingWrite),

    // Data handler (set by protocol implementations via AnyStream in step 1.2)
    data_handler: ?AnyStreamDataHandler,

    // Close callback; fired when the full close handshake completes
    close_ctx: ?*anyopaque,
    close_callback: ?*const fn (?*anyopaque, anyerror!void) void,

    // Protocol-level metadata (managed by mss.zig and protocol handlers via AnyStream)
    proto_msg_handler: ?libp2p.protocols.AnyProtocolMessageHandler,
    negotiated_protocol: ?libp2p.protocols.ProtocolId,
    proposed_protocols: ?[]const libp2p.protocols.ProtocolId,

    pub fn init(allocator: Allocator, stream_id: u32, session: *YamuxSession, state: StreamState) !*YamuxStream {
        const s = try allocator.create(YamuxStream);
        s.* = .{
            .allocator = allocator,
            .stream_id = stream_id,
            .session = session,
            .state = state,
            .recv_buf = .{},
            .recv_window_consumed = 0,
            .send_window = INITIAL_WINDOW,
            .pending_writes = .{},
            .data_handler = null,
            .close_ctx = null,
            .close_callback = null,
            .proto_msg_handler = null,
            .negotiated_protocol = null,
            .proposed_protocols = null,
        };
        return s;
    }

    /// Free owned memory. Does NOT notify data_handler.
    /// Must only be called from YamuxSession.deinit (which has already notified).
    pub fn deinit(self: *YamuxStream) void {
        self.recv_buf.deinit(self.allocator);
        for (self.pending_writes.items) |pw| {
            pw.callback(pw.callback_ctx, error.StreamClosed);
            self.allocator.free(pw.framed_buf);
        }
        self.pending_writes.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Writes data to this yamux stream.
    /// If the send window is insufficient, the write is queued until a WindowUpdate arrives.
    pub fn write(
        self: *YamuxStream,
        data: []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!usize) void,
    ) void {
        if (self.state == .closed or self.state == .local_close) {
            callback(callback_ctx, error.StreamClosed);
            return;
        }
        if (self.send_window < @as(u32, @intCast(@min(data.len, std.math.maxInt(u32))))) {
            // Queue: allocate the framed buffer now so we own the data
            const framed_buf = self.allocator.alloc(u8, HEADER_LEN + data.len) catch {
                callback(callback_ctx, error.OutOfMemory);
                return;
            };
            const frame = YamuxFrame{ .frame_type = TYPE_DATA, .flags = 0, .stream_id = self.stream_id, .length = @intCast(data.len) };
            frame.encode(framed_buf);
            @memcpy(framed_buf[HEADER_LEN..], data);
            self.pending_writes.append(self.allocator, .{
                .framed_buf = framed_buf,
                .data_len = data.len,
                .callback_ctx = callback_ctx,
                .callback = callback,
            }) catch {
                self.allocator.free(framed_buf);
                callback(callback_ctx, error.OutOfMemory);
            };
            return;
        }
        self.writeFramedData(data, callback_ctx, callback);
    }

    fn writeFramedData(
        self: *YamuxStream,
        data: []const u8,
        callback_ctx: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!usize) void,
    ) void {
        const framed_buf = self.allocator.alloc(u8, HEADER_LEN + data.len) catch {
            callback(callback_ctx, error.OutOfMemory);
            return;
        };
        const frame = YamuxFrame{ .frame_type = TYPE_DATA, .flags = 0, .stream_id = self.stream_id, .length = @intCast(data.len) };
        frame.encode(framed_buf);
        @memcpy(framed_buf[HEADER_LEN..], data);

        const write_ctx = self.allocator.create(FramedWriteCtx) catch {
            self.allocator.free(framed_buf);
            callback(callback_ctx, error.OutOfMemory);
            return;
        };
        write_ctx.* = .{
            .allocator = self.allocator,
            .framed_buf = framed_buf,
            .data_len = data.len,
            .user_ctx = callback_ctx,
            .user_callback = callback,
        };
        self.send_window -= @intCast(data.len);
        if (self.session.handler_ctx) |hctx| {
            hctx.write(framed_buf, write_ctx, FramedWriteCtx.writeCallback);
        } else {
            self.allocator.free(framed_buf);
            self.allocator.destroy(write_ctx);
            callback(callback_ctx, error.SessionNotActive);
        }
    }

    /// Drain pending writes that now fit in the send window.
    fn drainPendingWrites(self: *YamuxStream) void {
        while (self.pending_writes.items.len > 0) {
            const pw = self.pending_writes.items[0];
            const pw_len: u32 = @intCast(@min(pw.data_len, std.math.maxInt(u32)));
            if (self.send_window < pw_len) break;
            _ = self.pending_writes.orderedRemove(0);
            self.send_window -= pw_len;
            // framed_buf already contains the encoded header + data
            const write_ctx = self.allocator.create(FramedWriteCtx) catch {
                pw.callback(pw.callback_ctx, error.OutOfMemory);
                self.allocator.free(pw.framed_buf);
                continue;
            };
            write_ctx.* = .{
                .allocator = self.allocator,
                .framed_buf = pw.framed_buf,
                .data_len = pw.data_len,
                .user_ctx = pw.callback_ctx,
                .user_callback = pw.callback,
            };
            if (self.session.handler_ctx) |hctx| {
                hctx.write(pw.framed_buf, write_ctx, FramedWriteCtx.writeCallback);
            } else {
                pw.callback(pw.callback_ctx, error.SessionNotActive);
                self.allocator.free(pw.framed_buf);
                self.allocator.destroy(write_ctx);
            }
        }
    }

    /// Send FIN to close this stream.
    pub fn close(
        self: *YamuxStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!void) void,
    ) void {
        switch (self.state) {
            .closed => {
                // Already fully closed; fire callback immediately
                callback(callback_ctx, {});
                return;
            },
            .remote_close => {
                // Peer already closed their side; our FIN completes the handshake
                // Fire callback immediately after sending FIN (no more incoming data expected)
                self.state = .closed;
                self.session.sendFrame(.{ .frame_type = TYPE_DATA, .flags = FLAG_FIN, .stream_id = self.stream_id, .length = 0 });
                callback(callback_ctx, {});
                return;
            },
            else => {
                self.state = .local_close;
            },
        }
        self.close_ctx = callback_ctx;
        self.close_callback = callback;
        self.session.sendFrame(.{ .frame_type = TYPE_DATA, .flags = FLAG_FIN, .stream_id = self.stream_id, .length = 0 });
    }

    /// Register a data handler for incoming stream data.
    /// Step 1.2 will wrap this into the full AnyStream/AnyProtocolMessageHandler interface.
    pub fn setDataHandler(self: *YamuxStream, handler: AnyStreamDataHandler) void {
        self.data_handler = handler;
        // Deliver any data that arrived before the handler was registered
        if (self.recv_buf.items.len > 0) {
            const data = self.recv_buf.items;
            handler.onData(data) catch {};
            self.recv_buf.clearRetainingCapacity();
        }
    }

    /// Called by YamuxSession when bytes arrive for this stream.
    fn onData(self: *YamuxStream, data: []const u8, flags: u16) !void {
        if (flags & FLAG_RST != 0) {
            self.state = .closed;
            if (self.data_handler) |h| h.onClose();
            return;
        }

        if (flags & FLAG_FIN != 0) {
            switch (self.state) {
                .established => self.state = .remote_close,
                .local_close => {
                    // Both sides have now closed
                    self.state = .closed;
                    if (self.close_callback) |cb| cb(self.close_ctx, {});
                },
                else => {},
            }
            if (self.data_handler) |h| h.onClose();
        }

        if (data.len > 0) {
            self.recv_window_consumed += @intCast(data.len);
            if (self.recv_window_consumed >= WINDOW_UPDATE_THRESHOLD) {
                self.session.sendFrame(.{
                    .frame_type = TYPE_WINDOW_UPDATE,
                    .flags = 0,
                    .stream_id = self.stream_id,
                    .length = self.recv_window_consumed,
                });
                self.recv_window_consumed = 0;
            }

            if (self.data_handler) |h| {
                try h.onData(data);
            } else {
                try self.recv_buf.appendSlice(self.allocator, data);
            }
        }
    }

    /// Wrap this YamuxStream as an AnyStream for use by protocol handlers (mss, ping, etc.)
    pub fn any(self: *YamuxStream) libp2p.protocols.AnyStream {
        return .{ .ptr = self, .vtable = &yamux_stream_vtable };
    }
};

// --- AnyStream vtable functions for YamuxStream ---

fn yamuxStreamWriteFn(ptr: *anyopaque, data: []const u8, ctx: ?*anyopaque, cb: *const fn (?*anyopaque, anyerror!usize) void) void {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    self.write(data, ctx, cb);
}

fn yamuxStreamCloseFn(ptr: *anyopaque, ctx: ?*anyopaque, cb: *const fn (?*anyopaque, anyerror!void) void) void {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    self.close(ctx, cb);
}

fn yamuxStreamSetProtoMsgHandlerFn(ptr: *anyopaque, handler: libp2p.protocols.AnyProtocolMessageHandler) void {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    self.proto_msg_handler = handler;
}

fn yamuxStreamGetProtoMsgHandlerFn(ptr: *anyopaque) ?libp2p.protocols.AnyProtocolMessageHandler {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    return self.proto_msg_handler;
}

fn yamuxStreamSetNegotiatedProtocolFn(ptr: *anyopaque, proto: ?libp2p.protocols.ProtocolId) void {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    self.negotiated_protocol = proto;
}

fn yamuxStreamGetNegotiatedProtocolFn(ptr: *anyopaque) ?libp2p.protocols.ProtocolId {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    return self.negotiated_protocol;
}

fn yamuxStreamGetProposedProtocolsFn(ptr: *anyopaque) ?[]const libp2p.protocols.ProtocolId {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    return self.proposed_protocols;
}

fn yamuxStreamGetRemotePeerIdFn(ptr: *anyopaque) ?PeerId {
    const self: *YamuxStream = @ptrCast(@alignCast(ptr));
    return self.session.remote_peer_id;
}

const yamux_stream_vtable = libp2p.protocols.AnyStream.VTable{
    .writeFn = yamuxStreamWriteFn,
    .closeFn = yamuxStreamCloseFn,
    .setProtoMsgHandlerFn = yamuxStreamSetProtoMsgHandlerFn,
    .getProtoMsgHandlerFn = yamuxStreamGetProtoMsgHandlerFn,
    .setNegotiatedProtocolFn = yamuxStreamSetNegotiatedProtocolFn,
    .getNegotiatedProtocolFn = yamuxStreamGetNegotiatedProtocolFn,
    .getProposedProtocolsFn = yamuxStreamGetProposedProtocolsFn,
    .getRemotePeerIdFn = yamuxStreamGetRemotePeerIdFn,
};

/// Parser state for reading yamux frames from a byte stream.
const FrameParser = struct {
    allocator: Allocator,
    header_buf: [HEADER_LEN]u8 = undefined,
    header_pos: usize = 0,
    current_header: ?YamuxFrame = null,
    payload_buf: std.ArrayList(u8),

    fn init(allocator: Allocator) FrameParser {
        return .{ .allocator = allocator, .payload_buf = .{} };
    }

    fn deinit(self: *FrameParser) void {
        self.payload_buf.deinit(self.allocator);
    }

    /// Feed raw bytes; calls session.dispatchFrame for each complete frame.
    fn feed(self: *FrameParser, data: []const u8, session: *YamuxSession) !void {
        var pos: usize = 0;
        while (pos < data.len) {
            if (self.current_header == null) {
                const needed = HEADER_LEN - self.header_pos;
                const available = data.len - pos;
                const to_copy = @min(needed, available);
                @memcpy(self.header_buf[self.header_pos .. self.header_pos + to_copy], data[pos .. pos + to_copy]);
                self.header_pos += to_copy;
                pos += to_copy;
                if (self.header_pos == HEADER_LEN) {
                    self.current_header = YamuxFrame.decode(&self.header_buf);
                    self.header_pos = 0;
                    if (self.current_header.?.length == 0) {
                        try session.dispatchFrame(self.current_header.?, &.{});
                        self.current_header = null;
                    }
                }
            } else {
                const frame = self.current_header.?;
                const have: u32 = @intCast(self.payload_buf.items.len);
                const remaining: usize = frame.length - have;
                const available = data.len - pos;
                const to_copy = @min(remaining, available);
                try self.payload_buf.appendSlice(self.allocator, data[pos .. pos + to_copy]);
                pos += to_copy;
                if (self.payload_buf.items.len == frame.length) {
                    try session.dispatchFrame(frame, self.payload_buf.items);
                    self.payload_buf.clearRetainingCapacity();
                    self.current_header = null;
                }
            }
        }
    }
};

/// A pending acceptStream callback.
const AcceptCallback = struct {
    ctx: ?*anyopaque,
    callback: *const fn (?*anyopaque, anyerror!*YamuxStream) void,
};

/// YamuxSession manages all yamux streams over a single TCP AnyConn.
/// It is a ConnHandler inserted into the TCP connection's HandlerPipeline.
///
/// Usage:
///   var session = try YamuxSession.init(allocator, conn, is_client);
///   try conn.getPipeline().addLast("yamux", session.handler());
///   try conn.getPipeline().fireActive();
pub const YamuxSession = struct {
    allocator: Allocator,
    conn: AnyConn,
    is_client: bool,
    streams: std.AutoHashMap(u32, *YamuxStream),
    next_stream_id: u32,
    parser: FrameParser,
    pending_accept: std.ArrayList(*YamuxStream),
    accept_waiters: std.ArrayList(AcceptCallback),
    /// Remote peer identity; set after TLS handshake completes (Step 1.4).
    remote_peer_id: ?PeerId,
    /// Our own ConnHandlerContext in the pipeline. Set in onActiveImpl.
    /// Used for outbound writes so they pass through TLS (findPrevOutbound).
    handler_ctx: ?*ConnHandlerContext = null,

    pub fn init(allocator: Allocator, conn: AnyConn, is_client: bool) !*YamuxSession {
        const s = try allocator.create(YamuxSession);
        s.* = .{
            .allocator = allocator,
            .conn = conn,
            .is_client = is_client,
            .streams = std.AutoHashMap(u32, *YamuxStream).init(allocator),
            .next_stream_id = if (is_client) 1 else 2,
            .parser = FrameParser.init(allocator),
            .pending_accept = .{},
            .accept_waiters = .{},
            .remote_peer_id = null,
        };
        return s;
    }

    pub fn deinit(self: *YamuxSession) void {
        // Streams in pending_accept are also in the streams map; deinit them once via streams.
        var it = self.streams.valueIterator();
        while (it.next()) |stream| stream.*.deinit();
        self.streams.deinit();
        self.parser.deinit();
        // pending_accept holds references already freed above; just discard the list
        self.pending_accept.deinit(self.allocator);
        self.accept_waiters.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Open a new outbound stream. Sends SYN frame.
    pub fn openStream(self: *YamuxSession) !*YamuxStream {
        const id = self.next_stream_id;
        self.next_stream_id += 2;
        const stream = try YamuxStream.init(self.allocator, id, self, .syn_sent);
        try self.streams.put(id, stream);
        self.sendFrame(.{ .frame_type = TYPE_DATA, .flags = FLAG_SYN, .stream_id = id, .length = 0 });
        return stream;
    }

    /// Register a callback for the next inbound stream opened by the peer.
    pub fn acceptStream(
        self: *YamuxSession,
        callback_ctx: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!*YamuxStream) void,
    ) void {
        if (self.pending_accept.items.len > 0) {
            const stream = self.pending_accept.orderedRemove(0);
            callback(callback_ctx, stream);
            return;
        }
        self.accept_waiters.append(self.allocator, .{ .ctx = callback_ctx, .callback = callback }) catch {
            callback(callback_ctx, error.OutOfMemory);
        };
    }

    // --- Internal: single helper for all header-only frames ---

    /// Send a frame with no additional payload (header-only).
    /// Used for SYN, ACK, FIN, WindowUpdate, Ping.
    pub fn sendFrame(self: *YamuxSession, frame: YamuxFrame) void {
        const buf = self.allocator.alloc(u8, HEADER_LEN) catch return;
        frame.encode(buf);
        const write_ctx = self.allocator.create(FramedWriteCtx) catch {
            self.allocator.free(buf);
            return;
        };
        write_ctx.* = .{
            .allocator = self.allocator,
            .framed_buf = buf,
            .data_len = 0,
            .user_ctx = null,
            .user_callback = noopWriteCallback,
        };
        // Route through pipeline (ctx.write → findPrevOutbound → TLS → Head → TCP).
        // Never call conn.write() directly here; that bypasses TLS encryption.
        if (self.handler_ctx) |hctx| {
            hctx.write(buf, write_ctx, FramedWriteCtx.writeCallback);
        } else {
            // Pipeline not yet active; fall back to raw conn (pre-TLS setup only).
            self.allocator.free(buf);
            self.allocator.destroy(write_ctx);
        }
    }

    fn noopWriteCallback(_: ?*anyopaque, _: anyerror!usize) void {}

    // --- Frame dispatch ---

    fn dispatchFrame(self: *YamuxSession, frame: YamuxFrame, payload: []const u8) !void {
        switch (frame.frame_type) {
            TYPE_DATA => try self.handleData(frame, payload),
            TYPE_WINDOW_UPDATE => self.handleWindowUpdate(frame),
            TYPE_PING => self.handlePing(frame),
            TYPE_GO_AWAY => self.handleGoAway(),
            else => {}, // Unknown frame type; ignore per spec
        }
    }

    fn handleData(self: *YamuxSession, frame: YamuxFrame, payload: []const u8) !void {
        // New stream opened by peer: SYN flag set AND we haven't seen this ID before
        if (frame.flags & FLAG_SYN != 0 and self.streams.get(frame.stream_id) == null) {
            const stream = try YamuxStream.init(self.allocator, frame.stream_id, self, .syn_recv);
            try self.streams.put(frame.stream_id, stream);
            // Respond with SYN+ACK
            self.sendFrame(.{ .frame_type = TYPE_DATA, .flags = FLAG_SYN | FLAG_ACK, .stream_id = frame.stream_id, .length = 0 });
            stream.state = .established;

            // Deliver any payload that piggybacked on the SYN
            if (payload.len > 0) try stream.onData(payload, frame.flags & ~FLAG_SYN);

            if (self.accept_waiters.items.len > 0) {
                const waiter = self.accept_waiters.orderedRemove(0);
                waiter.callback(waiter.ctx, stream);
            } else {
                try self.pending_accept.append(self.allocator, stream);
            }
            return;
        }

        // ACK for a stream we opened: SYN+ACK response
        if (frame.flags & FLAG_ACK != 0) {
            if (self.streams.get(frame.stream_id)) |stream| {
                if (stream.state == .syn_sent) stream.state = .established;
                if (payload.len > 0) try stream.onData(payload, frame.flags & ~FLAG_ACK);
                return;
            }
        }

        // Regular data or FIN/RST for an existing stream
        if (self.streams.get(frame.stream_id)) |stream| {
            try stream.onData(payload, frame.flags);
        }
        // Unknown stream ID: ignore (peer may have reset it already)
    }

    fn handleWindowUpdate(self: *YamuxSession, frame: YamuxFrame) void {
        if (self.streams.get(frame.stream_id)) |stream| {
            stream.send_window +|= frame.length; // saturating add
            stream.drainPendingWrites();
        }
    }

    fn handlePing(self: *YamuxSession, frame: YamuxFrame) void {
        if (frame.flags & FLAG_ACK == 0) {
            // Peer sent a ping; reply with ACK and echo the payload (length field)
            self.sendFrame(.{ .frame_type = TYPE_PING, .flags = FLAG_ACK, .stream_id = 0, .length = frame.length });
        }
    }

    fn handleGoAway(self: *YamuxSession) void {
        var it = self.streams.valueIterator();
        while (it.next()) |stream| {
            stream.*.state = .closed;
            if (stream.*.data_handler) |h| h.onClose();
        }
    }

    // --- ConnHandler interface ---

    fn onActiveImpl(self: *YamuxSession, ctx: *ConnHandlerContext) !void {
        self.handler_ctx = ctx;
        try ctx.fireActive();
    }

    fn onInactiveImpl(self: *YamuxSession, ctx: *ConnHandlerContext) void {
        self.handleGoAway();
        ctx.fireInactive();
    }

    fn onReadImpl(self: *YamuxSession, ctx: *ConnHandlerContext, msg: []const u8) !void {
        _ = ctx; // yamux consumes all raw bytes; does not forward
        try self.parser.feed(msg, self);
    }

    fn onReadCompleteImpl(_: *YamuxSession, _: *ConnHandlerContext) void {}

    fn onErrorCaughtImpl(self: *YamuxSession, ctx: *ConnHandlerContext, err: anyerror) void {
        self.handleGoAway();
        ctx.fireErrorCaught(err);
    }

    fn writeImpl(_: *YamuxSession, ctx: *ConnHandlerContext, buffer: []const u8, cbi: ?*anyopaque, cb: *const fn (?*anyopaque, anyerror!usize) void) void {
        ctx.write(buffer, cbi, cb);
    }

    fn closeImpl(_: *YamuxSession, ctx: *ConnHandlerContext, cbi: ?*anyopaque, cb: *const fn (?*anyopaque, anyerror!void) void) void {
        ctx.close(cbi, cb);
    }

    // --- Static vtable wrappers ---

    fn vtableOnActive(ptr: *anyopaque, ctx: *ConnHandlerContext) !void {
        return @as(*YamuxSession, @ptrCast(@alignCast(ptr))).onActiveImpl(ctx);
    }
    fn vtableOnInactive(ptr: *anyopaque, ctx: *ConnHandlerContext) void {
        @as(*YamuxSession, @ptrCast(@alignCast(ptr))).onInactiveImpl(ctx);
    }
    fn vtableOnRead(ptr: *anyopaque, ctx: *ConnHandlerContext, msg: []const u8) !void {
        return @as(*YamuxSession, @ptrCast(@alignCast(ptr))).onReadImpl(ctx, msg);
    }
    fn vtableOnReadComplete(ptr: *anyopaque, ctx: *ConnHandlerContext) void {
        @as(*YamuxSession, @ptrCast(@alignCast(ptr))).onReadCompleteImpl(ctx);
    }
    fn vtableOnErrorCaught(ptr: *anyopaque, ctx: *ConnHandlerContext, err: anyerror) void {
        @as(*YamuxSession, @ptrCast(@alignCast(ptr))).onErrorCaughtImpl(ctx, err);
    }
    fn vtableWrite(ptr: *anyopaque, ctx: *ConnHandlerContext, buf: []const u8, cbi: ?*anyopaque, cb: *const fn (?*anyopaque, anyerror!usize) void) void {
        @as(*YamuxSession, @ptrCast(@alignCast(ptr))).writeImpl(ctx, buf, cbi, cb);
    }
    fn vtableClose(ptr: *anyopaque, ctx: *ConnHandlerContext, cbi: ?*anyopaque, cb: *const fn (?*anyopaque, anyerror!void) void) void {
        @as(*YamuxSession, @ptrCast(@alignCast(ptr))).closeImpl(ctx, cbi, cb);
    }

    const vtable = ConnHandlerVTable{
        .onActiveFn = vtableOnActive,
        .onInactiveFn = vtableOnInactive,
        .onReadFn = vtableOnRead,
        .onReadCompleteFn = vtableOnReadComplete,
        .onErrorCaughtFn = vtableOnErrorCaught,
        .writeFn = vtableWrite,
        .closeFn = vtableClose,
    };

    pub fn handler(self: *YamuxSession) AnyConnHandler {
        return .{ .instance = self, .vtable = &vtable };
    }
};

// --- Tests ---

test "YamuxFrame encode/decode roundtrip - DATA/SYN" {
    var buf: [HEADER_LEN]u8 = undefined;
    const frame = YamuxFrame{ .frame_type = TYPE_DATA, .flags = FLAG_SYN, .stream_id = 1, .length = 42 };
    frame.encode(&buf);
    const got = YamuxFrame.decode(&buf);
    try std.testing.expectEqual(frame.version, got.version);
    try std.testing.expectEqual(frame.frame_type, got.frame_type);
    try std.testing.expectEqual(frame.flags, got.flags);
    try std.testing.expectEqual(frame.stream_id, got.stream_id);
    try std.testing.expectEqual(frame.length, got.length);
}

test "YamuxFrame encode/decode roundtrip - WindowUpdate" {
    var buf: [HEADER_LEN]u8 = undefined;
    const frame = YamuxFrame{ .frame_type = TYPE_WINDOW_UPDATE, .flags = 0, .stream_id = 3, .length = INITIAL_WINDOW };
    frame.encode(&buf);
    const got = YamuxFrame.decode(&buf);
    try std.testing.expectEqual(TYPE_WINDOW_UPDATE, got.frame_type);
    try std.testing.expectEqual(@as(u32, 3), got.stream_id);
    try std.testing.expectEqual(INITIAL_WINDOW, got.length);
}

test "YamuxFrame encode/decode roundtrip - Ping ACK" {
    var buf: [HEADER_LEN]u8 = undefined;
    const frame = YamuxFrame{ .frame_type = TYPE_PING, .flags = FLAG_ACK, .stream_id = 0, .length = 0xDEADBEEF };
    frame.encode(&buf);
    const got = YamuxFrame.decode(&buf);
    try std.testing.expectEqual(TYPE_PING, got.frame_type);
    try std.testing.expectEqual(FLAG_ACK, got.flags);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), got.length);
}

test "FrameParser parses a complete frame fed in one chunk" {
    const allocator = std.testing.allocator;
    var parser = FrameParser.init(allocator);
    defer parser.deinit();

    // Build a DATA frame with 4 bytes payload
    var raw: [HEADER_LEN + 4]u8 = undefined;
    const frame = YamuxFrame{ .frame_type = TYPE_DATA, .flags = FLAG_SYN, .stream_id = 1, .length = 4 };
    frame.encode(raw[0..HEADER_LEN]);
    @memcpy(raw[HEADER_LEN..], "test");

    // Feed into a minimal mock session that records dispatched frames
    const MockSession = struct {
        dispatched_frame: ?YamuxFrame = null,
        dispatched_payload: [16]u8 = undefined,
        dispatched_payload_len: usize = 0,

        fn dispatchFrame(self: *@This(), f: YamuxFrame, payload: []const u8) !void {
            self.dispatched_frame = f;
            self.dispatched_payload_len = payload.len;
            if (payload.len > 0) @memcpy(self.dispatched_payload[0..payload.len], payload);
        }
    };
    var mock = MockSession{};

    // Reuse FrameParser.feed signature but call with mock
    // FrameParser.feed expects *YamuxSession; test it directly via the parse logic
    var pos: usize = 0;
    while (pos < raw.len) {
        if (parser.current_header == null) {
            const needed = HEADER_LEN - parser.header_pos;
            const available = raw.len - pos;
            const to_copy = @min(needed, available);
            @memcpy(parser.header_buf[parser.header_pos .. parser.header_pos + to_copy], raw[pos .. pos + to_copy]);
            parser.header_pos += to_copy;
            pos += to_copy;
            if (parser.header_pos == HEADER_LEN) {
                parser.current_header = YamuxFrame.decode(&parser.header_buf);
                parser.header_pos = 0;
                if (parser.current_header.?.length == 0) {
                    try mock.dispatchFrame(parser.current_header.?, &.{});
                    parser.current_header = null;
                }
            }
        } else {
            const hdr = parser.current_header.?;
            const have: u32 = @intCast(parser.payload_buf.items.len);
            const remaining: usize = hdr.length - have;
            const available = raw.len - pos;
            const to_copy = @min(remaining, available);
            try parser.payload_buf.appendSlice(allocator, raw[pos .. pos + to_copy]);
            pos += to_copy;
            if (parser.payload_buf.items.len == hdr.length) {
                try mock.dispatchFrame(hdr, parser.payload_buf.items);
                parser.payload_buf.clearRetainingCapacity();
                parser.current_header = null;
            }
        }
    }

    try std.testing.expect(mock.dispatched_frame != null);
    try std.testing.expectEqual(TYPE_DATA, mock.dispatched_frame.?.frame_type);
    try std.testing.expectEqual(FLAG_SYN, mock.dispatched_frame.?.flags);
    try std.testing.expectEqual(@as(u32, 1), mock.dispatched_frame.?.stream_id);
    try std.testing.expectEqualSlices(u8, "test", mock.dispatched_payload[0..mock.dispatched_payload_len]);
}

test "FrameParser handles frame split across two chunks" {
    const allocator = std.testing.allocator;
    var parser = FrameParser.init(allocator);
    defer parser.deinit();

    var raw: [HEADER_LEN + 6]u8 = undefined;
    const frame = YamuxFrame{ .frame_type = TYPE_DATA, .flags = 0, .stream_id = 7, .length = 6 };
    frame.encode(raw[0..HEADER_LEN]);
    @memcpy(raw[HEADER_LEN..], "hello!");

    // Split: first 8 bytes (partial header), then the rest
    const chunks = [_][]const u8{ raw[0..8], raw[8..] };

    var dispatched_stream_id: u32 = 0;
    var dispatched_payload: [16]u8 = undefined;
    var dispatched_len: usize = 0;

    for (chunks) |chunk| {
        var pos: usize = 0;
        while (pos < chunk.len) {
            if (parser.current_header == null) {
                const needed = HEADER_LEN - parser.header_pos;
                const available = chunk.len - pos;
                const to_copy = @min(needed, available);
                @memcpy(parser.header_buf[parser.header_pos .. parser.header_pos + to_copy], chunk[pos .. pos + to_copy]);
                parser.header_pos += to_copy;
                pos += to_copy;
                if (parser.header_pos == HEADER_LEN) {
                    parser.current_header = YamuxFrame.decode(&parser.header_buf);
                    parser.header_pos = 0;
                }
            } else {
                const hdr = parser.current_header.?;
                const have: u32 = @intCast(parser.payload_buf.items.len);
                const remaining: usize = hdr.length - have;
                const available = chunk.len - pos;
                const to_copy = @min(remaining, available);
                try parser.payload_buf.appendSlice(allocator, chunk[pos .. pos + to_copy]);
                pos += to_copy;
                if (parser.payload_buf.items.len == hdr.length) {
                    dispatched_stream_id = hdr.stream_id;
                    dispatched_len = parser.payload_buf.items.len;
                    @memcpy(dispatched_payload[0..dispatched_len], parser.payload_buf.items);
                    parser.payload_buf.clearRetainingCapacity();
                    parser.current_header = null;
                }
            }
        }
    }

    try std.testing.expectEqual(@as(u32, 7), dispatched_stream_id);
    try std.testing.expectEqualSlices(u8, "hello!", dispatched_payload[0..dispatched_len]);
}
