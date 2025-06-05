const std = @import("std");
const Allocator = std.mem.Allocator;
const frame = @import("frame.zig");
const Stream = @import("stream.zig").Stream;
const StreamError = @import("stream.zig").Error;
const conn = @import("../../conn.zig");
const AnyConn = conn.AnyConn;
const Config = @import("Config.zig");
const testing = std.testing;
const ThreadPool = std.Thread.Pool;
const BlockingQueue = @import("../../concurrent/blocking_queue.zig").BlockingQueue;
const xev = @import("xev");
const HeaderPool = std.heap.MemoryPool([frame.Header.SIZE]u8);

pub const Error = error{ KeepAliveTimeout, SessionShutdown, ConnectionWriteTimeout, OutOfMemory, RemoteGoAway, StreamsExhausted, DuplicateStream, GoAwayProtocolError, GoAwayInternalError, UnexpectedGoAwayError } || frame.Error || StreamError;
pub const SendQueue = std.SinglyLinkedList(*SendReady);

const SentCompletion = struct {
    done: std.Thread.ResetEvent = .{},
    rwlock: std.Thread.RwLock = .{},
    err: ?anyerror = null,

    pub fn setError(self: *SentCompletion, err: anyerror) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();

        if (self.err == null) {
            self.err = err;
        }
    }

    pub fn getErr(self: *SentCompletion) ?anyerror {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();

        return self.err;
    }
};

pub const SendReady = struct {
    hdr: []u8,
    body: ?[]u8 = null,
    sent_completion: ?*SentCompletion = null,
    allocator: Allocator,

    const Self = @This();

    pub fn init(hdr: []u8, body: ?[]u8, sent_completion: ?*SentCompletion, allocator: Allocator) !SendReady {
        // Since the body will be reused by the caller, we need to copy it.
        // This is possibly not the best way to do this, but it is the simplest.
        // We could also use a pool of buffers to avoid copying the body later.
        const hdr_copy = try allocator.dupe(u8, hdr);
        var body_copy: ?[]u8 = null;
        if (body) |b| {
            body_copy = try allocator.dupe(u8, b);
        }

        return .{
            .hdr = hdr_copy,
            .body = body_copy,
            .sent_completion = sent_completion,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.hdr);

        if (self.body) |body| {
            self.allocator.free(body);
        }
    }
};

// pub const SessionHandler= conn.GenericHandler(comptime Context: type, comptime onReadFn: fn(context:Context, buffer:[]u8)void)

pub const Session = struct {
    // remoteGoAway indicates the remote side does not want further connections.
    // Must be first for alignment.
    remote_go_away: std.atomic.Value(i32) = std.atomic.Value(i32).init(0),

    // localGoAway indicates that we should stop accepting further connections.
    // Must be first for alignment.
    local_go_away: std.atomic.Value(i32) = std.atomic.Value(i32).init(0),

    // nextStreamID is the next stream we should send.
    // This depends if we are a client/server.
    next_stream_id: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    // config holds our configuration
    config: *Config,

    // conn is the underlying connection
    conn: AnyConn,

    // bufRead is a buffered reader
    buf_read: std.io.AnyReader,

    // pings is used to track inflight pings
    pings: std.AutoHashMap(u32, std.Thread.ResetEvent),
    ping_id: u32 = 0,
    ping_mutex: std.Thread.Mutex = .{},

    // streams maps a stream id to a stream, and inflight has an entry
    // for any outgoing stream that has not yet been established.
    // Both are protected by stream_mutex.
    streams: std.AutoHashMap(u32, Stream),
    inflight: std.AutoHashMap(u32, void),
    stream_mutex: std.Thread.Mutex = .{},

    // syn_semaphore acts like a semaphore. It is sized to the AcceptBacklog
    // which is assumed to be symmetric between the client and server.
    // This allows the client to avoid exceeding the backlog and instead
    // blocks the open.
    syn_semaphore: std.Thread.Semaphore = .{},

    // accept_queue is used to pass ready streams to the client
    accept_queue: *BlockingQueue(*Stream),

    // send_queue is used to mark a stream as ready to send,
    // or to send a header out directly.
    send_queue: SendQueue,

    send_queue_sync: struct {
        mutex: std.Thread.Mutex = .{},
        not_empty_cond: std.Thread.Condition = .{},
        not_full_cond: std.Thread.Condition = .{},
    } = .{},

    send_queue_size: usize = 0,

    send_queue_capacity: usize = 64,

    // recv_done and send_done are used to coordinate shutdown
    recv_done: struct {
        mutex: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        done: bool = false,
    } = .{},

    send_done: struct {
        mutex: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        done: bool = false,
    } = .{},

    inbound_buffer: std.fifo.LinearFifo(u8, .Dynamic),

    header_pool: HeaderPool,

    shutdown_completion: std.Thread.ResetEvent = .{},

    shutdown_mutex: std.Thread.Mutex = .{},

    shutdown_err: ?anyerror = null,

    shutdown_err_mutex: std.Thread.Mutex = .{},

    allocator: Allocator,

    scheduler: *ThreadPool,

    pub fn init(allocator: Allocator, config: *Config, any_conn: AnyConn, s: *Session, scheduler: *ThreadPool) !void {
        s.* = Session{
            .config = config,
            .conn = any_conn,
            .buf_read = any_conn.reader(),
            .allocator = allocator,
            .accept_queue = try BlockingQueue(*Stream).create(allocator, config.accept_backlog),
            .send_queue = .{},
            .pings = std.AutoHashMap(u32, std.Thread.ResetEvent).init(allocator),
            .streams = std.AutoHashMap(u32, Stream).init(allocator),
            .inflight = std.AutoHashMap(u32, void).init(allocator),
            .scheduler = scheduler,
            .inbound_buffer = std.fifo.LinearFifo(u8, .Dynamic).init(allocator),
            .header_pool = HeaderPool.init(allocator),
        };
    }

    pub fn initAndStart(allocator: Allocator, config: *Config, any_conn: AnyConn, s: *Session, scheduler: *ThreadPool) !void {
        try init(allocator, config, any_conn, s, scheduler);

        try s.scheduler.spawn(sendLoopInThread, .{s});
    }

    pub fn deinit(self: *Session) void {
        self.streams.deinit();
        self.inflight.deinit();
        self.pings.deinit();
        self.accept_queue.destroy(self.allocator);
        self.send_queue_sync.mutex.lock();
        while (self.send_queue.popFirst()) |node| {
            const send_ready = node.data;
            if (send_ready.sent_completion) |sent_completion| {
                self.allocator.destroy(sent_completion);
            }
            send_ready.deinit();
            self.allocator.destroy(send_ready);
            self.allocator.destroy(node);
        }
        self.send_queue_sync.mutex.unlock();
        self.header_pool.deinit();
        self.inbound_buffer.deinit();
    }

    pub fn sendAndWait(self: *Session, hdr: []u8, body: ?[]u8) !void {
        const start_time = std.time.nanoTimestamp();
        const send_ready = try self.allocator.create(SendReady);
        const sent_completion = try self.allocator.create(SentCompletion);
        sent_completion.* = .{};
        send_ready.* = try SendReady.init(hdr, body, sent_completion, self.allocator);

        try self.waitEnqueue(send_ready, self.config.connection_write_timeout);

        const elapsed_time: i128 = std.time.nanoTimestamp() - start_time;
        if (self.config.connection_write_timeout <= elapsed_time) {
            return error.ConnectionWriteTimeout;
        }
        try self.waitSent(sent_completion, self.config.connection_write_timeout - @as(u64, @intCast(elapsed_time)));
    }

    pub fn send(self: *Session, hdr: []u8, body: ?[]u8) Error!void {
        const send_ready = try self.allocator.create(SendReady);
        send_ready.* = try SendReady.init(hdr, body, null, self.allocator);

        try self.waitEnqueue(send_ready, self.config.connection_write_timeout);
    }

    pub fn sendLoopInThread(self: *Session) void {
        self.sendLoop() catch |err| {
            std.debug.print("sendLoopInThread error: {}\n", .{err});
        };
    }

    pub fn sendLoop(self: *Session) !void {
        while (!self.shutdown_completion.isSet()) {
            self.send_queue_sync.mutex.lock();
            while (self.send_queue_size == 0) {
                // Wait for a notification that the queue is not empty
                self.send_queue_sync.not_empty_cond.wait(&self.send_queue_sync.mutex);
                if (self.shutdown_completion.isSet()) {
                    self.send_queue_sync.mutex.unlock();
                    return;
                }
            }
            const node = self.send_queue.popFirst().?;
            defer self.allocator.destroy(node);
            const send_ready = node.data;
            defer {
                send_ready.deinit();
                self.allocator.destroy(send_ready);
            }
            self.send_queue_size -= 1;
            self.send_queue_sync.not_full_cond.broadcast();
            self.send_queue_sync.mutex.unlock();

            // Check if the session is shutting down
            if (self.shutdown_completion.isSet()) {
                if (send_ready.sent_completion) |sent_completion| {
                    if (sent_completion.done.isSet()) {
                        self.allocator.destroy(sent_completion);
                    } else {
                        sent_completion.setError(Error.SessionShutdown);
                        sent_completion.done.set();
                    }
                }
                return;
            }

            _ = self.conn.write(send_ready.hdr) catch |err| {
                std.debug.print("sendLoop: write error: {}\n", .{err});
                if (send_ready.sent_completion) |sent_completion| {
                    if (sent_completion.done.isSet()) {
                        self.allocator.destroy(sent_completion);
                    } else {
                        sent_completion.setError(err);
                        sent_completion.done.set();
                    }
                }
                return err;
            };

            if (send_ready.body) |body| {
                _ = self.conn.write(body) catch |err| {
                    std.debug.print("sendLoop: write error: {}\n", .{err});
                    if (send_ready.sent_completion) |sent_completion| {
                        if (sent_completion.done.isSet()) {
                            self.allocator.destroy(sent_completion);
                        } else {
                            sent_completion.setError(err);
                            sent_completion.done.set();
                        }
                    }
                    return err;
                };
            }

            if (send_ready.sent_completion) |sent_completion| {
                if (sent_completion.done.isSet()) {
                    self.allocator.destroy(sent_completion);
                } else {
                    sent_completion.done.set();
                }
            }
        }
    }

    pub fn streamsNum(self: *Session) usize {
        self.stream_mutex.lock();
        defer self.stream_mutex.unlock();
        return self.streams.count();
    }

    pub fn openStream(self: *Session) !Stream {
        if (self.isClosed()) {
            return Error.SessionShutdown;
        }
        if (self.remote_go_away.load(.acquire) == 1) {
            return Error.RemoteGoAway;
        }

        // Block if we have too many inflight SYNs
        self.syn_semaphore.wait();
        errdefer self.syn_semaphore.post();
        if (self.isClosed()) {
            return Error.SessionShutdown;
        }

        const id = try self.getNextStreamId();

        var stream: Stream = undefined;
        try Stream.init(&stream, self, id, .init, self.allocator);
        errdefer stream.deinit();

        self.stream_mutex.lock();
        try self.streams.put(id, stream);
        try self.inflight.put(id, {});
        self.stream_mutex.unlock();

        // Setup timeout if needed
        if (self.config.stream_open_timeout > 0) {
            try self.scheduler.spawn(setOpenTimeoutInThread, .{ self, stream });
        }

        // Send the window update to create the stream
        stream.sendWindowUpdate() catch |err| {
            _ = self.streams.remove(id);
            _ = self.inflight.remove(id);
            self.syn_semaphore.post();
            return err;
        };

        return stream;
    }

    fn setOpenTimeoutInThread(self: *Session, stream: Stream) void {
        self.setOpenTimeout(stream) catch |err| {
            std.debug.print("setOpenTimeout error: {}\n", .{err});
        };
    }

    fn setOpenTimeout(self: *Session, _: Stream) !void {
        const start_time = std.time.nanoTimestamp();
        const timeout_ns = self.config.stream_open_timeout;

        while (true) {
            // Check if the stream is established
            // if (stream.isEstablished()) {
            //     return;
            // }

            // Check if the session is shutdown
            if (self.shutdown_completion.isSet()) {
                return;
            }

            // Check if timeout is reached
            const elapsed = std.time.nanoTimestamp() - start_time;
            if (elapsed >= timeout_ns) {
                std.debug.print("[ERR] yamux: aborted stream open: timeout\n", .{});
                self.close();
                return;
            }

            // Sleep for a short time before checking again
            const remaining = @as(u64, @intCast(@max(0, timeout_ns - elapsed)));
            const sleep_time = @min(remaining, 10 * std.time.ns_per_ms);
            std.time.sleep(sleep_time);
        }
    }

    pub fn isClosed(self: *Session) bool {
        return self.shutdown_completion.isSet();
    }

    pub fn close(self: *Session) void {
        self.shutdown_mutex.lock();
        defer self.shutdown_mutex.unlock();

        self.shutdown_err_mutex.lock();
        if (self.shutdown_err == null) {
            self.shutdown_err = Error.SessionShutdown;
        }
        self.shutdown_err_mutex.unlock();

        self.shutdown_completion.set();

        self.conn.close() catch |err| {
            std.log.warn("close: conn close error: {}\n", .{err});
        };
        self.recv_done.cond.broadcast();

        self.stream_mutex.lock();
        var iter = self.streams.valueIterator();
        while (iter.next()) |stream| {
            stream.forceClose();
        }
        self.stream_mutex.unlock();
        self.send_done.cond.broadcast();

        self.send_queue_sync.mutex.lock();
        self.send_queue_sync.not_empty_cond.broadcast();
        self.send_queue_sync.not_full_cond.broadcast();
        self.send_queue_sync.mutex.unlock();
    }

    pub fn ping(self: *Session) !u64 {
        var ping_notification: std.Thread.ResetEvent = .{};
        self.ping_mutex.lock();
        const ping_id = self.ping_id;
        self.ping_id += 1;
        try self.pings.put(ping_id, ping_notification);
        self.ping_mutex.unlock();

        const ping_hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
        defer self.allocator.free(ping_hdr);

        try frame.Header.init(frame.FrameType.PING, frame.FrameFlags.SYN, 0, ping_id).encode(ping_hdr);
        try self.sendAndWait(ping_hdr, null);

        const start = std.time.nanoTimestamp();
        ping_notification.timedWait(self.config.connection_write_timeout) catch |err| {
            if (self.shutdown_completion.isSet()) {
                return Error.SessionShutdown;
            }
            self.ping_mutex.lock();
            _ = self.pings.remove(ping_id);
            self.ping_mutex.unlock();
            return err;
        };

        if (self.shutdown_completion.isSet()) {
            return Error.SessionShutdown;
        }

        return @as(u64, @intCast(std.time.nanoTimestamp() - start));
    }

    fn keepalive(self: *Session) void {
        while (!self.shutdown_completion.isSet()) {
            std.time.sleep(self.config.keep_alive_interval);
            if (self.shutdown_completion.isSet()) {
                return;
            }
            self.ping() catch |err| {
                if (err != Error.SessionShutdown) {
                    std.log.warn("keepalive: ping error: {}\n", .{err});
                    self.setExitErr(Error.KeepAliveTimeout);
                }
                return;
            };
        }
    }

    fn recv(self: *Session) void {
        self.recvLoop() catch |err| {
            self.setExitErr(err);
        };
    }

    pub fn onRead(self: *Session, buffer: []u8, bytes_read: anyerror!usize) !void {
        const n = bytes_read catch |err| {
            if (err != xev.ReadError.EOF) {
                std.log.warn("onRead: read error: {}\n", .{err});
            }
            self.setExitErr(err);
            return err;
        };

        if (n == 0) {
            return;
        }

        self.inbound_buffer.write(buffer[0..n]) catch |err| {
            std.log.warn("onRead: write inbound buffer error: {}\n", .{err});
            self.setExitErr(err);
            return err;
        };

        while (true) {
            const available = self.inbound_buffer.readableSlice(0);
            if (available.len < frame.Header.SIZE) {
                return;
            }

            var temp_stream = std.io.fixedBufferStream(available);
            const reader = temp_stream.reader();

            var hdr_buf: [frame.Header.SIZE]u8 = undefined;
            reader.readNoEof(&hdr_buf) catch |err| {
                self.setExitErr(err);
                return err;
            };

            const header = frame.Header.decode(&hdr_buf) catch |err| {
                self.setExitErr(err);
                return err; // Propagate error
            };

            if (header.version != frame.PROTOCOL_VERSION) {
                self.setExitErr(Error.InvalidProtocolVersion);
                return Error.InvalidProtocolVersion;
            }

            const msg_type = header.frame_type;
            if (@intFromEnum(msg_type) < @intFromEnum(frame.FrameType.DATA) or @intFromEnum(msg_type) > @intFromEnum(frame.FrameType.GO_AWAY)) {
                self.setExitErr(Error.InvalidFrameType);
                return frame.Error.InvalidFrameType;
            }

            const frame_size = frame.Header.SIZE + header.length;
            if (available.len < frame_size) {
                return;
            }

            var body_slice: ?[]const u8 = null;
            if (header.length > 0) {
                const body_data = available[frame.Header.SIZE..frame_size];
                body_slice = body_data;
            }

            switch (header.frame_type) {
                .DATA, .WINDOW_UPDATE => try self.handleStreamMessage(header, body_slice),
                .PING => try self.handlePing(header),
                .GO_AWAY => try self.handleGoAway(header),
            }

            self.inbound_buffer.discard(frame_size);
        }
    }
    fn recvLoop(self: *Session) !void {
        defer self.recv_done.cond.broadcast();
        const hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
        while (true) {
            try self.buf_read.readNoEof(hdr);

            const header = try frame.Header.decode(hdr);
            if (header.version != frame.PROTOCOL_VERSION) {
                return frame.Error.InvalidProtocolVersion;
            }

            const msg_type = header.frame_type;
            if (msg_type < .DATA or msg_type > .GO_AWAY) {
                return frame.Error.InvalidFrameType;
            }

            switch (hdr.frame_type) {
                .DATA => self.handleStreamMessage(hdr),
                .PING => self.handlePing(hdr),
                .WINDOW_UPDATE => self.handleStreamMessage(hdr),
                .GO_AWAY => self.handleGoAway(hdr),
            }
        }
    }

    fn handleStreamMessage(self: *Session, hdr: frame.Header, _: ?[]const u8) Error!void {
        const stream_id = hdr.stream_id;
        const flags = hdr.flags;

        if (flags & frame.FrameFlags.SYN == frame.FrameFlags.SYN) {
            // Handle SYN
            try self.createInboundStream(stream_id);
        }

        self.stream_mutex.lock();
        var stream = self.streams.get(stream_id);
        self.stream_mutex.unlock();

        if (stream == null) {
            if (hdr.frame_type == frame.FrameType.DATA and hdr.length > 0) {
                std.log.warn("discarding data for stream {} with length {}\n", .{ stream_id, hdr.length });
                self.buf_read.skipBytes(hdr.length, .{}) catch |err| {
                    std.log.warn("error skipping bytes: {}\n", .{err});
                    return;
                };
            } else {
                std.log.warn("frame for missing stream {}\n", .{hdr});
            }

            return;
        }

        if (hdr.frame_type == .WINDOW_UPDATE) {
            stream.?.incrSendWindow(hdr, flags) catch |err| {
                std.log.warn("handleStreamMessage: incrSendWindow error: {}\n", .{err});
                const goaway_hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
                defer self.allocator.free(goaway_hdr);
                self.goAway(@intFromEnum(frame.GoAwayCode.PROTOCOL_ERROR), goaway_hdr) catch |goaway_err| {
                    std.log.warn("createInboundStream: goAway error: {}\n", .{goaway_err});
                };
                self.send(goaway_hdr, null) catch |send_err| {
                    std.log.warn("createInboundStream: send error: {}\n", .{send_err});
                };
                return err;
            };
        }

        stream.?.readData(hdr, flags, self.buf_read) catch |err| {
            std.log.warn("handleStreamMessage: readData error: {}\n", .{err});
            const goaway_hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
            defer self.allocator.free(goaway_hdr);
            self.goAway(@intFromEnum(frame.GoAwayCode.PROTOCOL_ERROR), goaway_hdr) catch |goaway_err| {
                std.log.warn("createInboundStream: goAway error: {}\n", .{goaway_err});
            };
            self.send(goaway_hdr, null) catch |send_err| {
                std.log.warn("createInboundStream: send error: {}\n", .{send_err});
            };
            return err;
        };
    }

    fn handlePing(self: *Session, hdr: frame.Header) Error!void {
        const flags = hdr.flags;
        const ping_id = hdr.length;

        if (flags & frame.FrameFlags.SYN == frame.FrameFlags.SYN) {
            const syn_hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
            defer self.allocator.free(syn_hdr);
            frame.Header.init(frame.FrameType.PING, frame.FrameFlags.ACK, 0, ping_id).encode(syn_hdr) catch |err| {
                std.log.warn("handlePing: encode error: {}\n", .{err});
            };
            self.send(syn_hdr, null) catch |err| {
                std.log.warn("handlePing: send error: {}\n", .{err});
            };
            return;
        }

        self.ping_mutex.lock();
        if (self.pings.getPtr(ping_id)) |ping_notification| {
            ping_notification.set();
            _ = self.pings.remove(ping_id);
        }
        self.ping_mutex.unlock();
    }

    fn handleGoAway(self: *Session, hdr: frame.Header) Error!void {
        const code = hdr.length;
        switch (@as(frame.GoAwayCode, @enumFromInt(code))) {
            frame.GoAwayCode.NORMAL => {
                _ = self.remote_go_away.swap(1, .acq_rel);
                return;
            },
            frame.GoAwayCode.PROTOCOL_ERROR => return Error.GoAwayProtocolError,
            frame.GoAwayCode.INTERNAL_ERROR => return Error.GoAwayInternalError,
        }
    }

    fn createInboundStream(self: *Session, id: u32) Error!void {
        // Reject immediately if we are doing a go away
        if (self.local_go_away.load(.acquire) == 1) {
            const hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
            defer self.allocator.free(hdr);
            try frame.Header.init(frame.FrameType.WINDOW_UPDATE, frame.FrameFlags.RST, id, 0).encode(hdr);
            return self.send(hdr, null);
        }

        var stream: Stream = undefined;
        try Stream.init(&stream, self, id, .syn_received, self.allocator);
        errdefer stream.deinit();

        self.stream_mutex.lock();
        defer self.stream_mutex.unlock();

        // Check if the stream is already established
        if (self.streams.contains(id)) {
            std.log.warn("createInboundStream: stream already exists\n", .{});
            const hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
            defer self.allocator.free(hdr);
            self.goAway(@intFromEnum(frame.GoAwayCode.PROTOCOL_ERROR), hdr) catch |err| {
                std.log.warn("createInboundStream: goAway error: {}\n", .{err});
            };
            self.send(hdr, null) catch |err| {
                std.log.warn("createInboundStream: send error: {}\n", .{err});
            };
            return Error.DuplicateStream;
        }

        try self.streams.put(id, stream);

        if (self.accept_queue.push(self.streams.getPtr(id).?, .{ .instant = {} }) == 0) {
            std.log.warn("ccreateInboundStream: accept_queue push failed since queue is full\n", .{});
            _ = self.streams.remove(id);
            const hdr = try self.allocator.alloc(u8, frame.Header.SIZE);
            defer self.allocator.free(hdr);
            frame.Header.init(frame.FrameType.WINDOW_UPDATE, frame.FrameFlags.RST, id, 0).encode(hdr) catch |err| {
                std.log.warn("createInboundStream: encode error: {}\n", .{err});
            };
            self.send(hdr, null) catch |err| {
                std.log.warn("createInboundStream: send error: {}\n", .{err});
            };
        }
    }

    pub fn closeStream(self: *Session, id: u32) void {
        self.stream_mutex.lock();
        if (self.inflight.contains(id)) {
            self.syn_semaphore.post();
            _ = self.inflight.remove(id);
        }
        _ = self.streams.remove(id);
        self.stream_mutex.unlock();
    }

    pub fn establishStream(self: *Session, id: u32) void {
        self.stream_mutex.lock();
        if (self.inflight.contains(id)) {
            _ = self.inflight.remove(id);
        } else {
            std.log.warn("establishStream: stream not found in inflight\n", .{});
        }
        self.syn_semaphore.post();
        self.stream_mutex.unlock();
    }

    fn getNextStreamId(self: *Session) !u32 {
        while (!self.shutdown_completion.isSet()) {
            const id = self.next_stream_id.load(.acquire);
            if (id >= std.math.maxInt(u32) - 1) {
                return Error.StreamsExhausted;
            }

            if (self.next_stream_id.cmpxchgWeak(id, id + 2, .acq_rel, .acquire) == id) {
                return id;
            }
        }

        return Error.SessionShutdown;
    }

    fn goAway(self: *Session, reason: u32, hdr: []u8) Error!void {
        _ = self.local_go_away.swap(1, .acq_rel);
        try frame.Header.init(frame.FrameType.GO_AWAY, 0, 0, reason).encode(hdr);
    }

    fn setExitErr(self: *Session, err: anyerror) void {
        self.shutdown_err_mutex.lock();
        if (self.shutdown_err == null) {
            self.shutdown_err = err;
        }
        self.shutdown_err_mutex.unlock();
        self.close();
    }

    fn waitEnqueue(self: *Session, send_ready: *SendReady, timeout_ns: u64) Error!void {
        self.send_queue_sync.mutex.lock();
        while (self.send_queue_size >= self.send_queue_capacity) {
            self.send_queue_sync.not_full_cond.timedWait(&self.send_queue_sync.mutex, timeout_ns) catch {
                self.send_queue_sync.mutex.unlock();
                if (send_ready.sent_completion) |sent_completion| {
                    self.allocator.destroy(sent_completion);
                }
                send_ready.deinit();
                self.allocator.destroy(send_ready);
                if (self.shutdown_completion.isSet()) {
                    return Error.SessionShutdown;
                }
                return Error.ConnectionWriteTimeout;
            };
            if (self.shutdown_completion.isSet()) {
                self.send_queue_sync.mutex.unlock();
                if (send_ready.sent_completion) |sent_completion| {
                    self.allocator.destroy(sent_completion);
                }
                send_ready.deinit();
                self.allocator.destroy(send_ready);
                return Error.SessionShutdown;
            }
        }
        if (self.shutdown_completion.isSet()) {
            self.send_queue_sync.mutex.unlock();
            if (send_ready.sent_completion) |sent_completion| {
                self.allocator.destroy(sent_completion);
            }
            send_ready.deinit();
            self.allocator.destroy(send_ready);
            return Error.SessionShutdown;
        }

        const node = try self.allocator.create(SendQueue.Node);
        node.* = .{ .data = send_ready };
        self.send_queue.prepend(node);
        self.send_queue_size += 1;
        self.send_queue_sync.not_empty_cond.signal();
        self.send_queue_sync.mutex.unlock();
    }

    fn waitSent(self: *Session, sent_completion: *SentCompletion, timeout_ns: u64) !void {
        sent_completion.done.timedWait(timeout_ns) catch {
            // If the wait times out, we will not destroy the sent_completion,
            // as it may be used by the send loop.
            sent_completion.done.set();

            if (self.shutdown_completion.isSet()) {
                return Error.SessionShutdown;
            }
            return Error.ConnectionWriteTimeout;
        };

        if (self.shutdown_completion.isSet()) {
            self.allocator.destroy(sent_completion);
            return Error.SessionShutdown;
        }

        if (sent_completion.getErr()) |err| {
            self.allocator.destroy(sent_completion);
            return err;
        }
        self.allocator.destroy(sent_completion);
    }
};

test "Session.send using PipeConn" {
    var pipes = try conn.createPipeConnPair();
    defer {
        pipes.client.close();
        pipes.server.close();
    }

    const client_conn = pipes.client.conn().any();

    var config = Config.defaultConfig();

    var session: Session = undefined;
    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = testing.allocator });
    defer pool.deinit();

    try Session.initAndStart(testing.allocator, &config, client_conn, &session, &pool);
    defer session.deinit();

    const hdr = "test header";
    const bd = "test body content";
    for (0..3) |_| {
        const header = try testing.allocator.dupe(u8, hdr);
        defer testing.allocator.free(header);

        const body = try testing.allocator.dupe(u8, bd);
        defer testing.allocator.free(body);

        // Send the data
        try session.sendAndWait(header, body);

        // Give time for sending to complete
        std.time.sleep(10 * std.time.ns_per_ms);
    }

    var buffer: [256]u8 = undefined;
    const bytes_read = try pipes.server.read(&buffer);

    try testing.expect(bytes_read == 84);

    const received_data = buffer[0..bytes_read];
    try testing.expectEqualSlices(u8, hdr, received_data[0..hdr.len]);
    try testing.expectEqualSlices(u8, bd, received_data[hdr.len .. hdr.len + 17]);

    session.close();
}

test "Session.send using PipeConn timeout" {
    var pipes = try conn.createPipeConnPair();
    defer {
        pipes.client.close();
        pipes.server.close();
    }

    const client_conn = pipes.client.conn().any();

    var config = Config.defaultConfig();
    config.connection_write_timeout = 1000 * std.time.ns_per_ms; // 1 second

    var session: Session = undefined;
    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = testing.allocator });
    defer pool.deinit();

    try Session.init(testing.allocator, &config, client_conn, &session, &pool);
    defer session.deinit();

    // Set the send queue capacity to 1 and make sendAndWait and send both timeout
    session.send_queue_sync.mutex.lock();
    session.send_queue_capacity = 1;
    session.send_queue_sync.mutex.unlock();

    const header = try testing.allocator.dupe(u8, "test header");
    defer testing.allocator.free(header);

    const body = try testing.allocator.dupe(u8, "test body content");
    defer testing.allocator.free(body);

    const res = session.sendAndWait(header, body);
    try testing.expectError(error.ConnectionWriteTimeout, res);

    const res1 = session.send(header, body);
    try testing.expectError(error.ConnectionWriteTimeout, res1);

    session.close();
}

test "Session.send after shutdown" {
    var pipes = try conn.createPipeConnPair();
    defer {
        pipes.client.close();
        pipes.server.close();
    }

    const client_conn = pipes.client.conn().any();

    var config = Config.defaultConfig();

    var session: Session = undefined;
    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = testing.allocator });
    defer pool.deinit();

    try Session.initAndStart(testing.allocator, &config, client_conn, &session, &pool);
    defer session.deinit();

    const header = try testing.allocator.dupe(u8, "test header");
    defer testing.allocator.free(header);

    const body = try testing.allocator.dupe(u8, "test body content");
    defer testing.allocator.free(body);

    session.close();

    // Give some time for shutdown to propagate
    std.time.sleep(10 * std.time.ns_per_ms);

    const send_result = session.sendAndWait(header, body);
    try testing.expectError(error.SessionShutdown, send_result);

    const send_result2 = session.send(header, body);
    try testing.expectError(error.SessionShutdown, send_result2);
}

test "Session shutdown during active sendAndWait operations" {
    var pipes = try conn.createPipeConnPair();
    defer {
        pipes.client.close();
        pipes.server.close();
    }

    const client_conn = pipes.client.conn().any();
    var config = Config.defaultConfig();

    var session: Session = undefined;
    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = testing.allocator });
    defer pool.deinit();

    try Session.initAndStart(testing.allocator, &config, client_conn, &session, &pool);
    defer session.deinit();

    var shutdown_error_detected = std.atomic.Value(bool).init(false);
    var should_exit = std.atomic.Value(bool).init(false);
    var sender_thread: std.Thread = undefined;

    // Start the sender thread that continuously calls sendAndWait
    sender_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Session, detected: *std.atomic.Value(bool), exit: *std.atomic.Value(bool)) !void {
            var i: usize = 0;
            while (!exit.load(.acquire)) {
                const header = try testing.allocator.alloc(u8, 32);
                defer testing.allocator.free(header);
                @memset(header, 'h');

                const body = try testing.allocator.alloc(u8, 64);
                defer testing.allocator.free(body);
                @memset(body, 'b');

                s.sendAndWait(header, body) catch |err| {
                    if (err == error.SessionShutdown) {
                        detected.store(true, .release);
                        return;
                    }
                    // Ignore timeout errors that might occur from pipe filling up
                    if (err != error.ConnectionWriteTimeout) {
                        std.debug.print("Unexpected error: {}\n", .{err});
                    }
                };

                i += 1;
                if (i % 10 == 0) {
                    // Give other threads a chance to run
                    std.time.sleep(1 * std.time.ns_per_ms);
                }
            }
        }
    }.run, .{ &session, &shutdown_error_detected, &should_exit });

    // Wait a bit to allow the sender to get into a rhythm
    std.time.sleep(50 * std.time.ns_per_ms);

    session.close();

    // Give the sender thread time to detect the shutdown and exit
    var timeout: usize = 0;
    while (!shutdown_error_detected.load(.acquire) and timeout < 100) {
        std.time.sleep(10 * std.time.ns_per_ms);
        timeout += 1;
    }

    should_exit.store(true, .release);
    sender_thread.join();

    try testing.expect(shutdown_error_detected.load(.acquire));
}

test "Session shutdown during active send operations" {
    var pipes = try conn.createPipeConnPair();
    defer {
        pipes.client.close();
        pipes.server.close();
    }

    const client_conn = pipes.client.conn().any();
    var config = Config.defaultConfig();

    var session: Session = undefined;
    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = testing.allocator });
    defer pool.deinit();

    try Session.initAndStart(testing.allocator, &config, client_conn, &session, &pool);
    defer session.deinit();

    // Create shared state between threads
    var shutdown_error_detected = std.atomic.Value(bool).init(false);
    var should_exit = std.atomic.Value(bool).init(false);
    var sender_thread: std.Thread = undefined;

    // Start the sender thread that continuously calls sendAndWait
    sender_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Session, detected: *std.atomic.Value(bool), exit: *std.atomic.Value(bool)) !void {
            var i: usize = 0;
            while (!exit.load(.acquire)) {
                const header = try testing.allocator.alloc(u8, 32);
                defer testing.allocator.free(header);
                @memset(header, 'h');

                const body = try testing.allocator.alloc(u8, 64);
                defer testing.allocator.free(body);
                @memset(body, 'b');

                s.send(header, body) catch |err| {
                    if (err == error.SessionShutdown) {
                        detected.store(true, .release);
                        return;
                    }
                    // Ignore timeout errors that might occur from pipe filling up
                    if (err != error.ConnectionWriteTimeout) {
                        std.debug.print("Unexpected error: {}\n", .{err});
                    }
                };

                i += 1;
                if (i % 10 == 0) {
                    // Give other threads a chance to run
                    std.time.sleep(1 * std.time.ns_per_ms);
                }
            }
        }
    }.run, .{ &session, &shutdown_error_detected, &should_exit });

    // Wait a bit to allow the sender to get into a rhythm
    std.time.sleep(50 * std.time.ns_per_ms);

    session.close();

    // Give the sender thread time to detect the shutdown and exit
    var timeout: usize = 0;
    while (!shutdown_error_detected.load(.acquire) and timeout < 100) {
        std.time.sleep(10 * std.time.ns_per_ms);
        timeout += 1;
    }

    should_exit.store(true, .release);
    sender_thread.join();

    try testing.expect(shutdown_error_detected.load(.acquire));
}
