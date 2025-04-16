const std = @import("std");
const Allocator = std.mem.Allocator;
const LinearFifo = std.fifo.LinearFifo;
const frame = @import("frame.zig");
const session = @import("session.zig");
const Config = @import("Config.zig");
const xev = @import("xev");

pub const StreamState = enum {
    init,
    syn_sent,
    syn_received,
    established,
    local_close,
    remote_close,
    closed,
    reset,
};

pub const Error = error{
    StreamClosed,
    StreamReset,
    StreamFlag,
    WriteTimeout,
    ReadTimeout,
    SessionShutdown,
};

pub const Stream = struct {
    recv_window: u32,
    send_window: std.atomic.Value(u32),

    id: u32,
    session: *session.Session,

    state: StreamState,
    state_mutex: std.Thread.Mutex,

    recv_buf: ?*LinearFifo(u8, .Dynamic),
    recv_mutex: std.Thread.Mutex,

    control_hdr: []u8,
    control_err: ?Error,
    control_mutex: std.Thread.Mutex,

    send_hdr: []u8,
    send_err: ?Error,
    send_mutex: std.Thread.Mutex,

    // Notification channels implemented with condition variables
    recv_completion: std.Thread.ResetEvent = .{},

    send_completion: std.Thread.ResetEvent = .{},

    read_deadline: std.atomic.Value(i64),
    write_deadline: std.atomic.Value(i64),

    // For establishment notification
    establish_completion: std.Thread.ResetEvent = .{},

    // Set with state_mutex held to honor the StreamCloseTimeout
    close_timer: ?*xev.Timer = null,

    c_close_timer: ?*xev.Completion = null,

    allocator: std.mem.Allocator,

    pub fn init(stream: *Stream, s: *session.Session, id: u32, state: StreamState, alloc: std.mem.Allocator) !void {
        const control_hdr = try alloc.alloc(u8, frame.Header.SIZE);
        @memset(control_hdr, 0);

        const send_hdr = try alloc.alloc(u8, frame.Header.SIZE);
        @memset(send_hdr, 0);

        stream.* = .{
            .id = id,
            .session = s,
            .state = state,
            .state_mutex = .{},
            .recv_buf = null,
            .recv_mutex = .{},
            .control_hdr = control_hdr,
            .control_err = null,
            .control_mutex = .{},
            .send_hdr = send_hdr,
            .send_err = null,
            .send_mutex = .{},
            .recv_window = Config.initial_stream_window,
            .send_window = std.atomic.Value(u32).init(Config.initial_stream_window),
            .read_deadline = std.atomic.Value(i64).init(0),
            .write_deadline = std.atomic.Value(i64).init(0),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *Stream) void {
        self.allocator.free(self.control_hdr);
        self.allocator.free(self.send_hdr);

        if (self.recv_buf) |buf| {
            buf.deinit();
            self.allocator.destroy(buf);
        }
        if (self.close_timer) |timer| {
            timer.deinit();
            self.allocator.destroy(timer);
        }
        if (self.c_close_timer) |c| {
            self.allocator.destroy(c);
        }
    }

    /// Reads data from the stream into the provided buffer
    /// Returns the number of bytes read or an error
    pub fn read(self: *Stream, buf: []u8) !usize {
        // Notify receivers when done
        defer {
            self.recv_completion.set();
        }

        while (true) {
            // Check if the stream is closed and there's no data buffered
            self.state_mutex.lock();
            switch (self.state) {
                .local_close => {
                    // LocalClose only prohibits further local writes
                    self.state_mutex.unlock();
                },
                .remote_close, .closed => {
                    self.recv_mutex.lock();
                    const is_empty = (self.recv_buf == null or
                        self.recv_buf.?.readableLength() == 0);
                    self.recv_mutex.unlock();

                    if (is_empty) {
                        self.state_mutex.unlock();
                        return error.EndOfStream;
                    }
                    self.state_mutex.unlock();
                },
                .reset => {
                    self.state_mutex.unlock();
                    return Error.StreamReset;
                },
                else => self.state_mutex.unlock(),
            }

            // Check if there is data available
            self.recv_mutex.lock();
            if (self.recv_buf == null or self.recv_buf.?.readableLength() == 0) {
                self.recv_mutex.unlock();

                const deadline = self.read_deadline.load(.acquire);
                const has_deadline = deadline != 0;

                if (has_deadline) {
                    const now = std.time.timestamp();
                    if (now >= deadline) {
                        return Error.ReadTimeout;
                    }

                    // Calculate timeout in seconds
                    const timeout_ns = deadline - now;

                    // Wait with timeout
                    try self.recv_completion.timedWait(@intCast(timeout_ns));
                } else {
                    // Wait without timeout
                    self.recv_completion.wait();
                }

                // Continue to start of loop
                continue;
            }

            // Read any bytes
            const n = self.recv_buf.?.read(buf);
            self.recv_mutex.unlock();

            // Send a window update potentially
            self.sendWindowUpdate() catch |err| {
                // Ignore SessionShutdown errors
                if (err != Error.SessionShutdown) {
                    return err;
                }
            };

            return n;
        }
    }

    /// sendWindowUpdate potentially sends a window update enabling
    /// further writes to take place. Must be invoked with the lock.
    pub fn sendWindowUpdate(self: *Stream) !void {
        self.control_mutex.lock();
        defer self.control_mutex.unlock();

        // Determine the delta update
        const max = self.session.config.max_stream_window_size;
        var buf_len: u32 = 0;

        self.recv_mutex.lock();
        if (self.recv_buf) |buf| {
            buf_len = @intCast(buf.readableLength());
        }
        const delta = (max - buf_len) - self.recv_window;

        // Determine the flags if any
        const flags = self.sendFlags();

        // Check if we can omit the update
        if (delta < (max / 2) and flags == 0) {
            self.recv_mutex.unlock();
            return;
        }

        // Update our window
        self.recv_window += delta;
        self.recv_mutex.unlock();

        // Send the header
        const header = frame.Header.init(.WINDOW_UPDATE, flags, self.id, delta);
        try header.encode(self.control_hdr);

        self.session.sendAndWait(self.control_hdr, null) catch |err| {
            if (err == Error.SessionShutdown or err == Error.WriteTimeout) {
                // Message left in ready queue, header re-use is unsafe.
                // Need to allocate a new header
                var new_hdr = [_]u8{0} ** frame.Header.SIZE;

                const old_hdr = self.control_hdr;
                self.control_hdr = &new_hdr;
                self.session.allocator.free(old_hdr);
            }
            return err;
        };

        return;
    }

    pub fn forceClose(self: *Stream) void {
        self.state_mutex.lock();
        self.state = .closed;
        self.state_mutex.unlock();
        self.notifyWaiters();
    }

    pub fn notifyWaiters(self: *Stream) void {
        self.recv_completion.set();
        self.send_completion.set();
        self.establish_completion.set();
    }

    pub fn closeTimeout(self: *Stream) void {
        self.forceClose();

        self.session.closeStream(self.id);

        self.send_mutex.lock();
        defer self.send_mutex.unlock();
        const hdr = self.allocator.alloc(u8, frame.Header.SIZE) catch unreachable;
        defer self.allocator.free(hdr);
        frame.Header.init(frame.FrameType.WINDOW_UPDATE, frame.FrameFlags.RST, self.id, 0).encode(hdr) catch unreachable;
        self.session.send(hdr, null) catch |err| {
            std.debug.print("Error sending close stream message: {}\n", .{err});
        };
    }

    pub fn incrSendWindow(_: *Stream, _: frame.Header, _: u16) Error!void {}

    pub fn readData(_: *Stream, _: frame.Header, _: u16, _: anytype) Error!void {}

    /// Determines any flags that are appropriate based on the current stream state.
    /// Must be called with state_mutex held.
    fn sendFlags(self: *Stream) u16 {
        self.state_mutex.lock();
        defer self.state_mutex.unlock();
        var flags: u16 = 0;

        switch (self.state) {
            .init => {
                flags |= frame.FrameFlags.SYN;
                self.state = .syn_sent;
            },
            .syn_received => {
                flags |= frame.FrameFlags.ACK;
                self.state = .established;
            },
            else => {},
        }

        return flags;
    }

    // fn processFlags(self: *Stream, flags: u16) void {
    //     self.state_mutex.lock();
    //     defer self.state_mutex.unlock();
    //
    //     var close_stream = false;
    //     defer {
    //         if(close_stream) {
    //             if(self.close_timer) |timer| {
    //                 timer.deinit();
    //                 self.session.allocator.destroy(timer);
    //             }
    //             self.state = .closed;
    //         }
    //     }
    //     if (flags & frame.FrameFlags.SYN) |syn| {
    //         if (syn) |syn| {
    //             self.state = .syn_received;
    //         }
    //     }
    //
    //     if (flags & frame.FrameFlags.ACK) |ack| {
    //         if (ack) |ack| {
    //             self.state = .established;
    //         }
    //     }
    //
    //     if (flags & frame.FrameFlags.FIN) |fin| {
    //         if (fin) |fin| {
    //             self.state = .remote_close;
    //         }
    //     }
    // }
};
