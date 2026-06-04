const std = @import("std");
const channel = @import("../../quic/io/channel.zig");
const io_time = @import("../../quic/io/time.zig");

/// The lane an outbound frame travels on, in decreasing priority.
/// subscribe is highest (subscriptions are rare and critical),
/// control is medium (GRAFT/PRUNE/IHAVE/IWANT/IDONTWANT),
/// data is lowest (published/forwarded application messages).
pub const Lane = enum { subscribe, control, data };

/// An owned, pre-framed RPC ready to write to a QUIC stream. The queue that
/// holds this value owns it and calls deinit on pop or deinit. The caller owns
/// it after popping and must call deinit when done.
///
/// bytes: length-prefixed wire bytes produced by pubsub.frameRpc; owned.
/// message_ids: owned ids carried for IDONTWANT purge; empty (&.{}) for all
///   lanes except data frames that may later be IDONTWANT-purged.
pub const OutboundFrame = struct {
    bytes: []u8,
    message_ids: [][]u8,

    pub fn deinit(self: *OutboundFrame, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        for (self.message_ids) |id| allocator.free(id);
        allocator.free(self.message_ids);
        self.* = undefined;
    }
};

pub const Options = struct {
    control_cap: usize = 4096,
    data_cap: usize = 1024,
};

/// Per-peer outbound queue, decoupled from the QUIC stream. A router fiber
/// pushes frames; a writer fiber (started in a later phase) pops and writes
/// them. Thread/fiber-safe: a std.Io.Mutex guards the lane FIFOs and a Signal
/// wakes a blocked popper with no lost-wakeup: the epoch is observed under the
/// lock before unlocking, so a notify that races after unlock advances the epoch
/// and the subsequent wait() returns immediately.
///
/// Lane storage: std.ArrayList(OutboundFrame) with a head cursor forming a
/// simple bounded FIFO. O(1) pop (head++) at the cost of dead prefix slots,
/// reclaimed lazily when the dead prefix is >= the live count (amortised O(1)).
/// This avoids per-item heap allocation without requiring a circular buffer.
pub const OutboundQueue = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex,
    signal: channel.Signal,
    /// subscribe lane: unbounded FIFO.
    sub_items: std.ArrayList(OutboundFrame),
    sub_head: usize,
    /// control lane: bounded by control_cap.
    ctrl_items: std.ArrayList(OutboundFrame),
    ctrl_head: usize,
    control_cap: usize,
    /// data lane: bounded by data_cap.
    data_items: std.ArrayList(OutboundFrame),
    data_head: usize,
    data_cap: usize,
    closed: bool,

    pub fn init(allocator: std.mem.Allocator, opts: Options) OutboundQueue {
        return .{
            .allocator = allocator,
            .mutex = .init,
            .signal = .{},
            .sub_items = .empty,
            .sub_head = 0,
            .ctrl_items = .empty,
            .ctrl_head = 0,
            .control_cap = opts.control_cap,
            .data_items = .empty,
            .data_head = 0,
            .data_cap = opts.data_cap,
            .closed = false,
        };
    }

    /// Free all remaining frames and release all lane storage.
    pub fn deinit(self: *OutboundQueue, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        self.drainLane(&self.sub_items, &self.sub_head);
        self.drainLane(&self.ctrl_items, &self.ctrl_head);
        self.drainLane(&self.data_items, &self.data_head);
        self.mutex.unlock(io);
        self.sub_items.deinit(self.allocator);
        self.ctrl_items.deinit(self.allocator);
        self.data_items.deinit(self.allocator);
    }

    pub const PushError = error{ LaneFull, Closed };

    /// Enqueue an owned frame on the given lane. On LaneFull or Closed the
    /// caller still owns `frame` and must deinit it. On success the queue
    /// takes ownership and wakes any blocked popper.
    pub fn push(self: *OutboundQueue, io: std.Io, lane: Lane, frame: OutboundFrame) PushError!void {
        self.mutex.lockUncancelable(io);
        if (self.closed) {
            self.mutex.unlock(io);
            return error.Closed;
        }
        switch (lane) {
            .subscribe => {
                self.sub_items.append(self.allocator, frame) catch {
                    self.mutex.unlock(io);
                    return error.LaneFull; // treat OOM as full
                };
            },
            .control => {
                if (self.ctrlLen() >= self.control_cap) {
                    self.mutex.unlock(io);
                    return error.LaneFull;
                }
                self.ctrl_items.append(self.allocator, frame) catch {
                    self.mutex.unlock(io);
                    return error.LaneFull;
                };
            },
            .data => {
                if (self.dataLen() >= self.data_cap) {
                    self.mutex.unlock(io);
                    return error.LaneFull;
                }
                self.data_items.append(self.allocator, frame) catch {
                    self.mutex.unlock(io);
                    return error.LaneFull;
                };
            },
        }
        self.mutex.unlock(io);
        self.signal.notify(io);
    }

    /// Block until a frame is available (priority: subscribe > control > data)
    /// or the queue is closed and empty. Transfers ownership to the caller.
    /// Returns error.Closed when the queue is closed and all lanes are drained.
    /// Returns error.Canceled if the fiber is cancelled while waiting.
    pub fn popBlocking(self: *OutboundQueue, io: std.Io) error{ Closed, Canceled }!OutboundFrame {
        while (true) {
            self.mutex.lockUncancelable(io);
            if (self.tryPopLocked()) |frame| {
                self.mutex.unlock(io);
                return frame;
            }
            if (self.closed) {
                self.mutex.unlock(io);
                return error.Closed;
            }
            // Observe the epoch *under* the lock so a notify that races after
            // unlock advances the epoch, making wait() return immediately and
            // eliminating the lost-wakeup window.
            const ep = self.signal.observe();
            self.mutex.unlock(io);
            self.signal.wait(io, ep) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
            };
        }
    }

    /// Remove and free every data-lane frame for which pred(ctx, frame) returns
    /// true. Returns the count of removed frames. Other lanes are untouched.
    pub fn removeData(
        self: *OutboundQueue,
        io: std.Io,
        ctx: anytype,
        comptime pred: fn (@TypeOf(ctx), *const OutboundFrame) bool,
    ) usize {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var removed: usize = 0;
        var i = self.data_head;
        while (i < self.data_items.items.len) {
            if (pred(ctx, &self.data_items.items[i])) {
                var frame = self.data_items.items[i];
                // Shift remaining items left to fill the gap.
                var j = i;
                while (j + 1 < self.data_items.items.len) : (j += 1) {
                    self.data_items.items[j] = self.data_items.items[j + 1];
                }
                self.data_items.shrinkRetainingCapacity(self.data_items.items.len - 1);
                frame.deinit(self.allocator);
                removed += 1;
                // Do NOT advance i: the item at position i is now the next one.
            } else {
                i += 1;
            }
        }
        return removed;
    }

    /// Close the queue and wake all blocked poppers. Poppers drain remaining
    /// frames and then return error.Closed.
    pub fn close(self: *OutboundQueue, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        self.closed = true;
        self.mutex.unlock(io);
        self.signal.notify(io);
    }

    // --- internal helpers (called under mutex) ---

    /// Return the number of un-popped items in the control lane.
    fn ctrlLen(self: *const OutboundQueue) usize {
        return self.ctrl_items.items.len - self.ctrl_head;
    }

    /// Return the number of un-popped items in the data lane.
    fn dataLen(self: *const OutboundQueue) usize {
        return self.data_items.items.len - self.data_head;
    }

    /// Try to pop a frame by priority (subscribe > control > data).
    /// Caller must hold mutex. Returns null if all lanes are empty.
    fn tryPopLocked(self: *OutboundQueue) ?OutboundFrame {
        if (self.sub_head < self.sub_items.items.len) {
            const frame = self.sub_items.items[self.sub_head];
            self.sub_head += 1;
            compactLane(&self.sub_items, &self.sub_head);
            return frame;
        }
        if (self.ctrl_head < self.ctrl_items.items.len) {
            const frame = self.ctrl_items.items[self.ctrl_head];
            self.ctrl_head += 1;
            compactLane(&self.ctrl_items, &self.ctrl_head);
            return frame;
        }
        if (self.data_head < self.data_items.items.len) {
            const frame = self.data_items.items[self.data_head];
            self.data_head += 1;
            compactLane(&self.data_items, &self.data_head);
            return frame;
        }
        return null;
    }

    /// Free all frames in a lane and reset its head cursor.
    fn drainLane(self: *OutboundQueue, items: *std.ArrayList(OutboundFrame), head: *usize) void {
        for (items.items[head.*..]) |*frame| {
            frame.deinit(self.allocator);
        }
        items.clearRetainingCapacity();
        head.* = 0;
    }

};

/// Compact a lane's backing array when the dead prefix is >= the live count.
/// Shifts live items to the front and resets the head cursor. Called after a
/// pop; at most one compaction per N pops (amortised O(1)). No allocator
/// needed — the backing storage is retained, not reallocated.
fn compactLane(items: *std.ArrayList(OutboundFrame), head: *usize) void {
    if (head.* == 0) return;
    const live = items.items.len - head.*;
    if (head.* < live) return;
    std.mem.copyForwards(OutboundFrame, items.items[0..live], items.items[head.*..]);
    items.shrinkRetainingCapacity(live);
    head.* = 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Build a minimal owned frame for testing: 1 byte, no message_ids.
fn testFrame(allocator: std.mem.Allocator, byte: u8) !OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    bytes[0] = byte;
    const ids = try allocator.alloc([]u8, 0);
    return OutboundFrame{ .bytes = bytes, .message_ids = ids };
}

test "OutboundFrame deinit frees owned slices" {
    const allocator = std.testing.allocator;
    var frame = try testFrame(allocator, 0x42);
    frame.deinit(allocator);
    // testing.allocator detects leaks at test end.
}

test "priority order: subscribe before control before data" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    const data = try testFrame(allocator, 'd');
    const ctrl = try testFrame(allocator, 'c');
    const sub = try testFrame(allocator, 's');

    try q.push(io, .data, data);
    try q.push(io, .control, ctrl);
    try q.push(io, .subscribe, sub);

    var f1 = try q.popBlocking(io);
    defer f1.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 's'), f1.bytes[0]);

    var f2 = try q.popBlocking(io);
    defer f2.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 'c'), f2.bytes[0]);

    var f3 = try q.popBlocking(io);
    defer f3.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 'd'), f3.bytes[0]);
}

test "FIFO within a lane" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    const fa = try testFrame(allocator, 1);
    const fb = try testFrame(allocator, 2);
    const fc = try testFrame(allocator, 3);
    try q.push(io, .data, fa);
    try q.push(io, .data, fb);
    try q.push(io, .data, fc);

    var r1 = try q.popBlocking(io);
    defer r1.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 1), r1.bytes[0]);

    var r2 = try q.popBlocking(io);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 2), r2.bytes[0]);

    var r3 = try q.popBlocking(io);
    defer r3.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 3), r3.bytes[0]);
}

test "control_cap and data_cap enforced" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{ .control_cap = 2, .data_cap = 1 });
    defer q.deinit(io);

    const c1 = try testFrame(allocator, 1);
    const c2 = try testFrame(allocator, 2);
    var c3 = try testFrame(allocator, 3); // rejected; caller must free

    try q.push(io, .control, c1);
    try q.push(io, .control, c2);
    try std.testing.expectError(error.LaneFull, q.push(io, .control, c3));
    c3.deinit(allocator); // still owned by caller after rejection

    const d1 = try testFrame(allocator, 10);
    var d2 = try testFrame(allocator, 11); // rejected

    try q.push(io, .data, d1);
    try std.testing.expectError(error.LaneFull, q.push(io, .data, d2));
    d2.deinit(allocator);

    // Drain accepted items so the queue deinit has nothing left.
    var rc1 = try q.popBlocking(io);
    rc1.deinit(allocator);
    var rc2 = try q.popBlocking(io);
    rc2.deinit(allocator);
    var rd1 = try q.popBlocking(io);
    rd1.deinit(allocator);
}

test "removeData removes matching data frames and leaves others" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    const d1 = try testFrame(allocator, 1);
    const d2 = try testFrame(allocator, 2);
    const d3 = try testFrame(allocator, 3);
    try q.push(io, .data, d1);
    try q.push(io, .data, d2);
    try q.push(io, .data, d3);

    const ctrl = try testFrame(allocator, 99);
    try q.push(io, .control, ctrl);

    // Remove data frames whose byte value is even (d2 only).
    const removed = q.removeData(io, @as(u8, 0), struct {
        fn pred(_: u8, frame: *const OutboundFrame) bool {
            return frame.bytes[0] % 2 == 0;
        }
    }.pred);
    try std.testing.expectEqual(@as(usize, 1), removed);

    // control pops first (priority), then d1 and d3; d2 is gone.
    var r_ctrl = try q.popBlocking(io);
    defer r_ctrl.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 99), r_ctrl.bytes[0]);

    var r1 = try q.popBlocking(io);
    defer r1.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 1), r1.bytes[0]);

    var r3 = try q.popBlocking(io);
    defer r3.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 3), r3.bytes[0]);
}

test "removeData with non-zero head cursor respects live range and FIFO order" {
    // Verifies that removeData correctly scans only the un-popped (live) slice
    // when data_head > 0, does not touch already-consumed slots, and that the
    // survivors arrive in FIFO order on subsequent pops. Also confirms that
    // std.testing.allocator reports no leaks (no double-free or dead-slot read).
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    // Push four data frames: bytes 10, 20, 30, 40.
    try q.push(io, .data, try testFrame(allocator, 10));
    try q.push(io, .data, try testFrame(allocator, 20));
    try q.push(io, .data, try testFrame(allocator, 30));
    try q.push(io, .data, try testFrame(allocator, 40));

    // Pop one frame (byte 10) — data_head advances to 1.
    var first = try q.popBlocking(io);
    try std.testing.expectEqual(@as(u8, 10), first.bytes[0]);
    first.deinit(allocator);

    // Remove frames whose byte equals 30 (one match, index 2 in the backing
    // array but index 1 relative to data_head). The frame at slot 0 (byte 10)
    // was already consumed and must not be touched.
    const removed = q.removeData(io, @as(u8, 30), struct {
        fn pred(target: u8, frame: *const OutboundFrame) bool {
            return frame.bytes[0] == target;
        }
    }.pred);
    try std.testing.expectEqual(@as(usize, 1), removed);

    // Remaining live frames in FIFO order: 20, 40.
    var r20 = try q.popBlocking(io);
    defer r20.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 20), r20.bytes[0]);

    var r40 = try q.popBlocking(io);
    defer r40.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 40), r40.bytes[0]);
}

test "close on empty queue returns Closed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    q.close(io);
    try std.testing.expectError(error.Closed, q.popBlocking(io));
}

test "close with frames queued drains before returning Closed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    const f1 = try testFrame(allocator, 1);
    const f2 = try testFrame(allocator, 2);
    try q.push(io, .data, f1);
    try q.push(io, .subscribe, f2);
    q.close(io);

    // Pop both frames before getting error.Closed.
    var r1 = try q.popBlocking(io);
    defer r1.deinit(allocator);
    var r2 = try q.popBlocking(io);
    defer r2.deinit(allocator);
    try std.testing.expectError(error.Closed, q.popBlocking(io));
}

test "deinit with frames in all lanes frees everything" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});

    try q.push(io, .subscribe, try testFrame(allocator, 1));
    try q.push(io, .control, try testFrame(allocator, 2));
    try q.push(io, .data, try testFrame(allocator, 3));

    // deinit must free all three frames; testing.allocator detects leaks.
    q.deinit(io);
}

/// Shared context for the blocking-pop wake test. Produced by the main test
/// fiber; consumed by the popper fiber running on a different OS thread.
const BlockCtx = struct {
    queue: *OutboundQueue,
    popped_byte: std.atomic.Value(u8) = .init(0),
};

fn blockedPopper(io: std.Io, ctx: *BlockCtx) void {
    var frame = ctx.queue.popBlocking(io) catch return;
    defer frame.deinit(std.testing.allocator);
    ctx.popped_byte.store(frame.bytes[0], .release);
}

test "popBlocking wakes when push arrives from another fiber" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    var ctx = BlockCtx{ .queue = &q };

    // Launch the popper as a concurrent fiber. The queue is empty so the popper
    // will park in signal.wait(). A subsequent push from this fiber notifies the
    // signal, wakes the popper, and the popper receives the frame — proving the
    // end-to-end wake path. The popped byte value (0xAB) confirms delivery.
    var fut = try std.Io.concurrent(io, blockedPopper, .{ io, &ctx });

    // Yield for 50 ms (fiber-safe: suspends this fiber, not the OS thread) to
    // give the popper fiber time to park in signal.wait() before we push.
    io_time.ms(50).sleep(io) catch {};

    // Push a frame; this notifies the signal and wakes the popper.
    const frame = try testFrame(allocator, 0xAB);
    try q.push(io, .data, frame);

    fut.await(io);

    // The popped byte proves the popper was woken and received the pushed frame.
    try std.testing.expectEqual(@as(u8, 0xAB), ctx.popped_byte.load(.acquire));
}
