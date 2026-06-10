const std = @import("std");
const AtomicRc = @import("ref_count").AtomicRc;
const channel = @import("../../quic/io/channel.zig");
const io_time = @import("../../quic/io/time.zig");
const pubsub = @import("pubsub.zig");
const PeerId = @import("peer_id").PeerId;

/// The lane an outbound frame travels on, in decreasing priority.
/// subscribe is highest (subscriptions are rare and critical),
/// control is medium (GRAFT/PRUNE/IHAVE/IWANT/IDONTWANT),
/// data is lowest (published/forwarded application messages).
pub const Lane = enum { subscribe, control, data };

/// A refcounted, heap-allocated, pre-framed RPC ready to write to a QUIC stream.
/// Forwarding a message to N peers frames the wire bytes ONCE and shares this one
/// allocation across every target's queue (the rust `Bytes` / go shared-message
/// model), so a 1 MiB message costs one copy, not N.
///
/// Ownership is the reference count: each holder (the builder, every queue that
/// accepts a push, the writer that pops it) owns exactly one reference. A holder
/// gives up its reference with `release`; the frame frees its own bytes/id/self
/// when the last reference is released. Use `create` to mint one (which sets the
/// initial count to the number of intended holders) and `retain` to add a holder.
///
/// bytes: length-prefixed wire bytes produced by pubsub.frameRpc; owned.
/// message_id: owned id carried for IDONTWANT purge; null for every lane
///   except data frames that may later be IDONTWANT-purged.
/// rc: live holder count; the frame frees itself on the 1→0 transition.
/// allocator: the allocator that owns bytes/id/self, used by the final release.
pub const OutboundFrame = struct {
    bytes: []u8,
    /// The id of the message a data frame carries (for IDONTWANT matching);
    /// null for control/subscribe frames. Owned by the frame — a PRIVATE copy,
    /// never a reference into the router's intern table: an interned id's
    /// LAST release unlinks it from the unsynchronized, router-fiber-owned
    /// intern hash table, and a frame's last reference is usually dropped on
    /// a writer fiber (the refcount itself is atomic; the table is not).
    message_id: ?[]u8,
    rc: AtomicRc,
    allocator: std.mem.Allocator,

    /// Mint a heap frame with `initial_refs` references (the number of intended
    /// holders — e.g. 1 for a builder that will `retain` per target before each
    /// push). Takes ownership of `bytes` and `message_id`; on the final release
    /// it frees both plus itself with `allocator`.
    pub fn create(
        allocator: std.mem.Allocator,
        bytes: []u8,
        message_id: ?[]u8,
        initial_refs: usize,
    ) std.mem.Allocator.Error!*OutboundFrame {
        const self = try allocator.create(OutboundFrame);
        self.* = .{
            .bytes = bytes,
            .message_id = message_id,
            .rc = .initCount(initial_refs),
            .allocator = allocator,
        };
        return self;
    }

    /// Add one holder. Cheap and unordered: a new holder only becomes reachable
    /// to other threads through the queue mutex / signal, which carry the
    /// happens-before edge, so the bump itself needs no ordering.
    pub fn retain(self: *OutboundFrame) void {
        self.rc.retain();
    }

    /// Give up one holder's reference. On the 1→0 transition this frees the
    /// bytes, the carried message id, and the frame itself. Writers release from
    /// different executor threads, so the decrement is `.release` and the freeing
    /// thread issues an `.acquire` fence first, establishing happens-before with
    /// every prior holder's writes before the memory is reclaimed.
    pub fn release(self: *OutboundFrame) void {
        if (!self.rc.release()) return;
        const allocator = self.allocator;
        allocator.free(self.bytes);
        if (self.message_id) |id| allocator.free(id);
        allocator.destroy(self);
    }
};

pub const Options = struct {
    /// Max un-popped frames the control lane will hold; further pushes fail.
    control_cap: usize = 4096,
    /// Max un-popped frames the data lane will hold; further pushes fail.
    data_cap: usize = 1024,
    /// Max un-popped frames the subscribe lane will hold; further pushes fail.
    /// Subscriptions are rare (one frame per local topic change, plus the
    /// full-set announce to a new peer), so this cap only ever binds when the
    /// writer is wedged on a stalled peer — where an unbounded lane would
    /// otherwise grow without limit (every other lane is capped).
    subscribe_cap: usize = 256,
};

/// A single-lane bounded FIFO of frame references, backed by a std.ArrayList plus
/// a head cursor. Each live slot holds one reference to a (shared) OutboundFrame;
/// pop transfers it out, removeIf/drain release it. Pop is O(1) (head++) at the
/// cost of dead prefix slots, which are reclaimed lazily (see `compact`) so the
/// amortised cost stays O(1) without a circular buffer. `cap == 0` means
/// unbounded.
///
/// Not synchronized on its own — OutboundQueue holds these behind its mutex.
const LaneFifo = struct {
    items: std.ArrayList(*OutboundFrame) = .empty,
    head: usize = 0,
    cap: usize,

    /// Number of un-popped (live) frames.
    fn len(self: *const LaneFifo) usize {
        return self.items.items.len - self.head;
    }

    /// Append a frame reference. Returns error.LaneFull when the lane is at
    /// capacity, or when the backing allocation fails (OOM is treated as full:
    /// the lane is bounded, so the caller's fallback — keep the reference and drop
    /// — is identical either way). On error the caller still owns the reference.
    fn push(self: *LaneFifo, allocator: std.mem.Allocator, frame: *OutboundFrame) error{LaneFull}!void {
        if (self.cap != 0 and self.len() >= self.cap) return error.LaneFull;
        self.items.append(allocator, frame) catch return error.LaneFull;
    }

    /// Remove and return the oldest live frame reference, transferring it to the
    /// caller. Returns null when the lane is empty.
    fn pop(self: *LaneFifo) ?*OutboundFrame {
        if (self.head >= self.items.items.len) return null;
        const frame = self.items.items[self.head];
        self.head += 1;
        self.compact();
        return frame;
    }

    /// Reclaim the dead prefix once it is at least as large as the live count by
    /// shifting live frames to the front and resetting the cursor. Triggering on
    /// `head >= live` bounds the wasted slots to the live count and amortises the
    /// shift to O(1) per pop. The backing storage is retained, never reallocated.
    fn compact(self: *LaneFifo) void {
        if (self.head == 0) return;
        const live = self.len();
        if (self.head < live) return;
        std.mem.copyForwards(*OutboundFrame, self.items.items[0..live], self.items.items[self.head..]);
        self.items.shrinkRetainingCapacity(live);
        self.head = 0;
    }

    /// Remove and release the reference for every live frame for which
    /// pred(ctx, frame) is true, preserving FIFO order of the survivors. Returns
    /// the count removed.
    ///
    /// Single pass with a read/write cursor over the live range: O(n), no
    /// repeated tail shifts. `head` is left untouched so already-popped slots are
    /// never read or released.
    fn removeIf(
        self: *LaneFifo,
        ctx: anytype,
        comptime pred: fn (@TypeOf(ctx), *const OutboundFrame) bool,
    ) usize {
        const live = self.items.items[self.head..];
        var write: usize = 0;
        var removed: usize = 0;
        for (live) |frame| {
            if (pred(ctx, frame)) {
                frame.release();
                removed += 1;
            } else {
                live[write] = frame;
                write += 1;
            }
        }
        self.items.shrinkRetainingCapacity(self.head + write);
        return removed;
    }

    /// Release every live frame reference and reset the cursor, retaining the
    /// backing storage. Slots before `head` were already transferred to a popper
    /// and must not be released here.
    fn drain(self: *LaneFifo) void {
        for (self.items.items[self.head..]) |frame| frame.release();
        self.items.clearRetainingCapacity();
        self.head = 0;
    }

    /// Release the backing storage. Call `drain` first to release any live frames.
    fn deinit(self: *LaneFifo, allocator: std.mem.Allocator) void {
        self.items.deinit(allocator);
    }
};

/// Per-peer outbound queue, decoupled from the QUIC stream. A router fiber
/// pushes frames; a writer fiber (started in a later phase) pops and writes
/// them. Thread/fiber-safe: a std.Io.Mutex guards the lane FIFOs and a Signal
/// wakes a blocked popper with no lost-wakeup: the epoch is observed under the
/// lock before unlocking, so a notify that races after unlock advances the epoch
/// and the subsequent wait() returns immediately.
///
/// Lanes are drained by priority subscribe > control > data; each lane is
/// bounded by its matching Options cap.
pub const OutboundQueue = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex,
    signal: channel.Signal,
    sub_lane: LaneFifo,
    ctrl_lane: LaneFifo,
    data_lane: LaneFifo,
    closed: bool,

    pub fn init(allocator: std.mem.Allocator, opts: Options) OutboundQueue {
        return .{
            .allocator = allocator,
            .mutex = .init,
            .signal = .{},
            .sub_lane = .{ .cap = opts.subscribe_cap },
            .ctrl_lane = .{ .cap = opts.control_cap },
            .data_lane = .{ .cap = opts.data_cap },
            .closed = false,
        };
    }

    /// Release every remaining frame reference and free all lane storage.
    pub fn deinit(self: *OutboundQueue, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        for (self.lanes()) |lane| lane.drain();
        self.mutex.unlock(io);
        for (self.lanes()) |lane| lane.deinit(self.allocator);
    }

    pub const PushError = error{ LaneFull, Closed };

    /// Enqueue a frame reference on the given lane. On SUCCESS the queue takes
    /// the reference (the caller must NOT release it). On LaneFull or Closed the
    /// queue did NOT take the reference and the caller still owns it (must
    /// `release` it). Success wakes any blocked popper.
    pub fn push(self: *OutboundQueue, io: std.Io, lane: Lane, frame: *OutboundFrame) PushError!void {
        self.mutex.lockUncancelable(io);
        if (self.closed) {
            self.mutex.unlock(io);
            return error.Closed;
        }
        self.laneFor(lane).push(self.allocator, frame) catch |err| {
            self.mutex.unlock(io);
            return err;
        };
        self.mutex.unlock(io);
        self.signal.notify(io);
    }

    /// Block until a frame is available (priority: subscribe > control > data)
    /// or the queue is closed and empty. Transfers the frame reference to the
    /// caller (which must eventually `release` it). Returns error.Closed when the
    /// queue is closed and all lanes are drained. Returns error.Canceled if the
    /// fiber is cancelled while waiting.
    pub fn popBlocking(self: *OutboundQueue, io: std.Io) error{ Closed, Canceled }!*OutboundFrame {
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

    /// Number of un-popped (live) frames on the data lane. Taken under the mutex
    /// so it never races a concurrent push/pop. Used by router unit tests to
    /// observe an IDONTWANT purge (frames removed) without draining the queue.
    pub fn dataLen(self: *OutboundQueue, io: std.Io) usize {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.data_lane.len();
    }

    /// Remove and release every data-lane frame for which pred(ctx, frame)
    /// returns true. Returns the count of removed frames. Other lanes are
    /// untouched.
    pub fn removeData(
        self: *OutboundQueue,
        io: std.Io,
        ctx: anytype,
        comptime pred: fn (@TypeOf(ctx), *const OutboundFrame) bool,
    ) usize {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.data_lane.removeIf(ctx, pred);
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

    /// The lanes in priority order; used for whole-queue operations (drain).
    fn lanes(self: *OutboundQueue) [3]*LaneFifo {
        return .{ &self.sub_lane, &self.ctrl_lane, &self.data_lane };
    }

    fn laneFor(self: *OutboundQueue, lane: Lane) *LaneFifo {
        return switch (lane) {
            .subscribe => &self.sub_lane,
            .control => &self.ctrl_lane,
            .data => &self.data_lane,
        };
    }

    /// Pop the highest-priority available frame reference. Returns null if all
    /// lanes are empty. Caller must hold the mutex.
    fn tryPopLocked(self: *OutboundQueue) ?*OutboundFrame {
        for (self.lanes()) |lane| {
            if (lane.pop()) |frame| return frame;
        }
        return null;
    }
};

/// Drains an OutboundQueue onto a peer's outbound stream, decoupled from the
/// stream's lifetime. The queue outlives any single stream: when a write fails
/// mid-frame we drop only that one in-flight frame, tear the stream down, and
/// lazily re-open a fresh stream on the next iteration to keep draining the
/// surviving queue (rust-libp2p semantics — go drops the whole queue instead).
///
/// Generic over a duck-typed `Sink` so it can be exercised with fakes here and
/// adapted to the real QUIC stream later. The sink must provide:
///   fn open(self: *Sink, io: std.Io) anyerror!void
///       (re)establish the current outbound stream.
///   fn writeFrame(self: *Sink, io: std.Io, bytes: []const u8) anyerror!void
///       write framed bytes to the current stream. A failure may leave a partial
///       (truncated) length-prefixed frame on the wire: an implementation over
///       stream.writeAll can error after some bytes have already gone out. The
///       writer's response is to close/reset the stream and re-open, so the
///       partial frame is never completed; the peer discards the malformed RPC
///       and gossip redundancy covers the lost message.
///   fn close(self: *Sink, io: std.Io) void
///       tear down the current stream; safe to call when none is open.
///
/// Lifetime: the `Sink` (and any stream it holds) MUST stay valid until the
/// writer fiber has fully exited. `run` may invoke `on_disconnect` and then its
/// top-level `defer sink.close(io)` still fires on the way out, so an
/// `on_disconnect` handler MUST NOT synchronously free or destroy the sink or
/// its stream — that trailing `close` would be a use-after-free. The owner must
/// join/await the writer fiber before freeing the sink.
pub fn PeerWriter(comptime Sink: type) type {
    return struct {
        const Self = @This();

        queue: *OutboundQueue,
        sink: *Sink,
        /// Open attempts before giving up on the peer (each followed by backoff).
        max_open_retries: usize = 5,
        /// Consecutive failed frame writes (each followed by a stream re-open)
        /// before giving up on the peer. A STALLED-but-alive peer fails every
        /// write at the sink's write timeout; without this bound the writer
        /// would cycle write-timeout -> reopen forever, draining one dropped
        /// frame per timeout while the peer pins its queue caps. Resets on any
        /// successful write.
        max_write_failures: usize = 3,
        /// Sleep between open attempts, in milliseconds.
        reopen_backoff_ms: u64 = 50,
        /// Invoked once if open retries are exhausted, so the router can tear the
        /// peer down. The writer never touches the queue lifecycle itself: on
        /// disconnect it releases only the in-flight popped frame's reference and
        /// returns — frames still queued stay in the (owner-held) OutboundQueue.
        /// The owner is responsible for draining/deinit-ing the queue after the
        /// disconnect, otherwise those unsent frames leak. The handler must also
        /// not free the sink/stream synchronously (see the Sink lifetime note
        /// above).
        on_disconnect: ?*const fn (?*anyopaque) void = null,
        disconnect_ctx: ?*anyopaque = null,
        /// Whether the sink currently holds an open stream.
        have_stream: bool = false,

        /// Fiber body. Pops frame references in priority order and writes them,
        /// re-opening the stream as needed, until the queue is closed/drained or
        /// cancelled. Each popped reference is released after its write attempt
        /// (the shared frame frees itself once the last writer releases it).
        /// Always releases the current stream on exit.
        pub fn run(self: *Self, io: std.Io) void {
            defer self.sink.close(io);
            var write_failures: usize = 0;
            while (true) {
                const frame = self.queue.popBlocking(io) catch return; // Closed/Canceled
                if (!self.have_stream) {
                    if (!self.ensureStream(io)) {
                        // Open retries exhausted: drop the popped frame's
                        // reference and hand the peer back to the router via
                        // on_disconnect.
                        frame.release();
                        if (self.on_disconnect) |cb| cb(self.disconnect_ctx);
                        return;
                    }
                }
                self.sink.writeFrame(io, frame.bytes) catch {
                    // The stream died or the write timed out (a stalled peer).
                    // Lose only this in-flight frame, close the stream, and
                    // re-open lazily next iteration — unless this keeps
                    // happening: after max_write_failures consecutive failures
                    // the peer is hopeless (stalled or flapping), so give it
                    // back to the router instead of cycling forever.
                    self.sink.close(io);
                    self.have_stream = false;
                    frame.release();
                    write_failures += 1;
                    if (write_failures >= self.max_write_failures) {
                        if (self.on_disconnect) |cb| cb(self.disconnect_ctx);
                        return;
                    }
                    continue;
                };
                write_failures = 0;
                frame.release();
            }
        }

        /// Open the stream with bounded retries and backoff. Returns true once a
        /// stream is open, false after exhausting retries (or on cancellation
        /// during backoff). Sets have_stream on success.
        fn ensureStream(self: *Self, io: std.Io) bool {
            var attempt: usize = 0;
            while (attempt < self.max_open_retries) : (attempt += 1) {
                if (self.sink.open(io)) |_| {
                    self.have_stream = true;
                    return true;
                } else |_| {
                    io_time.ms(self.reopen_backoff_ms).sleep(io) catch return false;
                }
            }
            return false;
        }
    };
}

/// An inbound RPC tagged with the peer that sent it, handed to the router's
/// inbox. The consumer that drains the inbox owns each rpc and must deinit it.
pub const InboundRpc = struct {
    peer: PeerId,
    rpc: pubsub.OwnedRpc,
};

/// Reads parsed RPCs off a peer's inbound stream and posts each, tagged with
/// the sender's PeerId, through a duck-typed `Poster`. A clean EOF or a broken
/// stream is signalled by `read` returning an error, on which the reader simply
/// exits — the router replaces it when a new inbound stream arrives.
///
/// The reader is generic over both ends so it can be exercised with fakes and
/// adapted to the real QUIC stream + a single-inbox router. It is decoupled from
/// the router's Command inbox: the router supplies a Poster that wraps each
/// InboundRpc into its own Command variant and posts it to the one inbox, so the
/// router keeps a single ordered queue for every event it processes.
///
///   `Source` must provide:
///     fn read(self: *Source, allocator: std.mem.Allocator, io: std.Io)
///         anyerror!pubsub.OwnedRpc
///         return the next parsed RPC, or an error on EOF/shutdown/broken stream.
///   `Poster` must provide:
///     fn post(self: *Poster, io: std.Io, rpc: InboundRpc) anyerror!void
///         hand off one inbound rpc. On a returned error the reader still owns
///         the rpc and frees it before exiting (the post failed to take it).
pub fn PeerReader(comptime Source: type, comptime Poster: type) type {
    return struct {
        const Self = @This();

        source: *Source,
        poster: *Poster,
        peer: PeerId,
        allocator: std.mem.Allocator,

        /// Fiber body. Reads RPCs and hands each to the poster until the stream
        /// ends/breaks or the poster rejects (closed inbox / cancelled).
        /// Ownership of each rpc transfers to the poster on success; an rpc that
        /// cannot be handed off is freed here.
        pub fn run(self: *Self, io: std.Io) void {
            while (true) {
                var owned = self.source.read(self.allocator, io) catch return;
                self.poster.post(io, .{ .peer = self.peer, .rpc = owned }) catch {
                    // Post failed (inbox closed or cancelled): we still own the
                    // rpc, so free it.
                    owned.deinit(self.allocator);
                    return;
                };
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Build a minimal shared frame for testing: 1 byte, no message id, one
/// reference. The caller owns that reference (release it, push it, or hand it to
/// a queue.deinit).
fn testFrame(allocator: std.mem.Allocator, byte: u8) !*OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    errdefer allocator.free(bytes);
    bytes[0] = byte;
    return OutboundFrame.create(allocator, bytes, null, 1);
}

/// Build a shared frame carrying a copy of `id` so the final release has a
/// real id allocation to free. Used to pin the message_id ownership contract.
/// One reference, owned by the caller.
fn testFrameWithId(allocator: std.mem.Allocator, byte: u8, id: []const u8) !*OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    errdefer allocator.free(bytes);
    bytes[0] = byte;
    const owned_id = try allocator.dupe(u8, id);
    errdefer allocator.free(owned_id);
    return OutboundFrame.create(allocator, bytes, owned_id, 1);
}

test "OutboundFrame release frees owned slices" {
    const allocator = std.testing.allocator;
    const frame = try testFrame(allocator, 0x42);
    frame.release();
    // testing.allocator detects leaks at test end.
}

test "OutboundFrame release frees the carried message id" {
    const allocator = std.testing.allocator;
    const frame = try testFrameWithId(allocator, 0x42, "id-a");
    try std.testing.expectEqualStrings("id-a", frame.message_id.?);
    frame.release();
    // testing.allocator detects a leak if the id is missed.
}

test "OutboundFrame retain/release: frees only on the last release" {
    const allocator = std.testing.allocator;
    // One holder, then add two more: three references total.
    const frame = try testFrameWithId(allocator, 0x01, "x");
    frame.retain();
    frame.retain();
    try std.testing.expectEqual(@as(usize, 3), frame.rc.count());
    // Release two; the bytes are still live (no double-free, no use-after-free).
    frame.release();
    frame.release();
    try std.testing.expectEqual(@as(usize, 1), frame.rc.count());
    try std.testing.expectEqual(@as(u8, 0x01), frame.bytes[0]);
    // The final release frees everything; testing.allocator confirms no leak.
    frame.release();
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

    const f1 = try q.popBlocking(io);
    defer f1.release();
    try std.testing.expectEqual(@as(u8, 's'), f1.bytes[0]);

    const f2 = try q.popBlocking(io);
    defer f2.release();
    try std.testing.expectEqual(@as(u8, 'c'), f2.bytes[0]);

    const f3 = try q.popBlocking(io);
    defer f3.release();
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

    const r1 = try q.popBlocking(io);
    defer r1.release();
    try std.testing.expectEqual(@as(u8, 1), r1.bytes[0]);

    const r2 = try q.popBlocking(io);
    defer r2.release();
    try std.testing.expectEqual(@as(u8, 2), r2.bytes[0]);

    const r3 = try q.popBlocking(io);
    defer r3.release();
    try std.testing.expectEqual(@as(u8, 3), r3.bytes[0]);
}

test "subscribe lane is unbounded (accepts far beyond the bounded caps)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Tiny bounded caps; subscribe has cap 0 (unbounded) and must ignore them.
    var q = OutboundQueue.init(allocator, .{ .control_cap = 1, .data_cap = 1 });
    defer q.deinit(io);

    const n = 50;
    var pushed: usize = 0;
    while (pushed < n) : (pushed += 1) {
        try q.push(io, .subscribe, try testFrame(allocator, @intCast(pushed)));
    }

    var popped: usize = 0;
    while (popped < n) : (popped += 1) {
        const f = try q.popBlocking(io);
        defer f.release();
        try std.testing.expectEqual(@as(u8, @intCast(popped)), f.bytes[0]);
    }
}

test "control_cap and data_cap enforced at and one past the boundary" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{ .control_cap = 2, .data_cap = 1 });
    defer q.deinit(io);

    const c1 = try testFrame(allocator, 1);
    const c2 = try testFrame(allocator, 2);
    const c3 = try testFrame(allocator, 3); // rejected; caller keeps its ref

    try q.push(io, .control, c1); // 1/2
    try q.push(io, .control, c2); // 2/2 — exactly at cap, still accepted
    try std.testing.expectError(error.LaneFull, q.push(io, .control, c3)); // one over
    c3.release(); // queue did not take the ref; caller releases it

    const d1 = try testFrame(allocator, 10);
    const d2 = try testFrame(allocator, 11); // rejected

    try q.push(io, .data, d1); // 1/1 — exactly at cap
    try std.testing.expectError(error.LaneFull, q.push(io, .data, d2)); // one over
    d2.release();

    // After popping one control frame the lane has room again (cap is on live
    // count, not lifetime count).
    const rc1 = try q.popBlocking(io);
    rc1.release();
    const c4 = try testFrame(allocator, 4);
    try q.push(io, .control, c4);

    // Drain accepted items so the queue deinit has nothing left.
    const rc2 = try q.popBlocking(io);
    rc2.release();
    const rc4 = try q.popBlocking(io);
    rc4.release();
    const rd1 = try q.popBlocking(io);
    rd1.release();
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
    const r_ctrl = try q.popBlocking(io);
    defer r_ctrl.release();
    try std.testing.expectEqual(@as(u8, 99), r_ctrl.bytes[0]);

    const r1 = try q.popBlocking(io);
    defer r1.release();
    try std.testing.expectEqual(@as(u8, 1), r1.bytes[0]);

    const r3 = try q.popBlocking(io);
    defer r3.release();
    try std.testing.expectEqual(@as(u8, 3), r3.bytes[0]);
}

test "removeData removing all and removing none" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const always = struct {
        fn pred(_: void, _: *const OutboundFrame) bool {
            return true;
        }
    }.pred;
    const never = struct {
        fn pred(_: void, _: *const OutboundFrame) bool {
            return false;
        }
    }.pred;

    // Remove none: every frame survives in FIFO order.
    {
        var q = OutboundQueue.init(allocator, .{});
        defer q.deinit(io);
        try q.push(io, .data, try testFrame(allocator, 1));
        try q.push(io, .data, try testFrame(allocator, 2));
        try std.testing.expectEqual(@as(usize, 0), q.removeData(io, {}, never));
        const a = try q.popBlocking(io);
        defer a.release();
        try std.testing.expectEqual(@as(u8, 1), a.bytes[0]);
        const b = try q.popBlocking(io);
        defer b.release();
        try std.testing.expectEqual(@as(u8, 2), b.bytes[0]);
    }

    // Remove all: the lane empties and the next pop would block, so just check
    // the count and let deinit confirm no leak / double-free.
    {
        var q = OutboundQueue.init(allocator, .{});
        defer q.deinit(io);
        try q.push(io, .data, try testFrame(allocator, 1));
        try q.push(io, .data, try testFrame(allocator, 2));
        try q.push(io, .data, try testFrame(allocator, 3));
        try std.testing.expectEqual(@as(usize, 3), q.removeData(io, {}, always));
    }

    // removeData on an empty lane is a no-op returning 0.
    {
        var q = OutboundQueue.init(allocator, .{});
        defer q.deinit(io);
        try std.testing.expectEqual(@as(usize, 0), q.removeData(io, {}, always));
    }
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
    const first = try q.popBlocking(io);
    try std.testing.expectEqual(@as(u8, 10), first.bytes[0]);
    first.release();

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
    const r20 = try q.popBlocking(io);
    defer r20.release();
    try std.testing.expectEqual(@as(u8, 20), r20.bytes[0]);

    const r40 = try q.popBlocking(io);
    defer r40.release();
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

test "push after close is rejected and caller keeps ownership" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    q.close(io);
    const f = try testFrame(allocator, 7);
    try std.testing.expectError(error.Closed, q.push(io, .data, f));
    f.release(); // queue did not take the ref; caller releases it
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
    const r1 = try q.popBlocking(io);
    defer r1.release();
    const r2 = try q.popBlocking(io);
    defer r2.release();
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
    const frame = ctx.queue.popBlocking(io) catch return;
    defer frame.release();
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

    // Yield (fiber-safe: suspends this fiber, not the OS thread) to give the
    // popper fiber time to park in signal.wait() before we push.
    io_time.ms(50).sleep(io) catch {};

    // Push a frame; this notifies the signal and wakes the popper.
    const frame = try testFrame(allocator, 0xAB);
    try q.push(io, .data, frame);

    fut.await(io);

    // The popped byte proves the popper was woken and received the pushed frame.
    try std.testing.expectEqual(@as(u8, 0xAB), ctx.popped_byte.load(.acquire));
}

/// Shared context for the close-wakes-popper test.
const CloseCtx = struct {
    queue: *OutboundQueue,
    got_closed: std.atomic.Value(bool) = .init(false),
};

fn closeWaitingPopper(io: std.Io, ctx: *CloseCtx) void {
    if (ctx.queue.popBlocking(io)) |frame| {
        frame.release();
    } else |err| switch (err) {
        error.Closed => ctx.got_closed.store(true, .release),
        error.Canceled => {},
    }
}

test "close wakes a blocked popper which then returns Closed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    var ctx = CloseCtx{ .queue = &q };

    // The popper parks on an empty queue; close() must notify the signal, wake
    // it, and (with all lanes empty) make popBlocking return error.Closed.
    var fut = try std.Io.concurrent(io, closeWaitingPopper, .{ io, &ctx });
    io_time.ms(50).sleep(io) catch {};
    q.close(io);
    fut.await(io);

    try std.testing.expect(ctx.got_closed.load(.acquire));
}

// --- PeerWriter / PeerReader fakes + tests ---------------------------------

/// A test Sink that records every byte written to each "stream" it opens. Each
/// open appends a fresh buffer; writes land in the current buffer. It can be
/// configured to fail the first `fail_open_count` opens and to fail the
/// `fail_on_write_n`-th write (1-based; 0 = never), so the writer's open-retry
/// and mid-write re-open paths can be exercised. Owns all buffers.
const FakeSink = struct {
    allocator: std.mem.Allocator,
    streams: std.ArrayList(std.ArrayList(u8)) = .empty,
    /// Number of leading open() calls that should fail.
    fail_open_count: usize = 0,
    open_calls: usize = 0,
    /// The 1-based write index that should fail; 0 disables write failures.
    fail_on_write_n: usize = 0,
    write_calls: usize = 0,
    /// Set true while a stream is open (cleared by close).
    open_now: bool = false,

    fn deinit(self: *FakeSink) void {
        for (self.streams.items) |*buf| buf.deinit(self.allocator);
        self.streams.deinit(self.allocator);
    }

    fn open(self: *FakeSink, io: std.Io) anyerror!void {
        _ = io;
        self.open_calls += 1;
        if (self.open_calls <= self.fail_open_count) return error.OpenFailed;
        try self.streams.append(self.allocator, .empty);
        self.open_now = true;
    }

    fn writeFrame(self: *FakeSink, io: std.Io, bytes: []const u8) anyerror!void {
        _ = io;
        self.write_calls += 1;
        if (self.fail_on_write_n != 0 and self.write_calls == self.fail_on_write_n) {
            return error.StreamShutdown;
        }
        try self.streams.items[self.streams.items.len - 1].appendSlice(self.allocator, bytes);
    }

    fn close(self: *FakeSink, io: std.Io) void {
        _ = io;
        self.open_now = false;
    }

    /// Number of streams that were successfully opened.
    fn streamCount(self: *const FakeSink) usize {
        return self.streams.items.len;
    }

    /// The bytes written to the i-th opened stream.
    fn streamBytes(self: *const FakeSink, i: usize) []const u8 {
        return self.streams.items[i].items;
    }
};

/// Build a shared frame (one reference) whose single byte is `byte`, matching
/// what the writer pops and writes to the sink. (One byte per frame keeps the
/// assertions about which bytes landed on which stream trivially readable.)
fn writerFrame(allocator: std.mem.Allocator, byte: u8) !*OutboundFrame {
    return testFrame(allocator, byte);
}

test "PeerWriter happy path: all frames written to a single stream in order" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    try q.push(io, .data, try writerFrame(allocator, 1));
    try q.push(io, .data, try writerFrame(allocator, 2));
    try q.push(io, .data, try writerFrame(allocator, 3));
    q.close(io);

    var sink = FakeSink{ .allocator = allocator };
    defer sink.deinit();

    var writer = PeerWriter(FakeSink){ .queue = &q, .sink = &sink };
    writer.run(io);

    // One stream opened, holding all three frames in order.
    try std.testing.expectEqual(@as(usize, 1), sink.streamCount());
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, sink.streamBytes(0));
}

test "PeerWriter re-opens on write failure, dropping only the in-flight frame" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    // Five distinguishable frames; the queue is closed so run() terminates.
    try q.push(io, .data, try writerFrame(allocator, 1));
    try q.push(io, .data, try writerFrame(allocator, 2));
    try q.push(io, .data, try writerFrame(allocator, 3));
    try q.push(io, .data, try writerFrame(allocator, 4));
    try q.push(io, .data, try writerFrame(allocator, 5));
    q.close(io);

    // The second write fails: frame #2 is the in-flight casualty.
    var sink = FakeSink{ .allocator = allocator, .fail_on_write_n = 2 };
    defer sink.deinit();

    var writer = PeerWriter(FakeSink){ .queue = &q, .sink = &sink };
    writer.run(io);

    // The surviving queue drained onto a fresh stream: frame #1 on the first
    // stream, frame #2 dropped, frames #3,#4,#5 on the second stream — no
    // duplication, order preserved.
    try std.testing.expectEqual(@as(usize, 2), sink.streamCount());
    try std.testing.expectEqualSlices(u8, &.{1}, sink.streamBytes(0));
    try std.testing.expectEqualSlices(u8, &.{ 3, 4, 5 }, sink.streamBytes(1));
}

/// Flag context for the open-exhaustion test's on_disconnect callback.
const DisconnectFlag = struct {
    fired: usize = 0,
    fn cb(ctx: ?*anyopaque) void {
        const self: *DisconnectFlag = @ptrCast(@alignCast(ctx.?));
        self.fired += 1;
    }
};

test "PeerWriter open exhaustion calls on_disconnect once and frees the frame" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);

    try q.push(io, .data, try writerFrame(allocator, 7));
    q.close(io);

    // Every open fails, so ensureStream exhausts its retries.
    var sink = FakeSink{ .allocator = allocator, .fail_open_count = std.math.maxInt(usize) };
    defer sink.deinit();

    var flag = DisconnectFlag{};
    var writer = PeerWriter(FakeSink){
        .queue = &q,
        .sink = &sink,
        .max_open_retries = 3,
        .reopen_backoff_ms = 0,
        .on_disconnect = DisconnectFlag.cb,
        .disconnect_ctx = &flag,
    };
    writer.run(io);

    // on_disconnect fired exactly once; the popped frame was freed (no leak);
    // no stream was ever opened.
    try std.testing.expectEqual(@as(usize, 1), flag.fired);
    try std.testing.expectEqual(@as(usize, 0), sink.streamCount());
}

test "PeerWriter returns immediately when the queue is already closed and empty" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var q = OutboundQueue.init(allocator, .{});
    defer q.deinit(io);
    q.close(io);

    var sink = FakeSink{ .allocator = allocator };
    defer sink.deinit();

    var writer = PeerWriter(FakeSink){ .queue = &q, .sink = &sink };
    writer.run(io); // popBlocking → Closed; defer sink.close runs, no stream opened.

    try std.testing.expectEqual(@as(usize, 0), sink.streamCount());
}

/// Build an OwnedRpc carrying a single subscription to `topic`, so the reader
/// tests can verify ordering by reading the topic id back out, and so
/// OwnedRpc.deinit has real heap bytes to free.
fn subscriptionRpc(allocator: std.mem.Allocator, topic: []const u8) !pubsub.OwnedRpc {
    const rpc_pb = @import("../../protobuf.zig").rpc;
    const sub = rpc_pb.RPC{
        .subscriptions = &[_]?rpc_pb.RPC.SubOpts{.{ .subscribe = true, .topicid = topic }},
    };
    // encode yields a const slice; OwnedRpc owns a mutable []u8 (matching the
    // heap buffer readRpc allocates), so copy into one the reader can borrow.
    const encoded = try sub.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);
    const payload = try allocator.dupe(u8, encoded);
    errdefer allocator.free(payload);
    return .{ .bytes = payload, .reader = try rpc_pb.RPCReader.init(payload) };
}

/// Read back the topic id of the first subscription in an OwnedRpc.
fn rpcTopic(owned: *pubsub.OwnedRpc) []const u8 {
    var reader = owned.reader;
    const sub = reader.subscriptionsNext() orelse return "";
    return sub.getTopicid();
}

/// A test Source that yields pre-built OwnedRpcs in order, then returns a
/// terminal error to signal EOF/shutdown. It owns any rpcs not yet read and
/// frees them on deinit.
const FakeSource = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayList(pubsub.OwnedRpc) = .empty,
    next: usize = 0,
    terminal: anyerror = error.EndOfStream,

    fn deinit(self: *FakeSource) void {
        // Free only rpcs the reader never consumed.
        for (self.items.items[self.next..]) |*owned| owned.deinit(self.allocator);
        self.items.deinit(self.allocator);
    }

    fn push(self: *FakeSource, owned: pubsub.OwnedRpc) !void {
        try self.items.append(self.allocator, owned);
    }

    fn read(self: *FakeSource, allocator: std.mem.Allocator, io: std.Io) anyerror!pubsub.OwnedRpc {
        _ = allocator;
        _ = io;
        if (self.next >= self.items.items.len) return self.terminal;
        const owned = self.items.items[self.next];
        self.next += 1;
        return owned;
    }
};

/// A test Poster that forwards each posted InboundRpc to a real std.Io.Queue,
/// mirroring how the router's poster will wrap an InboundRpc into a Command and
/// post it to the single router inbox. When `reject` is true every post fails so
/// the reader's free-on-rejection path can be exercised.
const FakePoster = struct {
    inbox: *std.Io.Queue(InboundRpc),
    reject: bool = false,

    fn post(self: *FakePoster, io: std.Io, rpc: InboundRpc) anyerror!void {
        if (self.reject) return error.Closed;
        try self.inbox.putOne(io, rpc);
    }
};

test "PeerReader posts each RPC tagged with the peer id, in order, then exits" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var source = FakeSource{ .allocator = allocator };
    defer source.deinit();
    try source.push(try subscriptionRpc(allocator, "t1"));
    try source.push(try subscriptionRpc(allocator, "t2"));
    try source.push(try subscriptionRpc(allocator, "t3"));

    var buffer: [4]InboundRpc = undefined;
    var inbox = std.Io.Queue(InboundRpc).init(&buffer);
    var poster = FakePoster{ .inbox = &inbox };

    const peer = try PeerId.random();
    var reader = PeerReader(FakeSource, FakePoster){
        .source = &source,
        .poster = &poster,
        .peer = peer,
        .allocator = allocator,
    };
    reader.run(io); // drains the source, posts 3 items, then EndOfStream → exit.

    const topics = [_][]const u8{ "t1", "t2", "t3" };
    for (topics) |want| {
        var item = try inbox.getOne(io);
        defer item.rpc.deinit(allocator);
        try std.testing.expect(item.peer.eql(&peer));
        try std.testing.expectEqualSlices(u8, want, rpcTopic(&item.rpc));
    }
}

test "PeerReader posts nothing and exits on an immediately broken stream" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // No items, terminal error simulating a broken stream.
    var source = FakeSource{ .allocator = allocator, .terminal = error.StreamShutdown };
    defer source.deinit();

    var buffer: [4]InboundRpc = undefined;
    var inbox = std.Io.Queue(InboundRpc).init(&buffer);
    var poster = FakePoster{ .inbox = &inbox };

    var reader = PeerReader(FakeSource, FakePoster){
        .source = &source,
        .poster = &poster,
        .peer = try PeerId.random(),
        .allocator = allocator,
    };
    reader.run(io);

    // Nothing was posted: closing the inbox lets a getOne report Closed.
    inbox.close(io);
    try std.testing.expectError(error.Closed, inbox.getOne(io));
}

test "PeerReader frees an undelivered RPC when the poster rejects" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var source = FakeSource{ .allocator = allocator };
    defer source.deinit();
    try source.push(try subscriptionRpc(allocator, "t1"));

    var buffer: [4]InboundRpc = undefined;
    var inbox = std.Io.Queue(InboundRpc).init(&buffer);
    var poster = FakePoster{ .inbox = &inbox, .reject = true };

    var reader = PeerReader(FakeSource, FakePoster){
        .source = &source,
        .poster = &poster,
        .peer = try PeerId.random(),
        .allocator = allocator,
    };
    // post fails on the first item → the reader frees the un-handed-off rpc and
    // exits. The testing allocator confirms nothing leaked.
    reader.run(io);
}
