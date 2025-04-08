//! Copied from https://github.com/ghostty-org/ghostty under the MIT license,
//! Add blocking pop and make the capacity could be configured at runtime, also
//! support multi producer and multi consumer.
//! Blocking queue implementation aimed primarily for message passing
//! between threads.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

/// Returns a blocking queue implementation for type T.
///
/// This is tailor made for ghostty usage so it isn't meant to be maximally
/// generic, but I'm happy to make it more generic over time. Traits of this
/// queue that are specific to our usage:
///
///   - Fixed size. We expect our queue to quickly drain and also not be
///     too large so we prefer a fixed size queue for now.
///   - No blocking pop. We use an external event loop mechanism such as
///     eventfd to notify our waiter that there is no data available so
///     we don't need to implement a blocking pop.
///   - Drain function. Most queues usually pop one at a time. We have
///     a mechanism for draining since on every IO loop our TTY drains
///     the full queue so we can get rid of the overhead of a ton of
///     locks and bounds checking and do a one-time drain.
///
/// One key usage pattern is that our blocking queues are single producer
/// single consumer (SPSC). This should let us do some interesting optimizations
/// in the future. At the time of writing this, the blocking queue implementation
/// is purposely naive to build something quickly, but we should benchmark
/// and make this more optimized as necessary.
pub fn BlockingQueue(
    comptime T: type,
) type {
    return struct {
        const Self = @This();

        // The type we use for queue size types. We can optimize this
        // in the future to be the correct bit-size for our preallocated
        // size for this queue.
        pub const Size = u32;

        /// Specifies the timeout for an operation.
        pub const Timeout = union(enum) {
            /// Fail instantly (non-blocking).
            instant: void,

            /// Run forever or until interrupted
            forever: void,

            /// Nanoseconds
            ns: u64,
        };

        /// Our data. The values are undefined until they are written.
        data: []T = undefined,

        /// The next location to write (next empty loc) and next location
        /// to read (next non-empty loc). The number of written elements.
        write: Size = 0,
        read: Size = 0,
        len: Size = 0,

        /// The big mutex that must be held to read/write.
        mutex: std.Thread.Mutex = .{},

        /// A CV for being notified when the queue is no longer full. This is
        /// used for writing. Note we DON'T have a CV for waiting on the
        /// queue not being EMPTY because we use external notifiers for that.
        cond_not_full: std.Thread.Condition = .{},
        not_full_waiters: usize = 0,

        /// The condition variable for being notified when the queue is no longer empty.
        /// This is used for blocking pop operations.
        cond_not_empty: std.Thread.Condition = .{},
        not_empty_waiters: usize = 0,

        /// The size of the queue. This is used for bounds checking.
        bounds: Size,

        /// Allocate the blocking queue on the heap.
        pub fn create(alloc: Allocator, capacity: usize) !*Self {
            const ptr = try alloc.create(Self);
            errdefer alloc.destroy(ptr);

            ptr.* = .{
                .data = try alloc.alloc(T, capacity),
                .bounds = @intCast(capacity),
                .len = 0,
                .write = 0,
                .read = 0,
                .mutex = .{},
                .cond_not_full = .{},
                .not_full_waiters = 0,
                .cond_not_empty = .{},
                .not_empty_waiters = 0,
            };

            return ptr;
        }

        /// Free all the resources for this queue. This should only be
        /// called once all producers and consumers have quit.
        pub fn destroy(self: *Self, alloc: Allocator) void {
            alloc.free(self.data);
            self.* = undefined;
            alloc.destroy(self);
        }

        /// Push a value to the queue. This returns the total size of the
        /// queue (unread items) after the push. A return value of zero
        /// means that the push failed.
        pub fn push(self: *Self, value: T, timeout: Timeout) Size {
            self.mutex.lock();
            defer self.mutex.unlock();

            // The
            while (self.full()) {
                switch (timeout) {
                    // If we're not waiting, then we failed to write.
                    .instant => return 0,

                    .forever => {
                        self.not_full_waiters += 1;
                        defer self.not_full_waiters -= 1;
                        self.cond_not_full.wait(&self.mutex);
                    },

                    .ns => |ns| {
                        self.not_full_waiters += 1;
                        defer self.not_full_waiters -= 1;
                        self.cond_not_full.timedWait(&self.mutex, ns) catch return 0;
                    },
                }

                // If we're still full, then we failed to write. This can
                // happen in situations where we are interrupted.
                if (self.full()) return 0;
            }

            // Add our data and update our accounting
            self.data[self.write] = value;
            self.write += 1;
            if (self.write >= self.bounds) self.write -= self.bounds;
            self.len += 1;

            // Signal any threads waiting for data
            if (self.not_empty_waiters > 0) {
                self.cond_not_empty.signal();
            }

            return self.len;
        }

        /// Pop a value from the queue without blocking.
        pub fn pop(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            // If we're empty we have nothing
            if (self.len == 0) return null;

            // Get the index we're going to read data from and do some
            // accounting. We don't copy the value here to avoid copying twice.
            const n = self.read;
            self.read += 1;
            if (self.read >= self.bounds) self.read -= self.bounds;
            self.len -= 1;

            // If we have consumers waiting on a full queue, notify.
            if (self.not_full_waiters > 0) self.cond_not_full.signal();

            return self.data[n];
        }

        /// Pop a value from the queue, potentially blocking based on timeout.
        /// Returns null if the queue is empty and the timeout is reached.
        pub fn blockingPop(self: *Self, timeout: Timeout) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            // If we're empty, wait based on timeout
            while (self.len == 0) {
                switch (timeout) {
                    // If we're not waiting, then we failed to read.
                    .instant => return null,

                    .forever => {
                        self.not_empty_waiters += 1;
                        defer self.not_empty_waiters -= 1;
                        self.cond_not_empty.wait(&self.mutex);
                    },

                    .ns => |ns| {
                        self.not_empty_waiters += 1;
                        defer self.not_empty_waiters -= 1;
                        self.cond_not_empty.timedWait(&self.mutex, ns) catch return null;
                    },
                }

                // If we're still empty, then we failed to read. This can
                // happen in situations where we are interrupted.
                if (self.len == 0) return null;
            }

            // Get the index we're going to read data from and do some
            // accounting
            const n = self.read;
            self.read += 1;
            if (self.read >= self.bounds) self.read -= self.bounds;
            self.len -= 1;

            // If we have producers waiting on a full queue, notify.
            if (self.not_full_waiters > 0) self.cond_not_full.signal();

            return self.data[n];
        }

        /// Pop all values from the queue. This will hold the big mutex
        /// until `deinit` is called on the return value. This is used if
        /// you know you're going to "pop" and utilize all the values
        /// quickly to avoid many locks, bounds checks, and cv signals.
        pub fn drain(self: *Self) DrainIterator {
            self.mutex.lock();
            return .{ .queue = self };
        }

        pub const DrainIterator = struct {
            queue: *Self,

            pub fn next(self: *DrainIterator) ?T {
                if (self.queue.len == 0) return null;

                // Read and account
                const n = self.queue.read;
                self.queue.read += 1;
                if (self.queue.read >= self.queue.bounds) self.queue.read -= self.queue.bounds;
                self.queue.len -= 1;

                return self.queue.data[n];
            }

            pub fn deinit(self: *DrainIterator) void {
                // If we have consumers waiting on a full queue, notify.
                if (self.queue.not_full_waiters > 0) self.queue.cond_not_full.signal();

                // Unlock
                self.queue.mutex.unlock();
            }
        };

        /// Returns true if the queue is full. This is not public because
        /// it requires the lock to be held.
        inline fn full(self: *Self) bool {
            return self.len == self.bounds;
        }
    };
}

test "basic push and pop" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Should have no values
    try testing.expect(q.pop() == null);

    // Push until we're full
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 2), q.push(2, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 3), q.push(3, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 4), q.push(4, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 0), q.push(5, .{ .instant = {} }));

    // Pop!
    try testing.expect(q.pop().? == 1);
    try testing.expect(q.pop().? == 2);
    try testing.expect(q.pop().? == 3);
    try testing.expect(q.pop().? == 4);
    try testing.expect(q.pop() == null);

    // Drain does nothing
    var it = q.drain();
    try testing.expect(it.next() == null);
    it.deinit();

    // Verify we can still push
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
}

test "timed push" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 1);
    defer q.destroy(alloc);

    // Push
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 0), q.push(2, .{ .instant = {} }));

    // Timed push should fail
    try testing.expectEqual(@as(Q.Size, 0), q.push(2, .{ .ns = 1000 }));
}

test "blockingPop with instant timeout" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Should return null on empty queue with instant timeout
    try testing.expect(q.blockingPop(.{ .instant = {} }) == null);

    // Add an item and verify we can pop it
    try testing.expectEqual(@as(Q.Size, 1), q.push(42, .{ .instant = {} }));
    try testing.expectEqual(@as(u64, 42), q.blockingPop(.{ .instant = {} }).?);

    // Queue should be empty again
    try testing.expect(q.blockingPop(.{ .instant = {} }) == null);
}

test "blockingPop with timed timeout" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Should timeout and return null
    const start = std.time.nanoTimestamp();
    try testing.expect(q.blockingPop(.{ .ns = 10 * std.time.ns_per_ms }) == null);
    const elapsed = std.time.nanoTimestamp() - start;

    // Verify we waited at least some time (allowing for some flexibility)
    try testing.expect(elapsed >= 1 * std.time.ns_per_ms);
}

test "blockingPop with producer thread" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Create a thread that will push a value after a delay
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(queue: *Q) void {
            // Wait a bit before pushing
            std.time.sleep(20 * std.time.ns_per_ms);
            _ = queue.push(99, .{ .instant = {} });
        }
    }.run, .{q});

    // This should block until the other thread pushes a value
    const value = q.blockingPop(.{ .forever = {} });
    try testing.expectEqual(@as(u64, 99), value.?);

    thread.join();
}

test "blockingPop notification when queue becomes non-empty" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    var value_received = std.atomic.Value(bool).init(false);

    // Create a thread that will wait for an item
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(queue: *Q, received: *std.atomic.Value(bool)) void {
            const val = queue.blockingPop(.{ .forever = {} });
            if (val != null and val.? == 123) {
                received.store(true, .release);
            }
        }
    }.run, .{ q, &value_received });

    // Give the consumer thread time to start waiting
    std.time.sleep(10 * std.time.ns_per_ms);

    // Push a value that should wake up the consumer
    _ = q.push(123, .{ .instant = {} });

    // Give the consumer time to process
    std.time.sleep(10 * std.time.ns_per_ms);

    try testing.expect(value_received.load(.acquire));
    consumer.join();
}

test "blockingPop after a full queue is drained" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Fill the queue
    _ = q.push(1, .{ .instant = {} });
    _ = q.push(2, .{ .instant = {} });
    _ = q.push(3, .{ .instant = {} });
    _ = q.push(4, .{ .instant = {} });

    // Should be full now
    try testing.expectEqual(@as(Q.Size, 0), q.push(5, .{ .instant = {} }));

    // Pop one value to make room
    try testing.expectEqual(@as(u64, 1), q.blockingPop(.{ .instant = {} }).?);

    // Now we should be able to push again
    try testing.expectEqual(@as(Q.Size, 4), q.push(5, .{ .instant = {} }));

    // Drain everything and verify the values
    try testing.expectEqual(@as(u64, 2), q.blockingPop(.{ .instant = {} }).?);
    try testing.expectEqual(@as(u64, 3), q.blockingPop(.{ .instant = {} }).?);
    try testing.expectEqual(@as(u64, 4), q.blockingPop(.{ .instant = {} }).?);
    try testing.expectEqual(@as(u64, 5), q.blockingPop(.{ .instant = {} }).?);

    // Queue should be empty again
    try testing.expect(q.blockingPop(.{ .instant = {} }) == null);
}

test "drain empty queue" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Drain empty queue
    var it = q.drain();
    try testing.expect(it.next() == null);
    it.deinit();

    // Verify queue still works after drain
    try testing.expectEqual(@as(Q.Size, 1), q.push(42, .{ .instant = {} }));
    try testing.expectEqual(@as(u64, 42), q.pop().?);
}

test "drain partially filled queue" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Fill queue partially
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 2), q.push(2, .{ .instant = {} }));

    // Drain and verify values
    var it = q.drain();
    try testing.expectEqual(@as(u64, 1), it.next().?);
    try testing.expectEqual(@as(u64, 2), it.next().?);
    try testing.expect(it.next() == null); // No more items
    it.deinit();

    // Queue should be empty now
    try testing.expect(q.pop() == null);
}

test "drain full queue" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Fill queue completely
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 2), q.push(2, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 3), q.push(3, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 4), q.push(4, .{ .instant = {} }));

    // Drain and verify values
    var it = q.drain();
    try testing.expectEqual(@as(u64, 1), it.next().?);
    try testing.expectEqual(@as(u64, 2), it.next().?);
    try testing.expectEqual(@as(u64, 3), it.next().?);
    try testing.expectEqual(@as(u64, 4), it.next().?);
    try testing.expect(it.next() == null);
    it.deinit();

    // After drain, should be able to push again
    try testing.expectEqual(@as(Q.Size, 1), q.push(5, .{ .instant = {} }));
}

test "drain with waiting producer" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 2);
    defer q.destroy(alloc);

    // Fill queue
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 2), q.push(2, .{ .instant = {} }));

    var push_done = std.atomic.Value(bool).init(false);

    // Create a thread that will try to push to the full queue
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(queue: *Q, done: *std.atomic.Value(bool)) void {
            // This will block until the queue has space
            _ = queue.push(3, .{ .forever = {} });
            done.store(true, .release);
        }
    }.run, .{ q, &push_done });

    // Give the producer time to start waiting
    std.time.sleep(10 * std.time.ns_per_ms);

    // At this point, producer should be waiting
    try testing.expect(!push_done.load(.acquire));

    // Drain queue which should free space and unblock producer
    var it = q.drain();
    try testing.expectEqual(@as(u64, 1), it.next().?);
    try testing.expectEqual(@as(u64, 2), it.next().?);
    it.deinit();

    // Give the producer time to complete
    std.time.sleep(10 * std.time.ns_per_ms);

    try testing.expect(push_done.load(.acquire));
    producer.join();

    // Verify the pushed value is in the queue
    try testing.expectEqual(@as(u64, 3), q.pop().?);
}

test "drain with wrap-around buffer" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Create a wrap-around by pushing and popping to advance read/write pointers
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 2), q.push(2, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 3), q.push(3, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 4), q.push(4, .{ .instant = {} }));

    // Pop some values
    try testing.expectEqual(@as(u64, 1), q.pop().?);
    try testing.expectEqual(@as(u64, 2), q.pop().?);

    // Push more values (these should wrap around)
    try testing.expectEqual(@as(Q.Size, 3), q.push(5, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 4), q.push(6, .{ .instant = {} }));

    // Drain should correctly handle the wrap-around
    var it = q.drain();
    try testing.expectEqual(@as(u64, 3), it.next().?);
    try testing.expectEqual(@as(u64, 4), it.next().?);
    try testing.expectEqual(@as(u64, 5), it.next().?);
    try testing.expectEqual(@as(u64, 6), it.next().?);
    try testing.expect(it.next() == null);
    it.deinit();

    // Queue should be empty
    try testing.expect(q.pop() == null);
}

test "interleaved drain and push operations" {
    const testing = std.testing;
    const alloc = testing.allocator;

    const Q = BlockingQueue(u64);
    const q = try Q.create(alloc, 4);
    defer q.destroy(alloc);

    // Initial data
    try testing.expectEqual(@as(Q.Size, 1), q.push(1, .{ .instant = {} }));
    try testing.expectEqual(@as(Q.Size, 2), q.push(2, .{ .instant = {} }));

    // Partial drain
    var it1 = q.drain();
    try testing.expectEqual(@as(u64, 1), it1.next().?);
    it1.deinit();

    // Push more data
    try testing.expectEqual(@as(Q.Size, 2), q.push(3, .{ .instant = {} }));

    // Another partial drain
    var it2 = q.drain();
    try testing.expectEqual(@as(u64, 2), it2.next().?);
    it2.deinit();

    // One more push and final drain
    try testing.expectEqual(@as(Q.Size, 2), q.push(4, .{ .instant = {} }));

    var it3 = q.drain();
    try testing.expectEqual(@as(u64, 3), it3.next().?);
    try testing.expectEqual(@as(u64, 4), it3.next().?);
    try testing.expect(it3.next() == null);
    it3.deinit();
}
