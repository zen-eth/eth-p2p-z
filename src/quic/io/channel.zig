const std = @import("std");

pub const Signal = struct {
    epoch: std.atomic.Value(u32) = .init(0),

    pub fn observe(s: *const Signal) u32 {
        return s.epoch.load(.acquire);
    }

    pub fn unchanged(s: *const Signal, observed: u32) bool {
        return s.observe() == observed;
    }

    pub fn wait(s: *Signal, io: std.Io, observed: u32) std.Io.Cancelable!void {
        return io.futexWait(u32, &s.epoch.raw, observed);
    }

    pub fn waitTimeout(s: *Signal, io: std.Io, observed: u32, timeout: std.Io.Timeout) std.Io.Cancelable!void {
        return io.futexWaitTimeout(u32, &s.epoch.raw, observed, timeout);
    }

    pub fn notify(s: *Signal, io: std.Io) void {
        _ = s.epoch.fetchAdd(1, .release);
        io.futexWake(u32, &s.epoch.raw, std.math.maxInt(u32));
    }
};

pub fn Bounded(
    comptime T: type,
    // When present, failed sends consume the item and discardQueued/release drop queued items.
    // The channel passes its `io` so cleanups can signal events, take mutexes, etc.
    comptime drop_fn: ?*const fn (*T, std.Io) void,
    comptime cost_fn: ?*const fn (*const T) usize,
) type {
    return struct {
        pub const State = struct {
            allocator: std.mem.Allocator,
            io: std.Io,
            slots: []T,
            queue: std.Io.Queue(T),
            ready: Signal = .{},
            writable: Signal = .{},
            meta_mutex: std.Io.Mutex = .init,
            closed: bool = false,
            byte_capacity: usize = 0,
            used_bytes: usize = 0,
            refs: std.atomic.Value(usize) = .init(1),

            pub fn init(allocator: std.mem.Allocator, io: std.Io, capacity: usize, byte_capacity: usize) std.mem.Allocator.Error!*State {
                const state = try allocator.create(State);
                errdefer allocator.destroy(state);
                const slots = try allocator.alloc(T, @max(@as(usize, 1), capacity));
                state.* = .{
                    .allocator = allocator,
                    .io = io,
                    .slots = slots,
                    .queue = .init(slots),
                    .byte_capacity = byte_capacity,
                };
                return state;
            }

            pub fn sender(s: *State) Sender {
                return .{ .state = s };
            }

            pub fn receiver(s: *State) Receiver {
                return .{ .state = s };
            }

            pub fn close(s: *State, io: std.Io) void {
                s.meta_mutex.lockUncancelable(io);
                if (s.closed) {
                    s.meta_mutex.unlock(io);
                    return;
                }
                s.closed = true;
                s.meta_mutex.unlock(io);

                s.queue.close(io);
                s.notify(io);
                s.writable.notify(io);
            }

            pub fn discardQueued(s: *State, io: std.Io) void {
                var item: T = undefined;
                while (s.tryRecv(io, &item)) dropItem(&item, io);
            }

            pub fn retain(s: *State) void {
                const previous = s.refs.fetchAdd(1, .acq_rel);
                std.debug.assert(previous > 0);
            }

            pub fn release(s: *State) void {
                const previous = s.refs.fetchSub(1, .acq_rel);
                std.debug.assert(previous > 0);
                if (previous != 1) return;

                const allocator = s.allocator;
                s.discardQueued(s.io);
                allocator.free(s.slots);
                allocator.destroy(s);
            }

            pub fn trySend(s: *State, io: std.Io, item: T) bool {
                const item_cost = costOf(&item);
                if (!s.reserve(io, item_cost)) {
                    var dropped = item;
                    dropItem(&dropped, io);
                    return false;
                }

                const written = s.queue.putUncancelable(io, &.{item}, 0) catch {
                    s.releaseCost(io, item_cost);
                    var dropped = item;
                    dropItem(&dropped, io);
                    return false;
                };
                if (written != 1) {
                    s.releaseCost(io, item_cost);
                    var dropped = item;
                    dropItem(&dropped, io);
                    return false;
                }

                s.notify(io);
                return true;
            }

            pub const SendError = error{ Closed, ItemTooLarge } || std.Io.Cancelable;

            pub fn send(s: *State, io: std.Io, item: T) SendError!void {
                errdefer {
                    var dropped = item;
                    dropItem(&dropped, io);
                }

                const item_cost = costOf(&item);
                try s.reserveBlocking(io, item_cost);
                errdefer s.releaseCost(io, item_cost);

                s.queue.putOne(io, item) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.Closed => return error.Closed,
                };
                s.notify(io);
            }

            pub fn sendUncancelable(s: *State, io: std.Io, item: T) error{ Closed, ItemTooLarge }!void {
                errdefer {
                    var dropped = item;
                    dropItem(&dropped, io);
                }

                const item_cost = costOf(&item);
                try s.reserveBlockingUncancelable(io, item_cost);
                errdefer s.releaseCost(io, item_cost);

                s.queue.putOneUncancelable(io, item) catch |err| switch (err) {
                    error.Closed => return error.Closed,
                };
                s.notify(io);
            }

            pub fn tryRecv(s: *State, io: std.Io, out: *T) bool {
                var buf: [1]T = undefined;
                const read = s.queue.getUncancelable(io, &buf, 0) catch return false;
                if (read == 0) return false;

                out.* = buf[0];
                s.releaseCost(io, costOf(out));
                return true;
            }

            pub const RecvError = error{Closed} || std.Io.Cancelable;

            pub fn recv(s: *State, io: std.Io) RecvError!T {
                const item = s.queue.getOne(io) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.Closed => return error.Closed,
                };
                s.releaseCost(io, costOf(&item));
                return item;
            }

            pub fn observe(s: *const State) u32 {
                return s.ready.observe();
            }

            pub fn wait(s: *State, io: std.Io, observed: u32) std.Io.Cancelable!void {
                return s.ready.wait(io, observed);
            }

            pub fn waitTimeout(s: *State, io: std.Io, observed: u32, timeout: std.Io.Timeout) std.Io.Cancelable!void {
                return s.ready.waitTimeout(io, observed, timeout);
            }

            pub fn unchanged(s: *const State, observed: u32) bool {
                return s.ready.unchanged(observed);
            }

            pub fn notify(s: *State, io: std.Io) void {
                s.ready.notify(io);
            }

            fn reserve(s: *State, io: std.Io, item_cost: usize) bool {
                s.meta_mutex.lockUncancelable(io);
                defer s.meta_mutex.unlock(io);
                if (s.closed) return false;
                if (s.byte_capacity > 0 and item_cost > s.byte_capacity -| s.used_bytes) return false;
                s.used_bytes += item_cost;
                return true;
            }

            fn reserveBlocking(s: *State, io: std.Io, item_cost: usize) SendError!void {
                if (s.byte_capacity == 0 or item_cost == 0) {
                    s.meta_mutex.lockUncancelable(io);
                    defer s.meta_mutex.unlock(io);
                    if (s.closed) return error.Closed;
                    return;
                }
                if (item_cost > s.byte_capacity) return error.ItemTooLarge;

                while (true) {
                    s.meta_mutex.lockUncancelable(io);
                    if (s.closed) {
                        s.meta_mutex.unlock(io);
                        return error.Closed;
                    }
                    if (item_cost <= s.byte_capacity -| s.used_bytes) {
                        s.used_bytes += item_cost;
                        s.meta_mutex.unlock(io);
                        return;
                    }
                    const observed = s.writable.observe();
                    s.meta_mutex.unlock(io);
                    try s.writable.wait(io, observed);
                }
            }

            fn reserveBlockingUncancelable(s: *State, io: std.Io, item_cost: usize) error{ Closed, ItemTooLarge }!void {
                if (s.byte_capacity == 0 or item_cost == 0) {
                    s.meta_mutex.lockUncancelable(io);
                    defer s.meta_mutex.unlock(io);
                    if (s.closed) return error.Closed;
                    return;
                }
                if (item_cost > s.byte_capacity) return error.ItemTooLarge;

                while (true) {
                    s.meta_mutex.lockUncancelable(io);
                    if (s.closed) {
                        s.meta_mutex.unlock(io);
                        return error.Closed;
                    }
                    if (item_cost <= s.byte_capacity -| s.used_bytes) {
                        s.used_bytes += item_cost;
                        s.meta_mutex.unlock(io);
                        return;
                    }
                    const observed = s.writable.observe();
                    s.meta_mutex.unlock(io);
                    s.writable.wait(io, observed) catch |err| switch (err) {
                        error.Canceled => io.recancel(),
                    };
                }
            }

            fn releaseCost(s: *State, io: std.Io, item_cost: usize) void {
                if (item_cost == 0) return;
                s.meta_mutex.lockUncancelable(io);
                s.used_bytes -|= item_cost;
                s.meta_mutex.unlock(io);
                s.writable.notify(io);
            }
        };

        pub const Sender = struct {
            state: *State,

            pub fn trySend(sender: Sender, io: std.Io, item: T) bool {
                return sender.state.trySend(io, item);
            }

            pub fn send(sender: Sender, io: std.Io, item: T) State.SendError!void {
                return sender.state.send(io, item);
            }

            pub fn sendUncancelable(sender: Sender, io: std.Io, item: T) error{ Closed, ItemTooLarge }!void {
                return sender.state.sendUncancelable(io, item);
            }

            pub fn sameChannel(sender: Sender, other: Sender) bool {
                return sender.state == other.state;
            }
        };

        pub const Receiver = struct {
            state: *State,

            pub fn tryRecv(receiver: Receiver, io: std.Io, out: *T) bool {
                return receiver.state.tryRecv(io, out);
            }

            pub fn recv(receiver: Receiver, io: std.Io) State.RecvError!T {
                return receiver.state.recv(io);
            }

            pub fn close(receiver: Receiver, io: std.Io) void {
                receiver.state.close(io);
            }

            pub fn discardQueued(receiver: Receiver, io: std.Io) void {
                receiver.state.discardQueued(io);
            }
        };

        fn costOf(item: *const T) usize {
            return if (cost_fn) |func| func(item) else 0;
        }

        fn dropItem(item: *T, io: std.Io) void {
            if (drop_fn) |func| func(item, io);
        }
    };
}

pub fn Unbounded(
    comptime T: type,
    // When present, failed sends consume the item and discardQueued/release drop queued items.
    // The channel passes its `io` so cleanups can signal events, take mutexes, etc.
    comptime drop_fn: ?*const fn (*T, std.Io) void,
) type {
    return struct {
        pub const State = struct {
            allocator: std.mem.Allocator,
            io: std.Io,
            meta_mutex: std.Io.Mutex = .init,
            head: ?*Node = null,
            tail: ?*Node = null,
            ready: Signal = .{},
            closed: bool = false,
            refs: std.atomic.Value(usize) = .init(1),

            const Node = struct {
                item: T,
                next: ?*Node = null,
            };

            pub fn init(allocator: std.mem.Allocator, io: std.Io) std.mem.Allocator.Error!*State {
                const state = try allocator.create(State);
                state.* = .{ .allocator = allocator, .io = io };
                return state;
            }

            pub fn sender(s: *State) Sender {
                return .{ .state = s };
            }

            pub fn receiver(s: *State) Receiver {
                return .{ .state = s };
            }

            pub fn open(s: *State, io: std.Io) void {
                s.meta_mutex.lockUncancelable(io);
                s.closed = false;
                s.meta_mutex.unlock(io);
                s.notify(io);
            }

            pub fn close(s: *State, io: std.Io) void {
                s.meta_mutex.lockUncancelable(io);
                if (s.closed) {
                    s.meta_mutex.unlock(io);
                    return;
                }
                s.closed = true;
                s.meta_mutex.unlock(io);
                s.notify(io);
            }

            pub fn discardQueued(s: *State, io: std.Io) void {
                s.meta_mutex.lockUncancelable(io);
                const head = s.head;
                s.head = null;
                s.tail = null;
                s.meta_mutex.unlock(io);
                s.freeList(head, io);
            }

            pub fn retain(s: *State) void {
                const previous = s.refs.fetchAdd(1, .acq_rel);
                std.debug.assert(previous > 0);
            }

            pub fn release(s: *State) void {
                const previous = s.refs.fetchSub(1, .acq_rel);
                std.debug.assert(previous > 0);
                if (previous != 1) return;

                const allocator = s.allocator;
                s.freeList(s.head, s.io);
                allocator.destroy(s);
            }

            pub fn trySend(s: *State, io: std.Io, item: T) std.mem.Allocator.Error!bool {
                const node = try s.allocator.create(Node);
                node.* = .{ .item = item };

                s.meta_mutex.lockUncancelable(io);
                if (s.closed) {
                    s.meta_mutex.unlock(io);
                    s.allocator.destroy(node);
                    var dropped = item;
                    dropItem(&dropped, io);
                    return false;
                }
                if (s.tail) |tail| {
                    tail.next = node;
                } else {
                    s.head = node;
                }
                s.tail = node;
                s.meta_mutex.unlock(io);

                s.notify(io);
                return true;
            }

            pub fn tryRecv(s: *State, io: std.Io) ?T {
                s.meta_mutex.lockUncancelable(io);
                const node = s.head orelse {
                    s.meta_mutex.unlock(io);
                    return null;
                };
                s.head = node.next;
                if (s.head == null) s.tail = null;
                s.meta_mutex.unlock(io);

                const item = node.item;
                s.allocator.destroy(node);
                return item;
            }

            pub fn observe(s: *const State) u32 {
                return s.ready.observe();
            }

            pub fn wait(s: *State, io: std.Io, observed: u32) std.Io.Cancelable!void {
                return s.ready.wait(io, observed);
            }

            pub fn waitTimeout(s: *State, io: std.Io, observed: u32, timeout: std.Io.Timeout) std.Io.Cancelable!void {
                return s.ready.waitTimeout(io, observed, timeout);
            }

            pub fn unchanged(s: *const State, observed: u32) bool {
                return s.ready.unchanged(observed);
            }

            pub fn notify(s: *State, io: std.Io) void {
                s.ready.notify(io);
            }

            fn freeList(s: *State, head: ?*Node, io: std.Io) void {
                var current = head;
                while (current) |node| {
                    current = node.next;
                    var item = node.item;
                    dropItem(&item, io);
                    s.allocator.destroy(node);
                }
            }
        };

        pub const Sender = struct {
            state: *State,

            pub fn trySend(sender: Sender, io: std.Io, item: T) std.mem.Allocator.Error!bool {
                return sender.state.trySend(io, item);
            }

            pub fn sameChannel(sender: Sender, other: Sender) bool {
                return sender.state == other.state;
            }
        };

        pub const Receiver = struct {
            state: *State,

            pub fn tryRecv(receiver: Receiver, io: std.Io) ?T {
                return receiver.state.tryRecv(io);
            }

            pub fn close(receiver: Receiver, io: std.Io) void {
                receiver.state.close(io);
            }

            pub fn discardQueued(receiver: Receiver, io: std.Io) void {
                receiver.state.discardQueued(io);
            }
        };

        fn dropItem(item: *T, io: std.Io) void {
            if (drop_fn) |func| func(item, io);
        }
    };
}

test "bounded channel preserves FIFO order" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const IntChannel = Bounded(u8, null, null);
    const state = try IntChannel.State.init(std.testing.allocator, io, 2, 0);
    defer state.release();
    defer state.close(io);

    const sender = state.sender();
    const receiver = state.receiver();
    try std.testing.expect(sender.trySend(io, 1));
    try std.testing.expect(sender.trySend(io, 2));
    try std.testing.expect(!sender.trySend(io, 3));

    var out: u8 = 0;
    try std.testing.expect(receiver.tryRecv(io, &out));
    try std.testing.expectEqual(@as(u8, 1), out);
    try std.testing.expect(receiver.tryRecv(io, &out));
    try std.testing.expectEqual(@as(u8, 2), out);
}

test "bounded channel releases byte accounting on blocking receive" {
    const SliceChannel = Bounded([]const u8, null, struct {
        fn cost(item: *const []const u8) usize {
            return item.len;
        }
    }.cost);

    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try SliceChannel.State.init(std.testing.allocator, io, 2, 3);
    defer state.release();
    defer state.close(io);

    const sender = state.sender();
    const receiver = state.receiver();
    try std.testing.expect(sender.trySend(io, "ab"));
    try std.testing.expect(sender.trySend(io, "c"));
    try std.testing.expect(!sender.trySend(io, "d"));

    try std.testing.expectEqualStrings("ab", try receiver.recv(io));
    try std.testing.expect(sender.trySend(io, "d"));
}

test "bounded channel release drops queued owned items" {
    const DropChannel = Bounded(*usize, struct {
        fn drop(item: **usize, _: std.Io) void {
            item.*.* += 1;
        }
    }.drop, null);

    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try DropChannel.State.init(std.testing.allocator, io, 1, 0);
    var drops: usize = 0;
    try std.testing.expect(state.sender().trySend(io, &drops));

    state.close(io);
    state.release();
    try std.testing.expectEqual(@as(usize, 1), drops);
}

test "bounded channel blocking send drops owned item on close" {
    const DropChannel = Bounded(*usize, struct {
        fn drop(item: **usize, _: std.Io) void {
            item.*.* += 1;
        }
    }.drop, null);

    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try DropChannel.State.init(std.testing.allocator, io, 1, 0);
    defer state.release();

    state.close(io);
    var drops: usize = 0;
    try std.testing.expectError(error.Closed, state.sender().send(io, &drops));
    try std.testing.expectEqual(@as(usize, 1), drops);

    try std.testing.expectError(error.Closed, state.sender().sendUncancelable(io, &drops));
    try std.testing.expectEqual(@as(usize, 2), drops);
}

test "bounded channel blocking send drops owned item when too large" {
    const DropChannel = Bounded(*usize, struct {
        fn drop(item: **usize, _: std.Io) void {
            item.*.* += 1;
        }
    }.drop, struct {
        fn cost(_: *const *usize) usize {
            return 2;
        }
    }.cost);

    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try DropChannel.State.init(std.testing.allocator, io, 1, 1);
    defer state.release();
    defer state.close(io);

    var drops: usize = 0;
    try std.testing.expectError(error.ItemTooLarge, state.sender().send(io, &drops));
    try std.testing.expectEqual(@as(usize, 1), drops);

    try std.testing.expectError(error.ItemTooLarge, state.sender().sendUncancelable(io, &drops));
    try std.testing.expectEqual(@as(usize, 2), drops);
}

test "unbounded channel preserves FIFO order" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const IntChannel = Unbounded(u8, null);
    const state = try IntChannel.State.init(std.testing.allocator, io);
    defer state.release();
    defer state.close(io);

    const sender = state.sender();
    const receiver = state.receiver();
    try std.testing.expect(try sender.trySend(io, 1));
    try std.testing.expect(try sender.trySend(io, 2));
    try std.testing.expectEqual(@as(u8, 1), receiver.tryRecv(io).?);
    try std.testing.expectEqual(@as(u8, 2), receiver.tryRecv(io).?);
}
