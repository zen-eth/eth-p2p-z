const std = @import("std");

const Impl = struct {
    pool: ?*Pool = null,
    slot_index: usize = 0,
    data: []u8 = &.{},
};

pub const Datagram = opaque {
    fn impl(d: *Datagram) *Impl {
        return @ptrCast(@alignCast(d));
    }

    fn constImpl(d: *const Datagram) *const Impl {
        return @ptrCast(@alignCast(d));
    }

    pub fn bytes(d: *const Datagram) []const u8 {
        return d.constImpl().data;
    }

    pub fn mutableBytes(d: *Datagram) []u8 {
        return d.impl().data;
    }

    pub fn setLen(d: *Datagram, len: usize) void {
        const data = d.impl();
        // `len` may only narrow within the slice the slot currently holds (the
        // length it was acquired with) — the handle has no access to the slot's
        // full backing buffer, so re-slicing past `data.len` is out of bounds.
        std.debug.assert(len <= data.data.len);
        data.data = data.data[0..len];
    }

    pub fn release(d: *Datagram) void {
        const data = d.impl();
        // Idempotent on the same handle instance: a second release is a no-op.
        // Note: retaining a `*Datagram` past `release` (so that a subsequent
        // `acquire` returns the same slot to another caller) is undefined behavior.
        const pool = data.pool orelse return;
        data.pool = null;
        pool.release(data.slot_index);
    }
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    slots: []Slot,
    storage: []u8,
    free_stack: []usize,
    free_len: usize,
    slot_size: usize,
    outstanding: usize = 0,
    closing: bool = false,
    destroy_started: bool = false,

    const Slot = struct {
        datagram: Impl = .{},
        in_use: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, io: std.Io, slots_count: usize, slot_size: usize) std.mem.Allocator.Error!*Pool {
        const pool = try allocator.create(Pool);
        errdefer allocator.destroy(pool);

        const slots = try allocator.alloc(Slot, slots_count);
        errdefer allocator.free(slots);

        const storage = try allocator.alloc(u8, slots_count * slot_size);
        errdefer allocator.free(storage);

        const free_stack = try allocator.alloc(usize, slots_count);
        errdefer allocator.free(free_stack);

        for (free_stack, 0..) |*entry, i| entry.* = slots_count - 1 - i;
        for (slots) |*slot| slot.* = .{};

        pool.* = .{
            .allocator = allocator,
            .io = io,
            .slots = slots,
            .storage = storage,
            .free_stack = free_stack,
            .free_len = slots_count,
            .slot_size = slot_size,
        };
        return pool;
    }

    pub fn close(pool: *Pool) void {
        var should_destroy = false;
        pool.mutex.lockUncancelable(pool.io);
        pool.closing = true;
        if (pool.outstanding == 0 and !pool.destroy_started) {
            pool.destroy_started = true;
            should_destroy = true;
        }
        pool.mutex.unlock(pool.io);

        if (should_destroy) pool.destroy();
    }

    pub fn acquire(pool: *Pool, len: usize) ?*Datagram {
        if (len > pool.slot_size) return null;

        pool.mutex.lockUncancelable(pool.io);
        defer pool.mutex.unlock(pool.io);

        if (pool.closing or pool.free_len == 0) return null;

        pool.free_len -= 1;
        const index = pool.free_stack[pool.free_len];
        const slot = &pool.slots[index];
        std.debug.assert(!slot.in_use);

        const start = index * pool.slot_size;
        const buf = pool.storage[start..][0..pool.slot_size];
        // The slot backs `slot_size` bytes; the returned Datagram is narrowed to
        // the requested `len`. `setLen` may only re-narrow within that `len`.
        slot.* = .{
            .datagram = .{
                .pool = pool,
                .slot_index = index,
                .data = buf[0..len],
            },
            .in_use = true,
        };
        pool.outstanding += 1;
        return @ptrCast(&slot.datagram);
    }

    pub fn slotSize(pool: *const Pool) usize {
        return pool.slot_size;
    }

    fn release(pool: *Pool, index: usize) void {
        var should_destroy = false;

        pool.mutex.lockUncancelable(pool.io);
        if (index < pool.slots.len) {
            const slot = &pool.slots[index];
            if (slot.in_use) {
                slot.in_use = false;
                // Leave slot.datagram untouched: the public `Datagram.release` already
                // nulled the handle's `pool` field, and the next `acquire` will
                // overwrite the slot fully. Re-initializing here would re-set the
                // `pool` pointer on the user's stale handle and defeat the
                // idempotency check at the public entrypoint.
                pool.free_stack[pool.free_len] = index;
                pool.free_len += 1;
                pool.outstanding -= 1;
            }
        }
        if (pool.closing) {
            if (pool.outstanding == 0) {
                if (!pool.destroy_started) {
                    pool.destroy_started = true;
                    should_destroy = true;
                }
            }
        }
        pool.mutex.unlock(pool.io);

        if (should_destroy) pool.destroy();
    }

    fn destroy(pool: *Pool) void {
        const allocator = pool.allocator;
        allocator.free(pool.free_stack);
        allocator.free(pool.storage);
        allocator.free(pool.slots);
        allocator.destroy(pool);
    }
};

test "datagram pool bounds slots and reuses released storage" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const pool = try Pool.init(allocator, io, 1, 8);
    const first = pool.acquire(4) orelse return error.TestUnexpectedResult;
    try std.testing.expect(pool.acquire(1) == null);
    @memcpy(first.mutableBytes(), "ping");
    try std.testing.expectEqualStrings("ping", first.bytes());
    first.release();

    const second = pool.acquire(8) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 8), second.bytes().len);
    second.release();
    pool.close();
}
