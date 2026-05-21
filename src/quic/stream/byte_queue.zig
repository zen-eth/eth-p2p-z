const std = @import("std");
const channel = @import("../io/channel.zig");

pub const ByteQueue = struct {
    buffer: []u8,
    queue: std.Io.TypeErasedQueue,
    mutex: std.Io.Mutex = .init,
    readable: channel.Signal = .{},
    writable: channel.Signal = .{},
    used_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) std.mem.Allocator.Error!ByteQueue {
        const buffer = try allocator.alloc(u8, @max(@as(usize, 1), capacity));
        return .{
            .buffer = buffer,
            .queue = .init(buffer),
        };
    }

    pub fn close(q: *ByteQueue, io: std.Io) void {
        q.queue.close(io);
        q.signalReadable(io);
        q.signalWritable(io);
    }

    pub fn deinit(q: *ByteQueue, allocator: std.mem.Allocator) void {
        allocator.free(q.buffer);
        q.* = undefined;
    }

    pub fn available(q: *ByteQueue, io: std.Io) usize {
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        return q.buffer.len - q.used_bytes;
    }

    pub fn used(q: *ByteQueue, io: std.Io) usize {
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        return q.used_bytes;
    }

    pub fn tryPutSome(q: *ByteQueue, io: std.Io, bytes: []const u8) usize {
        const len = @min(bytes.len, q.available(io));
        if (len == 0) return 0;
        if (!q.reserve(io, len)) return 0;

        const written = q.queue.putUncancelable(io, bytes[0..len], len) catch {
            q.release(io, len);
            return 0;
        };
        if (written < len) q.release(io, len - written);
        if (written > 0) q.signalReadable(io);
        return written;
    }

    /// All-or-nothing put. Returns true iff every byte in `bytes` was committed.
    ///
    /// Callers must pre-check capacity (e.g., via `available`) under the
    /// single-producer invariant. A partial write here would mean a prefix is
    /// visible to readers while the function reports failure; the queue type
    /// does not provide a transactional rollback, so we panic rather than
    /// silently corrupt the stream byte order.
    pub fn tryPutAll(q: *ByteQueue, io: std.Io, bytes: []const u8) bool {
        if (bytes.len == 0) return true;
        if (!q.reserve(io, bytes.len)) return false;

        const written = q.queue.putUncancelable(io, bytes, bytes.len) catch {
            q.release(io, bytes.len);
            return false;
        };
        if (written != bytes.len) {
            @panic("ByteQueue.tryPutAll: short write after successful reserve");
        }
        q.signalReadable(io);
        return true;
    }

    pub fn tryGet(q: *ByteQueue, io: std.Io, out: []u8) error{Closed}!usize {
        const read = try q.queue.getUncancelable(io, out, 0);
        if (read > 0) q.release(io, read);
        return read;
    }

    pub fn get(q: *ByteQueue, io: std.Io, out: []u8, min: usize) (error{Closed} || std.Io.Cancelable)!usize {
        const read = try q.queue.get(io, out, min);
        if (read > 0) q.release(io, read);
        return read;
    }

    pub fn reserve(q: *ByteQueue, io: std.Io, len: usize) bool {
        q.mutex.lockUncancelable(io);
        defer q.mutex.unlock(io);
        if (len > q.buffer.len - q.used_bytes) return false;
        q.used_bytes += len;
        return true;
    }

    pub fn release(q: *ByteQueue, io: std.Io, len: usize) void {
        q.mutex.lockUncancelable(io);
        q.used_bytes -|= len;
        q.mutex.unlock(io);
        q.signalWritable(io);
    }

    pub fn observeReadable(q: *const ByteQueue) u32 {
        return q.readable.observe();
    }

    pub fn observeWritable(q: *const ByteQueue) u32 {
        return q.writable.observe();
    }

    pub fn waitReadable(q: *ByteQueue, io: std.Io, observed_epoch: u32, deadline: std.Io.Timeout) std.Io.Cancelable!void {
        if (deadline == .none) return q.readable.wait(io, observed_epoch);
        return q.readable.waitTimeout(io, observed_epoch, deadline);
    }

    pub fn waitWritable(q: *ByteQueue, io: std.Io, observed_epoch: u32, deadline: std.Io.Timeout) std.Io.Cancelable!void {
        if (deadline == .none) return q.writable.wait(io, observed_epoch);
        return q.writable.waitTimeout(io, observed_epoch, deadline);
    }

    pub fn signalReadable(q: *ByteQueue, io: std.Io) void {
        q.readable.notify(io);
    }

    pub fn signalWritable(q: *ByteQueue, io: std.Io) void {
        q.writable.notify(io);
    }
};
