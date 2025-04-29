//! A simple implementation of a Future type in Zig.
//! This implementation uses a read-write lock to allow multiple readers and a single writer.
//! It also uses a reset event to signal when the future is done.

const std = @import("std");
const testing = std.testing;

/// A generic Future type that allows asynchronous setting and retrieval of a value or error.
/// This implementation ensures thread safety using a read-write lock and a reset event.
pub fn Future(comptime ValueType: type, comptime ErrorType: type) type {
    return struct {
        /// A reset event used to signal when the Future is completed.
        done: std.Thread.ResetEvent = .{},

        /// A read-write lock to ensure thread-safe access to the value and error.
        rwlock: std.Thread.RwLock = .{},

        /// The value held by the Future, or `null` if not set.
        value: ?ValueType = null,

        /// The error held by the Future, or `null` if not set.
        err: ?ErrorType = null,

        // pub const Self = @This();

        /// Sets an error for the Future. If an error is already set, this call has no effect.
        pub fn setError(self: *Future(ValueType, ErrorType), err: ErrorType) void {
            self.rwlock.lock();
            if (self.err == null) {
                self.err = err;
            }
            self.rwlock.unlock();

            self.setDone();
        }

        /// Sets a value for the Future. If a value is already set, this call has no effect.
        pub fn setValue(self: *Future(ValueType, ErrorType), value: ValueType) void {
            self.rwlock.lock();
            if (self.value == null) {
                self.value = value;
            }
            self.rwlock.unlock();

            self.setDone();
        }

        /// Retrieves the error set in the Future, or `null` if no error is set.
        pub fn getErr(self: *Future(ValueType, ErrorType)) ?ErrorType {
            self.rwlock.lockShared();
            defer self.rwlock.unlockShared();

            return self.err;
        }

        /// Retrieves the value set in the Future, or `null` if no value is set.
        pub fn getValue(self: *Future(ValueType, ErrorType)) ?ValueType {
            self.rwlock.lockShared();
            defer self.rwlock.unlockShared();

            return self.value;
        }

        /// Marks the Future as completed by signaling the reset event.
        pub fn setDone(self: *Future(ValueType, ErrorType)) void {
            self.done.set();
        }

        /// Waits for the Future to be completed. This call blocks until the reset event is signaled.
        pub fn wait(self: *Future(ValueType, ErrorType)) void {
            self.done.wait();
        }

        /// Waits for the Future to be completed with a timeout.
        pub fn timedWait(self: *Future(ValueType, ErrorType), timeout_ns: u64) error{Timeout}!void {
            return self.done.timedWait(timeout_ns);
        }

        /// Checks if the Future is completed.
        pub fn isDone(self: *Future(ValueType, ErrorType)) bool {
            return self.done.isSet();
        }
    };
}

test "Future - basic functionality" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };

    // Test setting value and waiting
    var thread = try std.Thread.spawn(.{}, struct {
        fn run(comp: *TestCompletion) void {
            std.time.sleep(10 * std.time.ns_per_ms);
            comp.setValue(42);
        }
    }.run, .{&completion});

    completion.wait();
    thread.join();

    try testing.expectEqual(@as(i32, 42), completion.getValue().?);
    try testing.expect(completion.getErr() == null);
}

test "Future - error handling" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };

    // Test setting error and checking it
    completion.setError(error.TestError);
    completion.wait(); // Should not block since setError calls setDone()

    try testing.expectEqual(error.TestError, completion.getErr().?);
    try testing.expect(completion.getValue() == null);
}

test "Future - first value only" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };

    // First value should be stored, second ignored
    completion.setValue(42);
    completion.setValue(100);

    try testing.expectEqual(@as(i32, 42), completion.getValue().?);
}

test "Future - first error only" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };

    // First error should be stored, second ignored
    completion.setError(error.TestError);
    completion.setError(error.OutOfMemory);

    try testing.expectEqual(error.TestError, completion.getErr().?);
}

test "Future - timed wait success" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };

    // Test timed wait with completion occurring before timeout
    var thread = try std.Thread.spawn(.{}, struct {
        fn run(comp: *TestCompletion) void {
            std.time.sleep(10 * std.time.ns_per_ms);
            comp.setValue(42);
        }
    }.run, .{&completion});

    try completion.timedWait(100 * std.time.ns_per_ms);
    thread.join();

    try testing.expectEqual(@as(i32, 42), completion.getValue().?);
}

test "Future - timed wait timeout" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };

    // Test timed wait with timeout occurring before completion
    const result = completion.timedWait(10 * std.time.ns_per_ms);
    try testing.expectError(error.Timeout, result);
}

test "Future - concurrent access" {
    const TestCompletion = Future(i32, anyerror);
    var completion = TestCompletion{
        .done = .{},
        .rwlock = .{},
        .value = null,
        .err = null,
    };
    const thread_count = 10;

    // Spawn multiple threads trying to set values
    var threads: [thread_count]std.Thread = undefined;

    for (0..thread_count) |i| {
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(comp: *TestCompletion, id: usize) void {
                if (id == 0) {
                    std.time.sleep(5 * std.time.ns_per_ms); // Slight delay for first thread
                }

                if (id % 2 == 0) {
                    comp.setValue(@intCast(id));
                } else {
                    comp.setError(error.TestError);
                }
            }
        }.run, .{ &completion, i });
    }

    // Wait for all threads to complete
    for (&threads) |*thread| {
        thread.join();
    }

    // Wait for completion and check result
    completion.wait();

    // Either a value or an error should be set
    try testing.expect(completion.getValue() != null or completion.getErr() != null);
}
