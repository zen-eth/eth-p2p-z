const std = @import("std");
const testing = std.testing;

pub fn Completion1(comptime ValueType: type, comptime ErrorType: type) type {
    return struct {
        done: std.Thread.ResetEvent,
        rwlock: std.Thread.RwLock,
        value: ?ValueType,
        err: ?ErrorType,

        pub const Self = @This();

        pub fn setError(self: *Self, err: ErrorType) void {
            self.rwlock.lock();
            if (self.err == null) {
                self.err = err;
            }
            self.rwlock.unlock();

            self.setDone();
        }

        pub fn setValue(self: *Self, value: ValueType) void {
            self.rwlock.lock();
            if (self.value == null) {
                self.value = value;
            }
            self.rwlock.unlock();

            self.setDone();
        }

        pub fn getErr(self: *Self) ?ErrorType {
            self.rwlock.lockShared();
            defer self.rwlock.unlockShared();

            return self.err;
        }

        pub fn getValue(self: *Self) ?ValueType {
            self.rwlock.lockShared();
            defer self.rwlock.unlockShared();

            return self.value;
        }

        pub fn setDone(self: *Self) void {
            self.done.set();
        }

        pub fn wait(self: *Self) void {
            self.done.wait();
        }

        pub fn timedWait(self: *Self, timeout_ns: u64) error{Timeout}!void {
            return self.done.timedWait(timeout_ns);
        }
    };
}

pub const Completion = struct {
    done: std.Thread.ResetEvent = .{},
    rwlock: std.Thread.RwLock = .{},
    err: ?anyerror = null,

    pub fn setError(self: *Completion, err: anyerror) void {
        self.rwlock.lock();
        if (self.err == null) {
            self.err = err;
        }
        self.rwlock.unlock();

        self.done.set();
    }

    pub fn getErr(self: *Completion) ?anyerror {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();

        return self.err;
    }

    pub fn setDone(self: *Completion) void {
        self.done.set();
    }

    pub fn wait(self: *Completion) void {
        self.done.wait();
    }

    pub fn timedWait(self: *Completion, timeout_ns: u64) error{Timeout}!void {
        return self.done.timedWait(timeout_ns);
    }
};

test "Completion - basic functionality" {
    var completion = Completion{};

    // Test setting done and waiting
    var thread = try std.Thread.spawn(.{}, struct {
        fn run(comp: *Completion) void {
            std.time.sleep(10 * std.time.ns_per_ms);
            comp.setDone();
        }
    }.run, .{&completion});

    completion.wait();
    thread.join();

    try testing.expect(completion.getErr() == null);
}

test "Completion - error handling" {
    var completion = Completion{};

    // Test setting error and checking it
    completion.setError(error.TestError);
    completion.wait(); // Should not block since setError calls done.set()

    try testing.expectEqual(error.TestError, completion.getErr().?);
}

test "Completion - first error only" {
    var completion = Completion{};

    // First error should be stored, second ignored
    completion.setError(error.TestError);
    completion.setError(error.OutOfMemory);

    try testing.expectEqual(error.TestError, completion.getErr().?);
}

test "Completion - timed wait success" {
    var completion = Completion{};

    // Test timed wait with completion occurring before timeout
    var thread = try std.Thread.spawn(.{}, struct {
        fn run(comp: *Completion) void {
            std.time.sleep(10 * std.time.ns_per_ms);
            comp.setDone();
        }
    }.run, .{&completion});

    try completion.timedWait(100 * std.time.ns_per_ms);
    thread.join();
}

test "Completion - timed wait timeout" {
    var completion = Completion{};

    // Test timed wait with timeout occurring before completion
    const result = completion.timedWait(10 * std.time.ns_per_ms);
    try testing.expectError(error.Timeout, result);
}

test "Completion - concurrent access" {
    var completion = Completion{};
    const thread_count = 10;

    // Spawn multiple threads trying to set errors
    var threads: [thread_count]std.Thread = undefined;

    for (0..thread_count) |i| {
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(comp: *Completion, id: usize) void {
                // Each thread tries to set a different error
                // Only the first one should succeed
                if (id == 0) {
                    std.time.sleep(5 * std.time.ns_per_ms); // Slight delay for first thread
                }

                if (id % 2 == 0) {
                    comp.setError(error.TestError);
                } else {
                    comp.setError(error.OutOfMemory);
                }
            }
        }.run, .{ &completion, i });
    }

    // Wait for all threads to complete
    for (&threads) |*thread| {
        thread.join();
    }

    // Wait for completion and check error
    completion.wait();

    // An error should be set, but we don't know which one since threads run concurrently
    try testing.expect(completion.getErr() != null);
}

test "Completion1 - basic functionality" {
    const TestCompletion = Completion1(i32, anyerror);
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

test "Completion1 - error handling" {
    const TestCompletion = Completion1(i32, anyerror);
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

test "Completion1 - first value only" {
    const TestCompletion = Completion1(i32, anyerror);
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

test "Completion1 - first error only" {
    const TestCompletion = Completion1(i32, anyerror);
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

test "Completion1 - timed wait success" {
    const TestCompletion = Completion1(i32, anyerror);
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

test "Completion1 - timed wait timeout" {
    const TestCompletion = Completion1(i32, anyerror);
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

test "Completion1 - concurrent access" {
    const TestCompletion = Completion1(i32, anyerror);
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
