const std = @import("std");
const testing = std.testing;

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
