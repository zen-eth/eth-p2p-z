const std = @import("std");

pub fn Future(comptime T: type) type {
    return struct {
        const Self = @This();

        value: ?T = null,
        err: ?anyerror = null,
        completed: bool = false,
        mutex: std.Thread.Mutex = .{},
        condition: std.Thread.Condition = .{},
        onSuccess: ?*const fn (T) void = null,
        onError: ?*const fn (anyerror) void = null,

        pub fn init() Self {
            return Self{};
        }

        pub fn listen(self: *Self, on_success: ?*const fn (T) void, on_error: ?*const fn (anyerror) void) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.onSuccess = on_success;
            self.onError = on_error;

            if (self.completed) {
                if (self.value) |v| {
                    if (self.onSuccess) |cb| cb(v);
                } else if (self.err) |e| {
                    if (self.onError) |cb| cb(e);
                }
            }
        }

        pub fn complete(self: *Self, data: T) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.value = data;
            self.completed = true;
            if (self.onSuccess) |cb| cb(data);
            self.condition.signal();
        }

        pub fn completeError(self: *Self, err: anyerror) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.err = err;
            self.completed = true;
            if (self.onError) |cb| cb(err);
            self.condition.signal();
        }

        pub fn wait(self: *Self) !T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (!self.completed) {
                self.condition.wait(&self.mutex);
            }

            if (self.err) |e| return e;
            return self.value.?;
        }
    };
}

test "Future basic completion" {
    var future = Future(u32).init();
    future.complete(42);
    const result = try future.wait();
    try std.testing.expectEqual(@as(u32, 42), result);
}

test "Future error handling" {
    var future = Future([]const u8).init();
    future.completeError(error.OutOfMemory);
    try std.testing.expectError(error.OutOfMemory, future.wait());
}

test "Future async completion" {
    var future = Future([]const u8).init();

    const thread = try std.Thread.spawn(.{}, struct {
        fn run(f: *Future([]const u8)) void {
            std.time.sleep(10 * std.time.ns_per_ms);
            f.complete("done");
        }
    }.run, .{&future});

    const result = try future.wait();
    try std.testing.expectEqualStrings("done", result);
    thread.join();
}

test "Future with callbacks" {
    const TestNamespace = struct {
        var called: bool = false;
    };

    const Context = struct {
        pub fn callback(value: u32) void {
            TestNamespace.called = true;
            std.testing.expectEqual(@as(u32, 123), value) catch unreachable;
        }
    };

    var future = Future(u32).init();
    TestNamespace.called = false;

    future.listen(
        Context.callback,
        null,
    );

    future.complete(123);
    try std.testing.expect(TestNamespace.called);
}

test "Future with error callback" {
    const TestNamespace = struct {
        var called: bool = false;
        var got_error: ?anyerror = null;
    };

    const Context = struct {
        pub fn onError(err: anyerror) void {
            TestNamespace.called = true;
            TestNamespace.got_error = err;
        }
    };

    var future = Future(u32).init();
    TestNamespace.called = false;
    TestNamespace.got_error = null;

    future.listen(
        null,
        Context.onError,
    );

    future.completeError(error.OutOfMemory);
    try std.testing.expect(TestNamespace.called);
    try std.testing.expectEqual(TestNamespace.got_error.?, error.OutOfMemory);
}
