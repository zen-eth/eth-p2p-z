//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");
const uv = @import("libuv");

pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});

    // stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    try stdout.print("Run `zig build test` to run the tests.\n", .{});

    try bw.flush(); // Don't forget to flush!

    var loop: uv.Loop = undefined;
    var server: uv.Tcp = undefined;
    var client: uv.Tcp = undefined;
    try loop.Init();

    try server.Init(&loop);
    try client.Init(&loop);

    const connectionCallback: uv.Stream.Callback.Connection = MyConnectionCallback;
    var sa = try uv.SocketAddress.FromString("127.0.0.1", 8080);
    server.Bind(&sa, false) catch unreachable;
    server.GetStream().Listen(128, connectionCallback) catch unreachable;
    var connect_req = uv.Request.Connect{};
    const connectCallback: uv.Stream.Callback.Connect = MyConnectCallback;
    client.Connect(&connect_req, &sa, connectCallback) catch unreachable;

    // Run the event loop once to process the connection
    try loop.Run(.Once);

    // Close handles before closing the loop
    server.GetHandle().Close(null);
    client.GetHandle().Close(null);

    // Run again to process close events
    try loop.Run(.Once);

    try loop.Close();
}

fn MyConnectionCallback(_: uv.Stream, _: ?uv.Stream.Error) void {
    // Handle connection result
    std.debug.print("Connection result\n", .{});
}

fn MyConnectCallback(_: *uv.Request.Connect, _: ?uv.Stream.Error) void {
    // Handle connection result
    std.debug.print("Connect result\n", .{});
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // Try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    const global = struct {
        fn testOne(input: []const u8) anyerror!void {
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(global.testOne, .{});
}
