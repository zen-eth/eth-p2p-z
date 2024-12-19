//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;
const uv = @import("libuv");

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    try testing.expect(add(3, 7) == 10);
}

test "libuv loop" {
    var loop: uv.Loop = undefined;
    try loop.Init();
    defer loop.Close() catch {};
    try loop.Run(uv.Loop.RunMode.Once);
}

test "libuv timer" {
    var loop: uv.Loop = undefined;
    var timer: uv.Timer = undefined;
    var count: u64 = 0;
    try loop.Init();

    timer.Init(&loop);
    timer.GetHandle().SetData(&count);

    try timer.Start(MyCallback, 5, 5);

    try loop.Run(.Default);

    // gracefully close timer
    timer.GetHandle().Close(null);
    try loop.Run(.Once);

    try loop.Close();
}

fn MyCallback(timer: *uv.Timer) void {
    const count: *u64 = timer.GetHandle().GetData(u64);
    count.* += 1;

    if (count.* >= 20) {
        timer.Stop() catch unreachable;
    }
}

fn MyConnectionCallback(_: uv.Stream, _: ?uv.Stream.Error) void {
    // Handle connection result
    std.debug.print("Connection result\n", .{});
}

fn MyConnectCallback(_: *uv.Request.Connect, _: ?uv.Stream.Error) void {
    // Handle connection result
    std.debug.print("Connect result\n", .{});
}

test "libuv tcp" {
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
