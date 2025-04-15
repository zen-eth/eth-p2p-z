const std = @import("std");
const xev_tcp = @import("xev.zig");
const XevTransport = xev_tcp.XevTransport;
const XevSocketChannel = xev_tcp.XevSocketChannel;
const XevListener = xev_tcp.XevListener;
const conn = @import("../../conn.zig");
const io_loop = @import("../../thread_event_loop.zig");

pub const Transport = union(enum) {
    xev: xev_tcp.Transport,

    pub const DialError = xev_tcp.Transport.DialError;

    pub const ListenError = xev_tcp.Transport.ListenError;

    pub fn dial(self: *Transport, addr: std.net.Address, connection: anytype) DialError!void {
        std.debug.print("Dialing address: {}\n", .{addr});
        switch (self.*) {
            .xev => |*x| return x.dial(addr, connection.context),
        }
    }

    pub fn listen(self: *Transport, addr: std.net.Address, listener: anytype) ListenError!void {
        switch (self.*) {
            .xev => |*x| return x.listen(addr, listener.context),
        }
    }
};

// test "dial connection refused" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     var l: io_loop.ThreadEventLoop = undefined;
//     try l.init(allocator);
//     defer {
//         l.close();
//         l.deinit();
//     }
//
//     const opts = XevTransport.Options{
//         .backlog = 128,
//     };
//
//     var transport: XevTransport = undefined;
//     try transport.init(&l, allocator, opts);
//     defer transport.deinit();
//
//     const xev_transport = transport.toTransport();
//
//     var channel: XevSocketChannel = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8091);
//     var xev_connection = channel.toConn();
//     var tcp_transport = Transport{
//         .xev = xev_transport,
//     };
//     try std.testing.expectError(error.ConnectionRefused, tcp_transport.dial(addr, &xev_connection));
//
//     var channel1: XevSocketChannel = undefined;
//     var xev_connection1 = channel1.toConn();
//     try std.testing.expectError(error.ConnectionRefused, tcp_transport.dial(addr, &xev_connection1));
//
//     const thread2 = try std.Thread.spawn(.{}, struct {
//         fn run(t: *Transport, a: std.net.Address) !void {
//             var ch: XevSocketChannel = undefined;
//             var xev_connection2 = ch.toConn();
//             try std.testing.expectError(error.ConnectionRefused, t.dial(a, &xev_connection2));
//         }
//     }.run, .{ &tcp_transport, addr });
//
//     thread2.join();
// }

test "dial and accept" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator);
    defer {
        sl.close();
        sl.deinit();
    }

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var transport: XevTransport = undefined;
    try transport.init(&sl, allocator, opts);
    defer transport.deinit();
    const xev_transport = transport.toTransport();
    var tcp_transport = Transport{
        .xev = xev_transport,
    };

    var listener: XevListener = undefined;
    const xev_listener = listener.toListener();
    const addr = try std.net.Address.parseIp("0.0.0.0", 8092);
    try tcp_transport.listen(addr, &xev_listener);

    std.debug.print("Listening on: {}\n", .{xev_listener});
    var channel: XevSocketChannel = undefined;
    var xev_connection = channel.toConn();
    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *const xev_tcp.Listener, _: *xev_tcp.Connection) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 2) : (accepted_count += 1) {
                var accepted_connection: xev_tcp.Connection = undefined;
                try l.accept(&accepted_connection);
                // std.debug.print("Accepted connection: {}\n", .{accepted_connection.context.*});
                // try std.testing.expectEqual(accepted_connection.context.direction, .INBOUND);
            }
        }
    }.run, .{ &xev_listener, &xev_connection });

    var cl: io_loop.ThreadEventLoop = undefined;
    try cl.init(allocator);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: XevTransport = undefined;
    try client.init(&cl, allocator, opts);
    defer client.deinit();
    const xev_client = client.toTransport();

    var channel1: XevSocketChannel = undefined;
    const xev_connection1 = channel1.toConn();
    try xev_client.dial(addr, xev_connection1);
    try std.testing.expectEqual(xev_connection1.context.direction, .OUTBOUND);

    var channel2: XevSocketChannel = undefined;
    const xev_connection2 = channel2.toConn();
    try xev_client.dial(addr, xev_connection2);
    std.debug.print("Accepted connection: {}\n", .{xev_connection2.context.*});
    std.debug.print("Accepted channel: {}\n", .{channel2});
    try std.testing.expectEqual(xev_connection2.context.direction, .OUTBOUND);

    accept_thread.join();
    // client.close();
    // transport.close();
}
