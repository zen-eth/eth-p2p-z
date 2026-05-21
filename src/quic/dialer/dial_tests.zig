const std = @import("std");
const support = @import("../endpoint/test_support.zig");

const AcceptCtx = support.AcceptCtx;
const TwoEndpoints = support.TwoEndpoints;
const closeStreamForTest = support.closeStreamForTest;
const default_handshake_timeout_ns = support.default_handshake_timeout_ns;
const receiveTimeout = support.receiveTimeout;

test "client dial routes inbound packets via the shared listen socket" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture.deinit();

    const server_addr = try fixture.bindServerLoopback();
    const client_addr = try fixture.bindClientLoopback();

    var accept_ctx = AcceptCtx{ .endpoint = fixture.server };
    const accept_thread = try std.Thread.spawn(.{}, AcceptCtx.run, .{&accept_ctx});

    const client_conn = try fixture.client.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
    });
    defer client_conn.deinit();

    accept_thread.join();
    if (accept_ctx.err) |err| return err;
    const server_conn = accept_ctx.conn orelse return error.TestExpectedEqual;
    defer server_conn.deinit();

    try std.testing.expect(fixture.client.stats().cid_map_entries > 0);

    const observed_client_addr = server_conn.remoteAddress();
    try std.testing.expectEqual(client_addr.getPort(), observed_client_addr.getPort());

    {
        const outbound = try client_conn.openStream(io);
        defer outbound.deinit();
        defer closeStreamForTest(io, outbound);
        try outbound.writeAll(io, "shared-socket payload", .{});
        const inbound = try server_conn.acceptStream(io, .{
            .timeout = support.timeout_s(1),
        });
        defer inbound.deinit();
        defer closeStreamForTest(io, inbound);
        var buf: [64]u8 = undefined;
        try inbound.readAll(io, buf[0..21], .{});
        try std.testing.expectEqualStrings("shared-socket payload", buf[0..21]);
    }
}

test "client dial without bind returns EndpointNotBound" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture.deinit();

    const server_addr = try fixture.bindServerLoopback();

    try std.testing.expectError(error.EndpointNotBound, fixture.client.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
    }));
}
