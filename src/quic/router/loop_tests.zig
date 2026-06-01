const std = @import("std");
const ssl = @import("ssl").c;
const support = @import("../endpoint/test_support.zig");

const AcceptCtx = support.AcceptCtx;
const TwoEndpoints = support.TwoEndpoints;
const closeStreamForTest = support.closeStreamForTest;
const default_handshake_timeout_ns = support.default_handshake_timeout_ns;
const local_conn_id_len = support.local_conn_id_len;
const LocalCid = support.LocalCid;
const makeUnsupportedVersionInitial = support.makeUnsupportedVersionInitial;
const QuicEndpoint = support.QuicEndpoint;
const Connection = support.Connection;
const receiveTimeout = support.receiveTimeout;
const waitCidMapCount = support.waitCidMapCount;

test "router emits version negotiation, drops unknown DCID, validates retry tokens" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try TwoEndpoints.init(allocator, io, .{ .endpoint = .{ .connection_accept_queue_len = 1 } }, .{});
    defer fixture.deinit();

    const server_addr = try fixture.bindServerLoopback();
    _ = try fixture.bindClientLoopback();

    const raw_bind: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const raw_socket = try std.Io.net.IpAddress.bind(&raw_bind, io, .{ .mode = .dgram });
    defer raw_socket.close(io);
    var server_addr_mut = server_addr;
    var empty_datagram_storage: [1]u8 = undefined;
    try raw_socket.send(io, &server_addr_mut, empty_datagram_storage[0..0]);

    // Pad to RFC 9000 §14.1 minimum so the server emits Version Negotiation;
    // shorter probes are dropped to prevent reflection amplification.
    var unsupported_initial: [1200]u8 = undefined;
    const unsupported_initial_len = makeUnsupportedVersionInitial(&unsupported_initial);
    try raw_socket.send(io, &server_addr_mut, unsupported_initial[0..unsupported_initial_len]);
    var version_negotiation_buf: [1500]u8 = undefined;
    const version_negotiation = try raw_socket.receiveTimeout(
        io,
        &version_negotiation_buf,
        support.timeout_s(1),
    );
    try std.testing.expect(version_negotiation.data.len >= 7);
    try std.testing.expect((version_negotiation.data[0] & 0x80) != 0);
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, version_negotiation.data[1..5], .big));

    var unknown_short: [1 + local_conn_id_len + 8]u8 = undefined;
    unknown_short[0] = 0x40;
    var unknown_dcid: LocalCid = undefined;
    if (ssl.RAND_bytes(&unknown_dcid, unknown_dcid.len) != 1) return error.TestUnexpectedResult;
    @memcpy(unknown_short[1..][0..unknown_dcid.len], &unknown_dcid);
    @memset(unknown_short[1 + local_conn_id_len ..], 0);
    try raw_socket.send(io, &server_addr_mut, &unknown_short);

    // Trigger Retry path via a dial that fails the peer-key check. The router
    // still validates the retry token round-trip even though we reject the
    // peer post-handshake. This test is about router behavior, so don't block
    // on the accept queue here; listener teardown will drain any queued
    // connection.
    var wrong_pub_key = try fixture.client_host.publicKey(allocator);
    defer if (wrong_pub_key.data) |data| allocator.free(data);

    try std.testing.expectError(error.PeerIdentityMismatch, fixture.client.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
        .expected_peer_key = &wrong_pub_key,
    }));

    const stats = fixture.server.stats();
    try std.testing.expect(stats.router_version_negotiation_sent > 0);
    try std.testing.expect(stats.router_unknown_dcid_packets > 0);
    try std.testing.expect(stats.router_retry_sent > 0);
    try std.testing.expect(stats.router_retry_token_validated > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.router_retry_token_invalid);
}

test "server shared UDP socket routes packets by destination connection id" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture_a = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture_a.deinit();
    // Second client uses its own TLS context.
    var fixture_b_only_client = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture_b_only_client.deinit();
    // We will just use fixture_a.server and treat both fixture_a.client and
    // fixture_b_only_client.client as independent dialers.
    const server_endpoint = fixture_a.server;
    const client_endpoint_a = fixture_a.client;
    const client_endpoint_b = fixture_b_only_client.client;

    const server_addr = try fixture_a.bindServerLoopback();
    _ = try fixture_a.bindClientLoopback();
    _ = try fixture_b_only_client.bindClientLoopback();

    var accept_a = AcceptCtx{ .endpoint = server_endpoint };
    const accept_thread_a = try std.Thread.spawn(.{}, AcceptCtx.run, .{&accept_a});
    const client_conn_a = try client_endpoint_a.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
    });
    var client_conn_a_live = true;
    errdefer if (client_conn_a_live) client_conn_a.deinit();
    accept_thread_a.join();
    if (accept_a.err) |err| return err;
    const server_conn_a = accept_a.conn orelse return error.TestExpectedEqual;
    var server_conn_a_live = true;
    errdefer if (server_conn_a_live) server_conn_a.deinit();

    const routed_before_b = server_endpoint.stats().router_packets_dispatched;
    const client_conn_b = try client_endpoint_b.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
    });
    try std.testing.expect(server_endpoint.stats().router_packets_dispatched > routed_before_b);
    var client_conn_b_live = true;
    errdefer if (client_conn_b_live) client_conn_b.deinit();

    const outbound_b = try client_conn_b.openStream(io);
    defer outbound_b.deinit();
    defer closeStreamForTest(io, outbound_b);
    const payload = "packet for second connection";
    try outbound_b.writeAll(io, payload, .{});

    try std.testing.expectError(error.Timeout, server_conn_a.acceptStream(io, .{
        .timeout = support.timeout_ms(100),
    }));

    client_conn_a.deinit();
    client_conn_a_live = false;
    client_conn_b.deinit();
    client_conn_b_live = false;
    server_conn_a.deinit();
    server_conn_a_live = false;
}

test "ipv6 unspecified listener accepts ipv4 dial via dual-stack socket" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture.deinit();

    const server_addr = fixture.server.bind(.{ .ip6 = .unspecified(0) }) catch |err| switch (err) {
        // Configuring a dual-stack socket needs the IPV6 socket-option family.
        // That family is `void` in Zig 0.16 `std.posix` on macOS, so the std
        // backend returns `OptionUnsupported` for any `ip6_only` bind there;
        // some sandboxes also lack IPv6 entirely. Skip (rather than fail) where
        // the platform/std cannot configure the dual-stack listener — the path
        // is still exercised on Linux.
        error.AddressUnavailable, error.AddressFamilyUnsupported, error.OptionUnsupported => return error.SkipZigTest,
        else => |e| return e,
    };

    // Dial the v6 dual-stack server via its v4-mapped form: the server
    // should observe an IPv4 peer address.
    const dial_addr: std.Io.net.IpAddress = .{ .ip4 = .loopback(server_addr.getPort()) };

    // Client binds an IPv4 socket so the dial uses an IPv4 source.
    _ = try fixture.bindClientLoopback();

    var accept_ctx = AcceptCtx{ .endpoint = fixture.server };
    const accept_thread = try std.Thread.spawn(.{}, AcceptCtx.run, .{&accept_ctx});

    const client_conn = try fixture.client.dial(dial_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
    });
    var client_conn_live = true;
    errdefer if (client_conn_live) client_conn.deinit();

    accept_thread.join();
    if (accept_ctx.err) |err| return err;
    const server_conn = accept_ctx.conn orelse return error.TestExpectedEqual;
    var server_conn_live = true;
    errdefer if (server_conn_live) server_conn.deinit();

    // The dual-stack server may observe the v4 peer either as a native v4
    // address or as the v4-mapped v6 form (`::ffff:127.0.0.1`). Either is
    // valid; what we want to confirm is that the handshake completed.
    _ = server_conn.remoteAddress();

    client_conn.deinit();
    client_conn_live = false;
    server_conn.deinit();
    server_conn_live = false;
}
