const std = @import("std");
const support = @import("test_support.zig");
const tls = @import("../../security/tls.zig");
const identity = @import("../../identity.zig");
const keys = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;

const AcceptCtx = support.AcceptCtx;
const TwoEndpoints = support.TwoEndpoints;
const closeStreamForTest = support.closeStreamForTest;
const default_handshake_timeout_ns = support.default_handshake_timeout_ns;
const receiveTimeout = support.receiveTimeout;
const waitCidMapCount = support.waitCidMapCount;
const waitOpenStreams = support.waitOpenStreams;
const waitPendingAcceptStreams = support.waitPendingAcceptStreams;

test "quic endpoint end-to-end: handshake, streams, datagrams, close" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try TwoEndpoints.init(allocator, io, .{ .endpoint = .{ .connection_accept_queue_len = 1 } }, .{});
    defer fixture.deinit();

    const server_addr = try fixture.bindServerLoopback();
    _ = try fixture.bindClientLoopback();

    var server_pub_key = try fixture.server_host.publicKey(allocator);
    defer if (server_pub_key.data) |data| allocator.free(data);
    var client_pub_key = try fixture.client_host.publicKey(allocator);
    defer if (client_pub_key.data) |data| allocator.free(data);

    var accept_ctx = AcceptCtx{ .endpoint = fixture.server };
    const accept_thread = try std.Thread.spawn(.{}, AcceptCtx.run, .{&accept_ctx});

    const client_conn = try fixture.client.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
        .expected_peer_key = &server_pub_key,
    });
    var client_conn_live = true;
    errdefer if (client_conn_live) client_conn.deinit();

    accept_thread.join();
    if (accept_ctx.err) |err| return err;
    const server_conn = accept_ctx.conn orelse return error.TestExpectedEqual;
    var server_conn_live = true;
    errdefer if (server_conn_live) server_conn.deinit();

    try std.testing.expect(client_conn.stats().packets_sent > 0);
    try std.testing.expect(server_conn.stats().packets_recv > 0);
    try std.testing.expect(fixture.client.stats().cid_map_entries > 0);
    try std.testing.expect(fixture.server.stats().cid_map_entries > 0);
    const client_seen_pk = client_conn.remotePublicKey();
    const server_seen_pk = server_conn.remotePublicKey();
    try std.testing.expect(support.publicKeysEqual(&client_seen_pk, &server_pub_key));
    try std.testing.expect(support.publicKeysEqual(&server_seen_pk, &client_pub_key));
    const client_remote_addr = client_conn.remoteAddress();
    try std.testing.expectEqual(server_addr.getPort(), client_remote_addr.getPort());

    {
        const outbound = try client_conn.openStream(io);
        defer outbound.deinit();
        defer closeStreamForTest(io, outbound);

        const payload = "loopback stream payload";
        try outbound.writeAll(io, payload, .{});
        try waitOpenStreams(client_conn, 1, io);

        var accept_queue_attempts: usize = 0;
        while (server_conn.stats().pending_accept_streams == 0 and accept_queue_attempts < 100) : (accept_queue_attempts += 1) {
            try support.timeout_ms(10).sleep(io);
        }
        try std.testing.expectEqual(@as(u64, 1), server_conn.stats().pending_accept_streams);

        const inbound = try server_conn.acceptStream(io, .{});
        defer inbound.deinit();
        defer closeStreamForTest(io, inbound);
        try waitOpenStreams(server_conn, 1, io);
        try waitPendingAcceptStreams(server_conn, 0, io);

        var recv_buf: [64]u8 = undefined;
        try inbound.readAll(io, recv_buf[0..payload.len], .{});
        try std.testing.expectEqualStrings(payload, recv_buf[0..payload.len]);

        const reply = "loopback reply";
        try inbound.writeAll(io, reply, .{});

        var reply_buf: [64]u8 = undefined;
        try outbound.readAll(io, reply_buf[0..reply.len], .{});
        try std.testing.expectEqualStrings(reply, reply_buf[0..reply.len]);

        const dgram_payload = "loopback datagram";
        try client_conn.sendDatagram(io, dgram_payload);
        const dgram = try server_conn.recvDatagram(io, .{});
        defer dgram.release();
        try std.testing.expectEqualStrings(dgram_payload, dgram.bytes());
        // Stats are eventually consistent: quiche increments dgram_sent during
        // the actor's flush stage, not when the user-facing send returns.
        try support.waitDatagramsSent(client_conn, 1, io);
        try support.waitDatagramsRecv(server_conn, 1, io);
        const client_stats = client_conn.stats();
        const server_stats = server_conn.stats();
        try std.testing.expect(client_stats.bytes_sent > 0);
        try std.testing.expect(server_stats.bytes_recv > 0);
        try std.testing.expect(client_stats.active_path != null);
        try std.testing.expect(server_stats.active_path != null);
        try std.testing.expect(client_stats.last_updated_mono_ns > 0);
        try std.testing.expect(server_stats.last_updated_mono_ns > 0);
        try outbound.closeWrite(io);
        try std.testing.expectError(error.EndOfStream, inbound.read(io, recv_buf[0..], .{
            .timeout = support.timeout_s(1),
        }));
        try std.testing.expectError(error.StreamShutdown, outbound.write(io, "x", .{}));
        try std.testing.expectError(error.Timeout, server_conn.recvDatagram(io, .{
            .timeout = support.timeout_ms(1),
        }));
        try std.testing.expectError(error.Timeout, server_conn.acceptStream(io, .{
            .timeout = support.timeout_ms(1),
        }));
    }

    {
        const first = try client_conn.openStream(io);
        defer first.deinit();
        defer closeStreamForTest(io, first);
        const second = try client_conn.openStream(io);
        defer second.deinit();
        defer closeStreamForTest(io, second);

        const first_payload = "queued stream one";
        const second_payload = "queued stream two";
        try first.writeAll(io, first_payload, .{});
        try second.writeAll(io, second_payload, .{});

        var attempts: usize = 0;
        while (server_conn.stats().pending_accept_streams < 2 and attempts < 100) : (attempts += 1) {
            try support.timeout_ms(10).sleep(io);
        }
        try std.testing.expectEqual(@as(u64, 2), server_conn.stats().pending_accept_streams);

        const accepted_first = try server_conn.acceptStream(io, .{});
        defer accepted_first.deinit();
        defer closeStreamForTest(io, accepted_first);
        var first_buf: [64]u8 = undefined;
        try accepted_first.readAll(io, first_buf[0..first_payload.len], .{});
        try std.testing.expectEqualStrings(first_payload, first_buf[0..first_payload.len]);
        try waitPendingAcceptStreams(server_conn, 1, io);

        const accepted_second = try server_conn.acceptStream(io, .{});
        defer accepted_second.deinit();
        defer closeStreamForTest(io, accepted_second);
        var second_buf: [64]u8 = undefined;
        try accepted_second.readAll(io, second_buf[0..second_payload.len], .{});
        try std.testing.expectEqualStrings(second_payload, second_buf[0..second_payload.len]);
        try waitPendingAcceptStreams(server_conn, 0, io);
    }
    // Graceful close (FIN + STOP_SENDING) leaves the actor entry in place
    // until quiche reports the stream finished — both peers must round-trip
    // their FINs before `collectFinishedStreamActor` reaps the slot. Poll
    // rather than asserting an exact moment-in-time count.
    try waitOpenStreams(client_conn, 0, io);
    try waitOpenStreams(server_conn, 0, io);

    client_conn.deinit();
    client_conn_live = false;
    server_conn.deinit();
    server_conn_live = false;
    try std.testing.expectEqual(@as(u64, 1), fixture.client.stats().connections_closed);
    try std.testing.expectEqual(@as(u64, 1), fixture.server.stats().connections_closed);
    try waitCidMapCount(fixture.client, 0, io);
    try waitCidMapCount(fixture.server, 0, io);
    try std.testing.expectEqual(@as(u64, 0), fixture.client.stats().active_actors);
    try std.testing.expectEqual(@as(u64, 0), fixture.server.stats().active_actors);
}

test "quic endpoint idle timeout closes and publishes stats" {
    const allocator = std.testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // keep_alive_period_ms=0 disables keep-alive so this test asserts the idle
    // timeout itself — otherwise a default-period PING would reset the idle timer
    // and the connection would never close (and the test would assert nothing).
    const short_idle_options: support.QuicEndpoint.Options = .{ .transport = .{ .max_idle_timeout_ms = 75, .keep_alive_period_ms = 0 } };
    var fixture = try TwoEndpoints.init(allocator, io, short_idle_options, short_idle_options);
    defer fixture.deinit();

    const server_addr = try fixture.bindServerLoopback();
    _ = try fixture.bindClientLoopback();

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

    var attempts: usize = 0;
    while ((!client_conn.isClosed() or !server_conn.isClosed()) and attempts < 300) : (attempts += 1) {
        try support.timeout_ms(10).sleep(io);
    }

    try std.testing.expect(client_conn.isClosed());
    try std.testing.expect(server_conn.isClosed());
    try std.testing.expectEqual(.idle_timeout, client_conn.stats().close_reason);
    try std.testing.expectEqual(.idle_timeout, server_conn.stats().close_reason);
    try std.testing.expectError(error.ConnectionClosed, client_conn.openStream(io));
}

test "secp256k1 host identity round-trips through cert build + verify (ECDSA cert key)" {
    // Proves a secp256k1 *identity* is fully supported end-to-end: the
    // production cert path (Context.create with an ECDSA P-256 cert key + the
    // secp256k1 host key signing the libp2p extension) builds a cert, and
    // verifyAndExtractPeerInfo extracts + verifies it. secp256k1 lives only in
    // the host identity (verified in software via the vendored secp lib), never
    // in the TLS cert key — so it never touches BoringSSL's EVP path. This also
    // exercises PeerId.fromPublicKey for secp256k1.
    const allocator = std.testing.allocator;

    var host = try identity.KeyPair.generate(.SECP256K1);
    defer host.deinit();

    var ctx = try tls.Context.create(allocator, &host, .ECDSA, @ptrCast(&host), identity.signWithKeyPair);
    defer ctx.deinit();

    const info = try tls.verifyAndExtractPeerInfo(allocator, ctx.subject_cert);
    defer allocator.free(info.host_pubkey.data.?);

    try std.testing.expect(info.is_valid);
    try std.testing.expectEqual(keys.KeyType.SECP256K1, info.host_pubkey.type);

    var expected_pub = try host.publicKey(allocator);
    defer allocator.free(expected_pub.data.?);
    const expected_pid = try PeerId.fromPublicKey(allocator, &expected_pub);
    try std.testing.expect(info.peer_id.eql(&expected_pid));
}
