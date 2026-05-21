//! Shared test helpers for the endpoint façade and the router/dialer
//! behavior-focused tests that share the same dual-endpoint fixture.

const std = @import("std");
const config = @import("../config.zig");
const connection_mod = @import("../connection/mod.zig");
const stream_mod = @import("../stream/mod.zig");
const cid_mod = @import("../connection/cid.zig");
const endpoint_handle = @import("handle.zig");
const io_time = @import("../io/time.zig");
const tls = @import("../../security/tls.zig");
const identity = @import("../../identity.zig");
const keys = @import("peer_id").keys;

pub const Connection = connection_mod.Connection;
pub const Stream = stream_mod.Stream;
pub const QuicEndpoint = endpoint_handle.QuicEndpoint;
pub const LocalCid = cid_mod.LocalCid;
pub const local_conn_id_len = cid_mod.local_cid_len;
pub const default_handshake_timeout_ns = config.default_handshake_timeout_ns;

pub fn closeStreamForTest(io: std.Io, stream: *Stream) void {
    stream.reset(io, 0) catch |err| std.log.debug("failed to reset QUIC stream during test cleanup: {}", .{err});
}

pub fn publicKeysEqual(a: *const keys.PublicKey, b: *const keys.PublicKey) bool {
    if (a.type != b.type) return false;
    const a_data = a.data orelse &.{};
    const b_data = b.data orelse &.{};
    return std.mem.eql(u8, a_data, b_data);
}

pub const receiveTimeout = io_time.receiveTimeout;
pub const timeout_ns = io_time.ns;
pub const timeout_us = io_time.us;
pub const timeout_ms = io_time.ms;
pub const timeout_s = io_time.s;

pub fn waitCidMapCount(endpoint: *QuicEndpoint, expected: usize, io: std.Io) !void {
    var attempts: usize = 0;
    while (endpoint.stats().cid_map_entries != expected and attempts < 100) : (attempts += 1) {
        try io_time.ms(10).sleep(io);
    }
    try std.testing.expectEqual(@as(u64, @intCast(expected)), endpoint.stats().cid_map_entries);
}

pub fn waitOpenStreams(conn: *Connection, expected: u64, io: std.Io) !void {
    var attempts: usize = 0;
    while (conn.stats().open_streams != expected and attempts < 100) : (attempts += 1) {
        try io_time.ms(10).sleep(io);
    }
    try std.testing.expectEqual(expected, conn.stats().open_streams);
}

pub fn waitPendingAcceptStreams(conn: *Connection, expected: u64, io: std.Io) !void {
    var attempts: usize = 0;
    while (conn.stats().pending_accept_streams != expected and attempts < 100) : (attempts += 1) {
        try io_time.ms(10).sleep(io);
    }
    try std.testing.expectEqual(expected, conn.stats().pending_accept_streams);
}

pub fn waitDatagramsSent(conn: *Connection, expected: u64, io: std.Io) !void {
    var attempts: usize = 0;
    while (conn.stats().datagrams_sent != expected and attempts < 100) : (attempts += 1) {
        try io_time.ms(10).sleep(io);
    }
    try std.testing.expectEqual(expected, conn.stats().datagrams_sent);
}

pub fn waitDatagramsRecv(conn: *Connection, expected: u64, io: std.Io) !void {
    var attempts: usize = 0;
    while (conn.stats().datagrams_recv != expected and attempts < 100) : (attempts += 1) {
        try io_time.ms(10).sleep(io);
    }
    try std.testing.expectEqual(expected, conn.stats().datagrams_recv);
}

/// Synthesize an Initial packet with an unsupported version to trigger
/// Version Negotiation. Pads to fill `buf` so callers can size to the
/// RFC 9000 §14.1 minimum (1200 bytes).
pub fn makeUnsupportedVersionInitial(buf: []u8) usize {
    var dcid: [local_conn_id_len]u8 = undefined;
    var scid: [local_conn_id_len]u8 = undefined;
    for (&dcid, 0..) |*byte, i| byte.* = @intCast(i);
    for (&scid, 0..) |*byte, i| byte.* = @intCast(0xa0 + i);

    var off: usize = 0;
    buf[off] = 0xc0;
    off += 1;
    std.mem.writeInt(u32, buf[off..][0..4], 0x0a0a0a0a, .big);
    off += 4;
    buf[off] = dcid.len;
    off += 1;
    @memcpy(buf[off..][0..dcid.len], &dcid);
    off += dcid.len;
    buf[off] = scid.len;
    off += 1;
    @memcpy(buf[off..][0..scid.len], &scid);
    off += scid.len;
    buf[off] = 0;
    off += 1;
    buf[off] = 1;
    off += 1;
    buf[off] = 0;
    off += 1;
    @memset(buf[off..], 0);
    return buf.len;
}

/// Spawned in a thread to collect a single inbound `*Connection` from the
/// server-side accept queue.
pub const AcceptCtx = struct {
    endpoint: *QuicEndpoint,
    conn: ?*Connection = null,
    err: ?anyerror = null,

    pub fn run(ctx: *AcceptCtx) void {
        ctx.conn = ctx.endpoint.accept() catch |err| {
            ctx.err = err;
            return;
        };
    }
};

/// Test fixture: two endpoints (server + client) with TLS contexts set up.
/// `server` is bound by the caller; `client` is left unbound. Owns the
/// underlying TLS/identity allocations and frees them on `deinit`.
pub const TwoEndpoints = struct {
    allocator: std.mem.Allocator,
    client_host: identity.KeyPair,
    server_host: identity.KeyPair,
    client_tls: tls.Context,
    server_tls: tls.Context,
    server: *QuicEndpoint,
    client: *QuicEndpoint,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        server_options: QuicEndpoint.Options,
        client_options: QuicEndpoint.Options,
    ) !TwoEndpoints {
        var client_host = try identity.KeyPair.generate(.ED25519);
        errdefer client_host.deinit();
        var server_host = try identity.KeyPair.generate(.ED25519);
        errdefer server_host.deinit();

        var client_tls = try tls.Context.create(
            allocator,
            &client_host,
            .ED25519,
            @ptrCast(&client_host),
            identity.signWithKeyPair,
        );
        errdefer client_tls.deinit();
        var server_tls = try tls.Context.create(
            allocator,
            &server_host,
            .ED25519,
            @ptrCast(&server_host),
            identity.signWithKeyPair,
        );
        errdefer server_tls.deinit();

        const server = try QuicEndpoint.init(allocator, io, server_tls.ssl_ctx, server_options);
        errdefer server.deinit();
        const client = try QuicEndpoint.init(allocator, io, client_tls.ssl_ctx, client_options);
        errdefer client.deinit();

        return .{
            .allocator = allocator,
            .client_host = client_host,
            .server_host = server_host,
            .client_tls = client_tls,
            .server_tls = server_tls,
            .server = server,
            .client = client,
        };
    }

    pub fn deinit(self: *TwoEndpoints) void {
        self.client.deinit();
        self.server.deinit();
        self.client_tls.deinit();
        self.server_tls.deinit();
        self.client_host.deinit();
        self.server_host.deinit();
    }

    /// Bind the server to a fresh loopback UDP port and return the actual
    /// bound address.
    pub fn bindServerLoopback(self: *TwoEndpoints) !std.Io.net.IpAddress {
        return self.server.bind(.{ .ip4 = .loopback(0) });
    }

    /// Bind the client to a fresh loopback UDP port and return the actual
    /// bound address. Required before any `client.dial(...)` call.
    pub fn bindClientLoopback(self: *TwoEndpoints) !std.Io.net.IpAddress {
        return self.client.bind(.{ .ip4 = .loopback(0) });
    }
};
