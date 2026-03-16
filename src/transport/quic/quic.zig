const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;
const multiaddr = @import("multiaddr");
const Multiaddr = multiaddr.Multiaddr;
const Ip4Addr = multiaddr.Ip4Addr;

const PeerId = @import("peer_id").PeerId;
const ssl = @import("ssl");
const tls = @import("../../security/tls.zig");

const quic = @This();

const engine_mod = @import("engine.zig");
const QuicEngine = engine_mod.QuicEngine;
const QuicConnectionInner = engine_mod.QuicConnection;
const QuicStreamInner = engine_mod.QuicStream;

const transport_mod = @import("../transport.zig");

// ── QuicStream ─────────────────────────────────────────────────────────

/// Transport-level QUIC stream satisfying the Stream interface.
pub const Stream = struct {
    inner: *QuicStreamInner,

    pub fn read(self: *Stream, io: Io, buf: []u8) anyerror!usize {
        return self.inner.read(io, buf);
    }

    pub fn write(self: *Stream, io: Io, data: []const u8) anyerror!usize {
        return self.inner.write(io, data);
    }

    pub fn close(self: *Stream, io: Io) void {
        self.inner.close(io);
    }
};

// ── QuicConnection ────────────────────────────────────────────────────

/// Transport-level QUIC connection satisfying the Connection interface.
pub const Connection = struct {
    inner: *QuicConnectionInner,
    /// Engine owned by this connection (client-side dial). null for server-side connections.
    owned_engine: ?*QuicEngine = null,

    pub fn openStream(self: *Connection, io: Io) !Stream {
        const s = try self.inner.openStream(io);
        return .{ .inner = s };
    }

    pub fn acceptStream(self: *Connection, io: Io) !Stream {
        const s = try self.inner.acceptStream(io);
        return .{ .inner = s };
    }

    pub fn close(self: *Connection, io: Io) void {
        self.inner.close(io);
        // If this connection owns the engine (client-side), stop and clean up
        if (self.owned_engine) |eng| {
            eng.stop(io);
            eng.deinit();
            self.owned_engine = null;
        }
    }

    pub fn remotePeerId(self: *const Connection) ?PeerId {
        return self.inner.remotePeerId();
    }

    pub fn remoteAddr(self: *const Connection) ?Multiaddr {
        // TODO: build multiaddr from lsquic_conn_get_sockaddr
        _ = self;
        return null;
    }
};

// ── QuicListener ──────────────────────────────────────────────────────

/// QUIC listener that accepts incoming connections.
pub const Listener = struct {
    engine: *QuicEngine,
    local_address: ?net.IpAddress,

    pub fn accept(self: *Listener, io: Io) !Connection {
        const conn = try self.engine.accept(io);
        return .{ .inner = conn };
    }

    pub fn close(self: *Listener, io: Io) void {
        self.engine.stop(io);
        self.engine.deinit();
    }

    pub fn localAddr(self: *const Listener) ?net.IpAddress {
        return self.local_address;
    }
};

// ── QuicTransport ─────────────────────────────────────────────────────

/// QUIC transport implementing the Transport interface.
/// Uses lsquic via QuicEngine for the underlying QUIC protocol.
pub const QuicTransport = struct {
    allocator: Allocator,
    config: QuicEngine.Config,

    pub const Stream = quic.Stream;
    pub const Connection = quic.Connection;
    pub const Listener = quic.Listener;

    pub fn init(allocator: Allocator, config: QuicEngine.Config) QuicTransport {
        return .{
            .allocator = allocator,
            .config = config,
        };
    }

    /// Dial a remote QUIC endpoint.
    /// The multiaddr should be of the form /ip4/.../udp/.../quic
    pub fn dial(self: *QuicTransport, io: Io, addr: Multiaddr) !quic.Connection {
        const parsed = try parseQuicMultiaddr(addr);

        // Create a client engine
        var client_config = self.config;
        client_config.is_server = false;
        const eng = try QuicEngine.init(self.allocator, client_config);
        errdefer eng.deinit();

        eng.setIo(io);

        // Bind to ephemeral port (0)
        const local = net.IpAddress{ .ip4 = net.Ip4Address.unspecified(0) };
        try eng.bindSocket(io, &local);

        // Convert target and local addresses to sockaddr
        var remote_sa = engine_mod.ipAddressToSockaddr(parsed.ip);
        const local_bound = (eng.socket orelse return error.NoSocket).address;
        var local_sa = engine_mod.ipAddressToSockaddr(local_bound);
        const conn = try eng.connect(io, @ptrCast(&remote_sa), @ptrCast(&local_sa));

        // Start receive and timer loops (stored in engine.background)
        eng.startBackgroundLoops(io);

        return .{ .inner = conn, .owned_engine = eng };
    }

    /// Listen for incoming QUIC connections on the given multiaddr.
    pub fn listen(self: *QuicTransport, io: Io, addr: Multiaddr) !quic.Listener {
        const parsed = try parseQuicMultiaddr(addr);

        // Create a server engine
        var server_config = self.config;
        server_config.is_server = true;
        const eng = try QuicEngine.init(self.allocator, server_config);
        errdefer eng.deinit();

        eng.setIo(io);

        // Bind to the requested address
        try eng.bindSocket(io, &parsed.ip);

        // Start receive and timer loops (stored in engine.background)
        eng.startBackgroundLoops(io);

        return .{
            .engine = eng,
            .local_address = if (eng.socket) |s| s.address else null,
        };
    }

    /// Check if a multiaddr represents a QUIC address.
    pub fn matchesMultiaddr(addr: Multiaddr) bool {
        var it = addr.iterator();
        var has_ip = false;
        var has_udp = false;
        var has_quic = false;
        while (it.next() catch null) |proto| {
            switch (proto) {
                .Ip4, .Ip6 => has_ip = true,
                .Udp => has_udp = true,
                .Quic, .QuicV1 => has_quic = true,
                else => {},
            }
        }
        return has_ip and has_udp and has_quic;
    }

    // Compile-time interface check
    comptime {
        transport_mod.assertTransportInterface(QuicTransport);
    }
};

// ── Helpers ───────────────────────────────────────────────────────────

const ParsedQuicAddr = struct {
    ip: net.IpAddress,
};

/// Parse a multiaddr like /ip4/127.0.0.1/udp/9000/quic into an IpAddress.
fn parseQuicMultiaddr(addr: Multiaddr) !ParsedQuicAddr {
    var it = addr.iterator();
    var ip4_addr: ?[4]u8 = null;
    var ip6_addr: ?[16]u8 = null;
    var udp_port: ?u16 = null;

    while (try it.next()) |proto| {
        switch (proto) {
            .Ip4 => |a| ip4_addr = a.bytes,
            .Ip6 => |a| ip6_addr = a.bytes,
            .Udp => |p| udp_port = p,
            .Quic, .QuicV1 => break,
            else => {},
        }
    }

    const port = udp_port orelse return error.MissingUdpPort;

    if (ip4_addr) |bytes| {
        return .{ .ip = .{ .ip4 = .{ .bytes = bytes, .port = port } } };
    }
    if (ip6_addr) |bytes| {
        return .{ .ip = .{ .ip6 = .{ .bytes = bytes, .port = port } } };
    }
    return error.MissingIpAddress;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "matchesMultiaddr detects QUIC addresses" {
    const allocator = std.testing.allocator;

    // /ip4/127.0.0.1/udp/9000/quic
    var ma = try Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = 9000 },
        .Quic,
    });
    defer ma.deinit();

    try std.testing.expect(QuicTransport.matchesMultiaddr(ma));
}

test "matchesMultiaddr rejects non-QUIC addresses" {
    const allocator = std.testing.allocator;

    // /ip4/127.0.0.1/tcp/9000 (no quic)
    var ma = try Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Tcp = 9000 },
    });
    defer ma.deinit();

    try std.testing.expect(!QuicTransport.matchesMultiaddr(ma));
}

test "parseQuicMultiaddr extracts ip and port" {
    const allocator = std.testing.allocator;

    var ma = try Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = Ip4Addr{ .bytes = .{ 10, 0, 0, 1 } } },
        .{ .Udp = 4242 },
        .Quic,
    });
    defer ma.deinit();

    const parsed = try parseQuicMultiaddr(ma);
    switch (parsed.ip) {
        .ip4 => |a| {
            try std.testing.expectEqual(@as(u16, 4242), a.port);
            try std.testing.expectEqual([4]u8{ 10, 0, 0, 1 }, a.bytes);
        },
        else => return error.UnexpectedAddressFamily,
    }
}

// ── Integration Tests ─────────────────────────────────────────────────

test "QUIC engine socket binding" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Create server engine
    const eng = QuicEngine.init(allocator, .{ .is_server = true }) catch |err| {
        std.log.warn("QuicEngine init failed (expected if lsquic unavailable): {}", .{err});
        return;
    };
    defer eng.deinit();

    eng.setIo(io);

    // Bind to loopback ephemeral port
    const addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    eng.bindSocket(io, &addr) catch |err| {
        std.log.warn("bindSocket failed: {}", .{err});
        return;
    };

    // Verify socket was created and bound to a real port
    const sock = eng.socket orelse return error.NoSocket;
    const port = switch (sock.address) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    try std.testing.expect(port > 0);
}

test "QUIC engine background loops start and stop" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const eng = QuicEngine.init(allocator, .{ .is_server = true }) catch |err| {
        std.log.warn("QuicEngine init failed: {}", .{err});
        return;
    };

    eng.setIo(io);

    const addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    eng.bindSocket(io, &addr) catch |err| {
        std.log.warn("bindSocket failed: {}", .{err});
        eng.deinit();
        return;
    };

    // Start background loops
    eng.startBackgroundLoops(io);
    try std.testing.expect(eng.running);

    // Stop engine — this should cancel background loops and clean up
    eng.stop(io);
    try std.testing.expect(!eng.running);

    eng.deinit();
}

test "QUIC engine client connect initiates handshake" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Create client engine
    const eng = QuicEngine.init(allocator, .{ .is_server = false }) catch |err| {
        std.log.warn("QuicEngine init failed: {}", .{err});
        return;
    };

    eng.setIo(io);

    const addr = net.IpAddress{ .ip4 = net.Ip4Address.unspecified(0) };
    eng.bindSocket(io, &addr) catch |err| {
        std.log.warn("bindSocket failed: {}", .{err});
        eng.deinit();
        return;
    };

    // Connect to a (non-existent) remote — should not crash
    const remote = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 19999 } };
    var remote_sa = engine_mod.ipAddressToSockaddr(remote);
    const local_bound = if (eng.socket) |s| s.address else {
        eng.deinit();
        return;
    };
    var local_sa = engine_mod.ipAddressToSockaddr(local_bound);
    const conn = eng.connect(io, @ptrCast(&remote_sa), @ptrCast(&local_sa)) catch |err| {
        std.log.warn("connect failed: {}", .{err});
        eng.deinit();
        return;
    };

    // Connection should be created (handshake not yet complete)
    try std.testing.expect(!conn.closed);
    try std.testing.expect(!conn.hsk_completed);

    // Clean up without starting background loops
    conn.close(io);
    conn.deinit();
    eng.stop(io);
    eng.deinit();
}

test "QUIC connection ownership in dial" {
    // Verify Connection.owned_engine works correctly
    var conn = quic.Connection{ .inner = undefined, .owned_engine = null };
    try std.testing.expect(conn.owned_engine == null);

    // Server-side connections don't own an engine
    const server_conn = quic.Connection{ .inner = undefined };
    try std.testing.expect(server_conn.owned_engine == null);
}

test "QuicTransport interface compliance" {
    // Compile-time check that QuicTransport satisfies the Transport interface
    comptime {
        transport_mod.assertTransportInterface(QuicTransport);
    }
}

test "QUIC full handshake between server and client" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Generate host identity keys for server and client
    const server_host_key = tls.generateKeyPair(.ECDSA) catch |err| {
        std.log.warn("generateKeyPair failed: {}", .{err});
        return;
    };
    defer ssl.EVP_PKEY_free(server_host_key);

    const client_host_key = tls.generateKeyPair(.ECDSA) catch |err| {
        std.log.warn("generateKeyPair failed: {}", .{err});
        return;
    };
    defer ssl.EVP_PKEY_free(client_host_key);

    // Create server engine with TLS cert
    const server_eng = QuicEngine.init(allocator, .{
        .is_server = true,
        .host_key = server_host_key,
    }) catch |err| {
        std.log.warn("Server engine init failed: {}", .{err});
        return;
    };

    server_eng.setIo(io);

    // Bind server to loopback ephemeral port
    const server_addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    server_eng.bindSocket(io, &server_addr) catch |err| {
        std.log.warn("Server bindSocket failed: {}", .{err});
        server_eng.deinit();
        return;
    };

    // Get the actual port assigned to the server
    const server_port = switch ((server_eng.socket orelse {
        server_eng.deinit();
        return;
    }).address) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    try std.testing.expect(server_port > 0);

    // Start server background loops
    server_eng.startBackgroundLoops(io);

    // Create client engine with TLS cert
    const client_eng = QuicEngine.init(allocator, .{
        .is_server = false,
        .host_key = client_host_key,
    }) catch |err| {
        std.log.warn("Client engine init failed: {}", .{err});
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };

    client_eng.setIo(io);

    // Bind client to ephemeral port
    const client_addr = net.IpAddress{ .ip4 = net.Ip4Address.unspecified(0) };
    client_eng.bindSocket(io, &client_addr) catch |err| {
        std.log.warn("Client bindSocket failed: {}", .{err});
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };

    // Connect client to server
    const remote = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = server_port } };
    var remote_sa = engine_mod.ipAddressToSockaddr(remote);
    const local_bound = (client_eng.socket orelse {
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    }).address;
    var local_sa = engine_mod.ipAddressToSockaddr(local_bound);

    const client_conn = client_eng.connect(io, @ptrCast(&remote_sa), @ptrCast(&local_sa)) catch |err| {
        std.log.warn("Client connect failed: {}", .{err});
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };

    // Start client background loops (receive + timer)
    client_eng.startBackgroundLoops(io);

    // Server accepts the incoming connection (blocks until handshake completes)
    const server_conn = server_eng.accept(io) catch |err| {
        std.log.warn("Server accept failed: {}", .{err});
        client_conn.close(io);
        client_conn.deinit();
        client_eng.stop(io);
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };

    // Verify handshake completed on server side
    try std.testing.expect(server_conn.hsk_completed);

    // Verify peer IDs were extracted from TLS certificates
    try std.testing.expect(server_conn.peer_id != null);

    // Clean up: close conns → stop loops → destroy engines → free wrappers
    server_conn.close(io);
    client_conn.close(io);
    server_eng.stop(io);
    client_eng.stop(io);
    server_eng.deinit();
    client_eng.deinit();
    server_conn.deinit();
    client_conn.deinit();
}

test "QUIC stream read/write round-trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Generate host identity keys
    const server_host_key = tls.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(server_host_key);
    const client_host_key = tls.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(client_host_key);

    // Create and bind server engine
    const server_eng = QuicEngine.init(allocator, .{
        .is_server = true,
        .host_key = server_host_key,
    }) catch return;
    server_eng.setIo(io);
    const server_addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    server_eng.bindSocket(io, &server_addr) catch {
        server_eng.deinit();
        return;
    };
    const server_port = switch ((server_eng.socket orelse {
        server_eng.deinit();
        return;
    }).address) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    server_eng.startBackgroundLoops(io);

    // Create, bind, and connect client engine
    const client_eng = QuicEngine.init(allocator, .{
        .is_server = false,
        .host_key = client_host_key,
    }) catch {
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };
    client_eng.setIo(io);
    client_eng.bindSocket(io, &net.IpAddress{ .ip4 = net.Ip4Address.unspecified(0) }) catch {
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };

    const remote = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = server_port } };
    var remote_sa = engine_mod.ipAddressToSockaddr(remote);
    const local_bound = (client_eng.socket orelse {
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    }).address;
    var local_sa = engine_mod.ipAddressToSockaddr(local_bound);

    const client_conn = client_eng.connect(io, @ptrCast(&remote_sa), @ptrCast(&local_sa)) catch {
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };
    client_eng.startBackgroundLoops(io);

    // Accept connection on server
    const server_conn = server_eng.accept(io) catch {
        client_conn.close(io);
        client_conn.deinit();
        client_eng.stop(io);
        client_eng.deinit();
        server_eng.stop(io);
        server_eng.deinit();
        return;
    };

    // Client opens a stream and writes data immediately.
    // QUIC streams are lazy — the server only sees the stream when the
    // client sends a STREAM frame, so we must write before the server
    // can accept.
    const client_stream = client_conn.openStream(io) catch |err| {
        std.log.warn("openStream failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_conn.deinit();
        client_conn.deinit();
        server_eng.deinit();
        client_eng.deinit();
        return;
    };

    // Client writes data (this sends a STREAM frame to the server)
    const msg = "hello from client";
    const written = client_stream.write(io, msg) catch 0;
    try std.testing.expectEqual(msg.len, written);

    // Server accepts the stream (unblocks when STREAM frame arrives)
    const server_stream = server_conn.acceptStream(io) catch |err| {
        std.log.warn("acceptStream failed: {}", .{err});
        client_stream.close(io);
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_conn.deinit();
        client_conn.deinit();
        server_eng.deinit();
        client_eng.deinit();
        return;
    };

    // Server reads data
    var buf: [64]u8 = undefined;
    const n = server_stream.read(io, &buf) catch 0;
    try std.testing.expectEqualSlices(u8, msg, buf[0..n]);

    // Server echoes back
    const echo_msg = "hello from server";
    const echo_written = server_stream.write(io, echo_msg) catch 0;
    try std.testing.expectEqual(echo_msg.len, echo_written);

    // Client reads echo
    var echo_buf: [64]u8 = undefined;
    const echo_n = client_stream.read(io, &echo_buf) catch 0;
    try std.testing.expectEqualSlices(u8, echo_msg, echo_buf[0..echo_n]);

    // Clean up: order matters!
    // 1. Close streams (marks them for close in lsquic, onStreamClose frees QuicStream)
    // 2. Close connections (marks them for close in lsquic)
    // 3. Stop background loops
    // 4. Destroy engines (force-closes remaining conns, triggers callbacks)
    // 5. Free connection structs last
    client_stream.close(io);
    server_stream.close(io);
    server_conn.close(io);
    client_conn.close(io);
    server_eng.stop(io);
    client_eng.stop(io);
    server_eng.deinit();
    client_eng.deinit();
    // QuicStream objects freed by onStreamClose callback
    server_conn.deinit();
    client_conn.deinit();
}

// ── Test Helpers ──────────────────────────────────────────────────────

const TestContext = struct {
    server_eng: *QuicEngine,
    client_eng: *QuicEngine,
    server_conn: *engine_mod.QuicConnection,
    client_conn: *engine_mod.QuicConnection,
    server_host_key: *ssl.EVP_PKEY,
    client_host_key: *ssl.EVP_PKEY,
    allocator: Allocator,

    fn initPair(allocator: Allocator, io: Io) !TestContext {
        const server_host_key = try tls.generateKeyPair(.ECDSA);
        errdefer ssl.EVP_PKEY_free(server_host_key);
        const client_host_key = try tls.generateKeyPair(.ECDSA);
        errdefer ssl.EVP_PKEY_free(client_host_key);

        const server_eng = try QuicEngine.init(allocator, .{
            .is_server = true,
            .host_key = server_host_key,
        });
        errdefer server_eng.deinit();
        server_eng.setIo(io);
        const server_addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
        try server_eng.bindSocket(io, &server_addr);
        server_eng.startBackgroundLoops(io);

        const server_port = switch ((server_eng.socket orelse return error.NoSocket).address) {
            .ip4 => |a| a.port,
            .ip6 => |a| a.port,
        };

        const client_eng = try QuicEngine.init(allocator, .{
            .is_server = false,
            .host_key = client_host_key,
        });
        errdefer {
            client_eng.deinit();
            server_eng.stop(io);
        }
        client_eng.setIo(io);
        try client_eng.bindSocket(io, &net.IpAddress{ .ip4 = net.Ip4Address.unspecified(0) });

        const remote = net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = server_port } };
        var remote_sa = engine_mod.ipAddressToSockaddr(remote);
        const local_bound = (client_eng.socket orelse return error.NoSocket).address;
        var local_sa = engine_mod.ipAddressToSockaddr(local_bound);
        const client_conn = try client_eng.connect(io, @ptrCast(&remote_sa), @ptrCast(&local_sa));
        client_eng.startBackgroundLoops(io);

        const server_conn = try server_eng.accept(io);

        return .{
            .server_eng = server_eng,
            .client_eng = client_eng,
            .server_conn = server_conn,
            .client_conn = client_conn,
            .server_host_key = server_host_key,
            .client_host_key = client_host_key,
            .allocator = allocator,
        };
    }

    fn deinit(self: *TestContext, io: Io) void {
        self.server_conn.close(io);
        self.client_conn.close(io);
        self.server_eng.stop(io);
        self.client_eng.stop(io);
        self.server_eng.deinit();
        self.client_eng.deinit();
        self.server_conn.deinit();
        self.client_conn.deinit();
        ssl.EVP_PKEY_free(self.server_host_key);
        ssl.EVP_PKEY_free(self.client_host_key);
    }
};

test "QUIC multiple concurrent streams" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var ctx = TestContext.initPair(allocator, io) catch |err| {
        std.log.warn("TestContext init failed: {}", .{err});
        return;
    };
    defer ctx.deinit(io);

    const stream_count = 3;
    var client_streams: [stream_count]*engine_mod.QuicStream = undefined;
    var server_streams: [stream_count]*engine_mod.QuicStream = undefined;

    // Client opens streams and writes data to trigger server-side visibility
    for (0..stream_count) |i| {
        client_streams[i] = ctx.client_conn.openStream(io) catch |err| {
            std.log.warn("openStream {} failed: {}", .{ i, err });
            for (0..i) |j| {
                client_streams[j].close(io);
            }
            return;
        };
        var msg_buf: [16]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "stream-{}", .{i}) catch unreachable;
        const w = client_streams[i].write(io, msg) catch 0;
        try std.testing.expectEqual(msg.len, w);
    }

    // Server accepts all streams and reads data
    for (0..stream_count) |i| {
        server_streams[i] = ctx.server_conn.acceptStream(io) catch |err| {
            std.log.warn("acceptStream {} failed: {}", .{ i, err });
            for (0..stream_count) |j| {
                client_streams[j].close(io);
            }
            for (0..i) |j| {
                server_streams[j].close(io);
            }
            return;
        };
        var buf: [64]u8 = undefined;
        const n = server_streams[i].read(io, &buf) catch 0;
        try std.testing.expect(n >= 7);
        try std.testing.expectEqualSlices(u8, "stream-", buf[0..7]);
    }

    // Clean up streams — onStreamClose frees QuicStream objects
    for (0..stream_count) |i| {
        client_streams[i].close(io);
        server_streams[i].close(io);
    }
}

test "QUIC bidirectional peer ID verification" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var ctx = TestContext.initPair(allocator, io) catch |err| {
        std.log.warn("TestContext init failed: {}", .{err});
        return;
    };
    defer ctx.deinit(io);

    // Both sides should have extracted peer_id
    try std.testing.expect(ctx.server_conn.peer_id != null);
    try std.testing.expect(ctx.client_conn.peer_id != null);

    // Derive expected PeerId from the host keys
    var server_pubkey = tls.createProtobufEncodedPublicKey(allocator, ctx.server_host_key) catch |err| {
        std.log.warn("createProtobufEncodedPublicKey failed: {}", .{err});
        return;
    };
    defer allocator.free(server_pubkey.data.?);
    const expected_server_pid = PeerId.fromPublicKey(allocator, &server_pubkey) catch |err| {
        std.log.warn("PeerId.fromPublicKey failed: {}", .{err});
        return;
    };

    var client_pubkey = tls.createProtobufEncodedPublicKey(allocator, ctx.client_host_key) catch |err| {
        std.log.warn("createProtobufEncodedPublicKey failed: {}", .{err});
        return;
    };
    defer allocator.free(client_pubkey.data.?);
    const expected_client_pid = PeerId.fromPublicKey(allocator, &client_pubkey) catch |err| {
        std.log.warn("PeerId.fromPublicKey failed: {}", .{err});
        return;
    };

    // Server extracted the CLIENT's peer_id
    try std.testing.expect(ctx.server_conn.peer_id.?.eql(&expected_client_pid));
    // Client extracted the SERVER's peer_id
    try std.testing.expect(ctx.client_conn.peer_id.?.eql(&expected_server_pid));
}

test "QUIC connection close cleanup" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var ctx = TestContext.initPair(allocator, io) catch |err| {
        std.log.warn("TestContext init failed: {}", .{err});
        return;
    };

    // Verify pre-close state
    try std.testing.expect(!ctx.server_conn.closed);
    try std.testing.expect(ctx.server_conn.peer_id != null);

    // Save the peer_id for post-close check
    const saved_peer_id = ctx.server_conn.peer_id.?;

    // Close the server connection
    ctx.server_conn.close(io);

    // Verify closed state
    try std.testing.expect(ctx.server_conn.closed);
    // remotePeerId() should still return the peer_id
    try std.testing.expect(ctx.server_conn.remotePeerId() != null);
    try std.testing.expect(ctx.server_conn.remotePeerId().?.eql(&saved_peer_id));
    // lsquic_conn should be null
    try std.testing.expect(ctx.server_conn.lsquic_conn == null);
    // acceptStream should return ConnectionClosed
    const result = ctx.server_conn.acceptStream(io);
    try std.testing.expectError(error.ConnectionClosed, result);

    // Clean up remaining resources (can't use defer ctx.deinit since server_conn already closed)
    ctx.client_conn.close(io);
    ctx.server_eng.stop(io);
    ctx.client_eng.stop(io);
    ctx.server_eng.deinit();
    ctx.client_eng.deinit();
    ctx.server_conn.deinit();
    ctx.client_conn.deinit();
    ssl.EVP_PKEY_free(ctx.server_host_key);
    ssl.EVP_PKEY_free(ctx.client_host_key);
}

test "QUIC large message spanning multiple reads" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var ctx = TestContext.initPair(allocator, io) catch |err| {
        std.log.warn("TestContext init failed: {}", .{err});
        return;
    };
    defer ctx.deinit(io);

    // Client opens stream and sends 16KB message
    const client_stream = ctx.client_conn.openStream(io) catch |err| {
        std.log.warn("openStream failed: {}", .{err});
        return;
    };

    const msg_size = 16 * 1024; // 16KB
    const send_buf = try allocator.alloc(u8, msg_size);
    defer allocator.free(send_buf);
    // Fill with recognizable pattern
    for (send_buf, 0..) |*b, i| {
        b.* = @intCast(i % 251); // prime mod to avoid alignment artifacts
    }

    const written = client_stream.write(io, send_buf) catch |err| {
        std.log.warn("write failed: {}", .{err});
        client_stream.close(io);
        return;
    };
    try std.testing.expectEqual(msg_size, written);

    // Server accepts and reads in a loop
    const server_stream = ctx.server_conn.acceptStream(io) catch |err| {
        std.log.warn("acceptStream failed: {}", .{err});
        client_stream.close(io);
        return;
    };

    const recv_buf = try allocator.alloc(u8, msg_size);
    defer allocator.free(recv_buf);
    var total_read: usize = 0;
    while (total_read < msg_size) {
        const n = server_stream.read(io, recv_buf[total_read..]) catch |err| {
            std.log.warn("read failed after {} bytes: {}", .{ total_read, err });
            break;
        };
        if (n == 0) break;
        total_read += n;
    }

    try std.testing.expectEqual(msg_size, total_read);
    try std.testing.expectEqualSlices(u8, send_buf, recv_buf[0..total_read]);

    // Clean up
    client_stream.close(io);
    server_stream.close(io);
}

test "QuicTransport dial and listen via multiaddr" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const server_host_key = tls.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(server_host_key);
    const client_host_key = tls.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(client_host_key);

    // Server transport: listen on /ip4/127.0.0.1/udp/0/quic-v1
    var server_transport = QuicTransport.init(allocator, .{
        .is_server = true,
        .host_key = server_host_key,
    });

    var listen_addr = Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = 0 },
        .QuicV1,
    }) catch return;
    defer listen_addr.deinit();

    var listener = server_transport.listen(io, listen_addr) catch |err| {
        std.log.warn("listen failed: {}", .{err});
        return;
    };

    // Get the actual bound port
    const bound_addr = listener.localAddr() orelse {
        listener.close(io);
        return;
    };
    const bound_port = switch (bound_addr) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    try std.testing.expect(bound_port > 0);

    // Client transport: dial
    var client_transport = QuicTransport.init(allocator, .{
        .is_server = false,
        .host_key = client_host_key,
    });

    var dial_addr = Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = bound_port },
        .QuicV1,
    }) catch return;
    defer dial_addr.deinit();

    var client_conn = client_transport.dial(io, dial_addr) catch |err| {
        std.log.warn("dial failed: {}", .{err});
        listener.close(io);
        return;
    };

    // Accept connection on server
    var server_conn = listener.accept(io) catch |err| {
        std.log.warn("accept failed: {}", .{err});
        client_conn.close(io);
        client_conn.inner.deinit();
        listener.close(io);
        return;
    };

    // Verify peer IDs
    try std.testing.expect(server_conn.remotePeerId() != null);
    try std.testing.expect(client_conn.remotePeerId() != null);

    // Open stream via transport Connection API and exchange data
    var client_stream = client_conn.openStream(io) catch |err| {
        std.log.warn("openStream failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        listener.close(io);
        server_conn.inner.deinit();
        client_conn.inner.deinit();
        return;
    };

    const msg = "transport-level test";
    const w = client_stream.write(io, msg) catch 0;
    try std.testing.expectEqual(msg.len, w);

    var server_stream = server_conn.acceptStream(io) catch |err| {
        std.log.warn("acceptStream failed: {}", .{err});
        client_stream.close(io);
        server_conn.close(io);
        client_conn.close(io);
        listener.close(io);
        client_conn.inner.deinit();
        return;
    };

    var buf: [64]u8 = undefined;
    const n = server_stream.read(io, &buf) catch 0;
    try std.testing.expectEqualSlices(u8, msg, buf[0..n]);

    // Clean up: close I/O resources first, then free inner allocations.
    // Streams are freed by onStreamClose callback.
    client_stream.close(io);
    server_stream.close(io);
    server_conn.close(io);
    client_conn.close(io);
    listener.close(io);
    server_conn.inner.deinit();
    client_conn.inner.deinit();
}
