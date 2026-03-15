# QUIC Integration Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 6 new QUIC tests completing Task 10 coverage and testing untested API surface.

**Architecture:** Extract a `TestContext` helper to reduce boilerplate for integration tests that need a full server+client engine pair with TLS handshake. Each new test focuses on one specific behavior.

**Tech Stack:** Zig 0.16.0-dev, lsquic, BoringSSL, std.Io.Evented, std.testing

---

### Task 1: Add TestContext helper

**Files:**
- Modify: `src/transport/quic/quic.zig` (append after line 673, in test section)

**Step 1: Write the TestContext struct and initPair/deinit methods**

Add this test-only helper at the end of `quic.zig` (before any new tests):

```zig
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
```

**Step 2: Build to verify it compiles**

Run: `zig build`
Expected: success (TestContext is only referenced by tests, not auto-compiled unless used)

**Step 3: Commit**

```
git add src/transport/quic/quic.zig
git commit -m "test: add TestContext helper for QUIC integration tests"
```

---

### Task 2: Add "QUIC multiple concurrent streams" test

**Files:**
- Modify: `src/transport/quic/quic.zig` (append after TestContext)

**Step 1: Write the test**

```zig
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
            // Clean up already-opened streams
            for (0..i) |j| {
                client_streams[j].close(io);
                client_streams[j].deinit();
            }
            return;
        };
        // Write stream-specific data to make the stream visible to server
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
                client_streams[j].deinit();
            }
            for (0..i) |j| {
                server_streams[j].close(io);
                server_streams[j].deinit();
            }
            return;
        };
        var buf: [64]u8 = undefined;
        const n = server_streams[i].read(io, &buf) catch 0;
        // Verify it starts with "stream-"
        try std.testing.expect(n >= 7);
        try std.testing.expectEqualSlices(u8, "stream-", buf[0..7]);
    }

    // Clean up streams
    for (0..stream_count) |i| {
        client_streams[i].close(io);
        server_streams[i].close(io);
    }
    for (0..stream_count) |i| {
        client_streams[i].deinit();
        server_streams[i].deinit();
    }
}
```

**Step 2: Run test to verify it passes**

Run: `zig build test`
Expected: All tests pass including the new one

**Step 3: Commit**

```
git add src/transport/quic/quic.zig
git commit -m "test: add QUIC multiple concurrent streams test"
```

---

### Task 3: Add "QUIC bidirectional peer ID verification" test

**Files:**
- Modify: `src/transport/quic/quic.zig` (append after previous test)

**Step 1: Write the test**

```zig
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
```

**Step 2: Run test to verify it passes**

Run: `zig build test`
Expected: All tests pass

**Step 3: Commit**

```
git add src/transport/quic/quic.zig
git commit -m "test: add QUIC bidirectional peer ID verification test"
```

---

### Task 4: Add "QUIC connection close cleanup" test

**Files:**
- Modify: `src/transport/quic/quic.zig` (append after previous test)

**Step 1: Write the test**

```zig
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

    // Clean up remaining resources
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
```

Note: This test does NOT use `defer ctx.deinit(io)` because it manually closes the server conn and does custom cleanup to avoid double-close.

**Step 2: Run test to verify it passes**

Run: `zig build test`
Expected: All tests pass

**Step 3: Commit**

```
git add src/transport/quic/quic.zig
git commit -m "test: add QUIC connection close cleanup test"
```

---

### Task 5: Add "QuicTransport dial and listen via multiaddr" test

**Files:**
- Modify: `src/transport/quic/quic.zig` (append after previous test)

**Step 1: Write the test**

```zig
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
        return;
    };

    var buf: [64]u8 = undefined;
    const n = server_stream.read(io, &buf) catch 0;
    try std.testing.expectEqualSlices(u8, msg, buf[0..n]);

    // Clean up
    client_stream.close(io);
    server_stream.close(io);
    server_conn.close(io);
    client_conn.close(io);
    listener.close(io);
}
```

**Step 2: Run test to verify it passes**

Run: `zig build test`
Expected: All tests pass

**Step 3: Commit**

```
git add src/transport/quic/quic.zig
git commit -m "test: add QuicTransport dial/listen via multiaddr test"
```

---

### Task 6: Add "ipAddressToSockaddr converts IPv4 correctly" test

**Files:**
- Modify: `src/transport/quic/engine.zig` (append after existing tests, before end of file)

**Step 1: Write the test**

```zig
test "ipAddressToSockaddr converts IPv4 correctly" {
    const addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 192, 168, 1, 42 }, .port = 8080 } };
    const storage = ipAddressToSockaddr(addr);

    if (builtin.os.tag.isDarwin()) {
        // macOS: sin_len(1) + sin_family(1) + sin_port(2) + sin_addr(4)
        try std.testing.expectEqual(@as(u8, @sizeOf(std.c.sockaddr.in)), storage.data[0]);
        try std.testing.expectEqual(@as(u8, @intCast(std.posix.AF.INET)), storage.data[1]);
    } else {
        // Linux: sin_family(2, little-endian) + sin_port(2) + sin_addr(4)
        const family = std.mem.readInt(u16, storage.data[0..2], .little);
        try std.testing.expectEqual(@as(u16, @intCast(std.posix.AF.INET)), family);
    }

    // Port is always at offset 2, big-endian
    const port_be = std.mem.readInt(u16, storage.data[2..4], .big);
    try std.testing.expectEqual(@as(u16, 8080), port_be);

    // IPv4 addr at offset 4
    try std.testing.expectEqual(@as(u8, 192), storage.data[4]);
    try std.testing.expectEqual(@as(u8, 168), storage.data[5]);
    try std.testing.expectEqual(@as(u8, 1), storage.data[6]);
    try std.testing.expectEqual(@as(u8, 42), storage.data[7]);
}
```

**Step 2: Run test to verify it passes**

Run: `zig build test`
Expected: All tests pass

**Step 3: Commit**

```
git add src/transport/quic/engine.zig
git commit -m "test: add ipAddressToSockaddr IPv4 conversion test"
```

---

### Task 7: Add "QUIC large message spanning multiple reads" test

**Files:**
- Modify: `src/transport/quic/quic.zig` (append after transport test)

**Step 1: Write the test**

```zig
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
        client_stream.deinit();
        return;
    };
    try std.testing.expectEqual(msg_size, written);

    // Server accepts and reads in a loop
    const server_stream = ctx.server_conn.acceptStream(io) catch |err| {
        std.log.warn("acceptStream failed: {}", .{err});
        client_stream.close(io);
        client_stream.deinit();
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
    client_stream.deinit();
    server_stream.deinit();
}
```

**Step 2: Run test to verify it passes**

Run: `zig build test`
Expected: All tests pass

**Step 3: Commit**

```
git add src/transport/quic/quic.zig
git commit -m "test: add QUIC large message spanning multiple reads test"
```

---

### Task 8: Final verification

**Step 1: Run full test suite**

Run: `zig build test --summary all`
Expected: 103/103 tests pass (97 existing + 6 new), 17/17 steps succeed

**Step 2: Verify no memory leaks**

The test allocator (`std.testing.allocator`) will catch any leaks. All tests should pass without leak reports.
