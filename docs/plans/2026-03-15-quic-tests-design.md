# Design: Additional QUIC Integration Tests

## Goal

Complete Task 10 (QUIC integration tests) and add full coverage for untested API surface. Six new tests plus a test helper to reduce boilerplate.

## Test Helper: `TestContext`

A struct that sets up a server+client engine pair with TLS keys, performs the handshake, and provides cleanup. Reduces each integration test from ~80 lines to ~20 lines.

```
TestContext {
    server_eng, client_eng: *QuicEngine
    server_conn, client_conn: *QuicConnectionInner
    server_host_key, client_host_key: *ssl.EVP_PKEY
    allocator: Allocator

    initPair(allocator, io) -> TestContext   // setup + handshake
    deinit(self, io) -> void                 // ordered cleanup
}
```

## New Tests

### 1. "QUIC multiple concurrent streams"
- Open 3 streams from client, write distinct data on each
- Server accepts 3 streams, reads from each
- Verify each stream received correct data
- Covers: stream multiplexing, concurrent stream handling

### 2. "QUIC bidirectional peer ID verification"
- Verify both server_conn.peer_id and client_conn.peer_id are non-null
- Derive expected PeerId from each host key, compare against actual extracted peer_id
- Covers: customVerifyCallback + CertVerifyCtx for both onNewConn (server) and onHskDone (client)

### 3. "QUIC connection close cleanup"
- Handshake, open a stream, then close connection
- Assert conn.closed == true
- Assert acceptStream returns ConnectionClosed (stream_queue closed)
- Assert remotePeerId() still returns the peer_id after close
- Covers: resource cleanup, state after close

### 4. "QuicTransport dial and listen via multiaddr"
- QuicTransport.listen() on /ip4/127.0.0.1/udp/0/quic-v1
- QuicTransport.dial() to connect
- Open stream, exchange data, verify round-trip
- Covers: high-level transport API, multiaddr parsing, ownership

### 5. "ipAddressToSockaddr converts IPv4 correctly"
- Pure unit test, no IO needed
- Convert known IpAddress to SockaddrStorage
- Verify AF_INET family, port (big-endian), address bytes at correct offsets
- Covers: sockaddr layout correctness

### 6. "QUIC large message spanning multiple reads"
- Send a 16KB message from client
- Server reads in a loop accumulating chunks until all bytes received
- Verify data integrity (content matches)
- Covers: QUIC fragmentation, multi-packet messages, read loop pattern

## Files Modified

- `src/transport/quic/quic.zig` — add TestContext + 5 integration tests + 1 transport API test
- `src/transport/quic/engine.zig` — add 1 unit test for ipAddressToSockaddr
