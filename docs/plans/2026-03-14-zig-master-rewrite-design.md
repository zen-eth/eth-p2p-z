# Architecture Rewrite Plan: zig-libp2p on Zig Master + std.Io

## Context

The zig-libp2p project (eth-p2p-z) needs a full ground-up rewrite to:
1. **Upgrade from Zig 0.15.2 to Zig 0.16.0-dev (master)** — the new `std.Io` interface (PR #25592) replaces the need for libxev entirely, providing built-in async/await, io_uring/kqueue backends, queues, groups, and cancellation.
2. **Use `std.Io.Evented` (not Threaded)** — use the event-loop backend: **io_uring on Linux**, **kqueue on macOS**. No thread-pool I/O.
3. **Replace callback-based async with suspension-based `std.Io`** — eliminates ~400 lines of manual `ThreadEventLoop`, MPSC queues, callback nesting, and thread marshalling.
4. **Replace VTable pattern with comptime protocol composition** — the protocol stack is known at compile time; comptime generics give zero-cost abstractions, inlining, and compile-time error checking.
5. **Focus on QUIC transport first, advance interop testing** — TCP and WebTransport deferred to later phases.
6. **Upgrade lsquic dependency** — adapt from `zen-eth/lsquic@anshalshukla/zig-0.15.2-upgrade` to work with Zig master.
7. **Remove TLS threadlocal hack** — use lsquic's `ea_verify_cert`/`ea_verify_ctx` API instead of OpenSSL `SSL_CTX_set_verify` + threadlocal state (inspired by nim-libp2p's approach).

## Architecture Overview

```
Layer 5: Application (user code)
Layer 4: Protocol Handlers (ping, identify, gossipsub, DHT, mDNS)
Layer 3: Protocol Negotiation (multistream-select)
Layer 2: Connection + Stream Multiplexing (QUIC native streams)
Layer 1: Security (TLS via lsquic, no threadlocal)
Layer 0: Transport (QUIC)
I/O:     std.Io.Evented (io_uring on Linux, kqueue on macOS)
```

**End-user API:**
```zig
const Node = Switch(.{
    .transports = .{QuicTransport},
    .protocols = .{
        PingProtocol,
        IdentifyProtocol,
        GossipsubProtocol,
    },
});

pub fn main() !void {
    var evented: std.Io.Evented = .init(.{});
    defer evented.deinit();
    const io = evented.io();

    var node = try Node.init(io, allocator, config);
    defer node.deinit();

    try node.listen("/ip4/0.0.0.0/udp/4001/quic-v1");
}
```

## Module Layout

```
src/
  root.zig                          -- public API re-exports

  io/
    adapter.zig                     -- std.Io utility wrappers, timeout helpers
    udp.zig                         -- UDP socket ops via std.Io (for lsquic)

  transport/
    transport.zig                   -- comptime Transport interface assertions
    quic/
      quic.zig                      -- QuicTransport, QuicConnection, QuicStream
      engine.zig                    -- lsquic engine wrapper (C interop + Io.Queue bridge)

  security/
    security.zig                    -- comptime Security interface assertions
    tls.zig                         -- BoringSSL cert gen/verify (no threadlocal, uses ea_verify_cert)
    identity.zig                    -- KeyPair, PeerId (ported from current src/identity.zig)

  protocol/
    protocol.zig                    -- comptime Protocol interface assertions
    multistream.zig                 -- Multistream-select negotiation (comptime generic)
    ping.zig                        -- Ping protocol
    identify.zig                    -- Identify protocol
    gossipsub/
      gossipsub.zig                 -- GossipSub v1.1
      mcache.zig                    -- Message cache
      router.zig                    -- Router state machine

  peer/
    peer.zig                        -- PeerId, PeerInfo
    store.zig                       -- Peer store / address book
    conn_manager.zig                -- Connection limits, pruning
    discovery/
      dht.zig                       -- Kademlia DHT
      mdns.zig                      -- mDNS discovery

  switch.zig                        -- Switch: comptime-composed orchestrator

  proto/                            -- Generated protobuf (keep gremlin)
    identify.proto.zig
    rpc.proto.zig

  util/
    linear_fifo.zig                 -- Buffer utility (reuse from current)
    protobuf.zig                    -- Protobuf helpers (reuse from current)
```

## Key Design Decisions

### 1. Comptime Transport Interface (duck-typing, no VTables)

Each transport provides concrete types checked at comptime:

```zig
// src/transport/transport.zig
pub fn assertTransportInterface(comptime T: type) void {
    if (!@hasDecl(T, "Connection")) @compileError("Transport missing Connection type");
    if (!@hasDecl(T, "Stream")) @compileError("Transport missing Stream type");
    if (!@hasDecl(T, "Listener")) @compileError("Transport missing Listener type");
    // Assert fn signatures: dial, listen, close, matchesMultiaddr
}
```

Required shape:
- `fn dial(self, io: *Io, addr: Multiaddr) DialError!Connection`
- `fn listen(self, io: *Io, addr: Multiaddr) ListenError!Listener`
- Connection: `openStream`, `acceptStream`, `close`, `remotePeerId`, `remoteAddr`
- Stream: `read(io, buf)`, `write(io, data)`, `close(io)`
- Listener: `accept(io)`, `close`, `localAddr`

### 2. std.Io.Evented Replaces ThreadEventLoop + libxev

**I/O backend: `std.Io.Evented`** — uses kqueue on macOS, io_uring on Linux. No thread pool. Single event-loop driven.

| Current (libxev + manual) | New (std.Io.Evented) |
|---------------------------|----------------------|
| `ThreadEventLoop` + OS thread | `std.Io.Evented` (kqueue/io_uring event loop) |
| MPSC queue + `xev.Async` notifier | `Io.Queue(T)` |
| Callback `fn(ctx: ?*anyopaque, res)` | Direct return values (suspension-based) |
| `xev.Timer` for scheduling | `io.sleep(Duration)` |
| `xev.TCP` for socket ops | `io.tcpConnect()`, `io.tcpBind()` |
| `xev.UDP` for QUIC packet I/O | `io.recvFrom()`, `io.sendTo()` |
| `xev.Completion` pools | Not needed (std.Io manages internally) |
| Manual thread dispatch | `io.async()` / `io.concurrent()` |

**Files deleted:** `thread_event_loop.zig`, `thread_event_loop/tasks/*.zig`, `xev_backend.zig`, `concurrent/*.zig`

### 3. lsquic-to-std.Io Bridge (Io.Queue pattern)

The critical bridge between lsquic's C callbacks and std.Io's suspension model:

1. **Engine loop task** runs `io.recvFrom()` on UDP socket, feeds packets to `lsquic_engine_packet_in()`, calls `lsquic_engine_process_conns()`.
2. **lsquic callbacks** (`onNewConn`, `onNewStream`, `onStreamRead`) push events into per-connection/per-stream `Io.Queue`s.
3. **Application code** calls `queue.getOne(io)` which suspends until the event arrives.

```zig
// QuicEngine drives lsquic via std.Io
pub fn run(self: *QuicEngine, io: *Io) !void {
    var group = io.group();
    try group.async(self.udpReceiveLoop, .{io});    // UDP read -> lsquic
    try group.async(self.engineProcessLoop, .{io});  // Timer-driven process_conns
    try group.wait();
}

// Per-stream queues prevent cross-talk
pub const QuicStream = struct {
    read_channel: Io.Queue(ReadEvent),
    pub fn read(self: *QuicStream, io: *Io, buf: []u8) !usize {
        const event = self.read_channel.getOne(io);
        @memcpy(buf[0..event.data.len], event.data);
        return event.data.len;
    }
};
```

### 4. Removing TLS threadlocal Hack (ea_verify_cert approach)

**Current problem** (`src/security/tls.zig:28`): A `threadlocal var g_peer_cert` bridges two callbacks:
- OpenSSL's `SSL_CTX_set_verify` callback captures the peer cert during TLS handshake
- lsquic's `on_hsk_done` callback needs the cert to extract PeerId

The threadlocal works because both callbacks run on the same thread, but it's fragile and won't work with `std.Io.Evented`.

**Solution: Use lsquic's `ea_verify_cert`/`ea_verify_ctx`** (already in the API but currently unused):

```zig
// lsquic_engine_api has:
//   int (*ea_verify_cert)(void *verify_ctx, struct stack_st_X509 *chain);
//   void *ea_verify_ctx;

const CertVerifyCtx = struct {
    allocator: Allocator,
    // Per-engine map: lsquic_conn_t -> verified PeerInfo
    verified_peers: std.AutoHashMap(*lsquic.lsquic_conn_t, PeerInfo),
    mutex: std.Thread.Mutex = .{},
};

fn verifyCert(verify_ctx: ?*anyopaque, chain: ?*ssl.stack_st_X509) callconv(.c) c_int {
    const ctx: *CertVerifyCtx = @ptrCast(@alignCast(verify_ctx));
    // 1. Extract first cert from chain
    // 2. Verify libp2p extension (signature, time validity)
    // 3. Extract PeerId from cert
    // 4. Store in ctx.verified_peers keyed by conn pointer
    // 5. Return 0 on success, -1 on failure
    // No threadlocal needed — context flows through ea_verify_ctx
}

// In engine_api setup:
const engine_api: lsquic.lsquic_engine_api = .{
    // ...existing fields...
    .ea_verify_cert = verifyCert,
    .ea_verify_ctx = cert_verify_ctx,
};
```

**Inspiration from nim-libp2p**: nim-libp2p passes TLS config (cert, key, verifier) as explicit object fields through the call chain — `QuicTransport.makeConfig()` produces a `TLSConfig` passed directly to `QuicServer.new()` and `QuicClient.new()`. No globals or threadlocals. Our approach follows the same principle: the verify context is an explicit struct pointer passed through lsquic's `ea_verify_ctx`.

### 5. Switch Comptime Composition (QUIC-focused)

```zig
pub fn Switch(comptime config: SwitchConfig) type {
    // Validate at comptime
    inline for (config.transports) |T| comptime assertTransportInterface(T);
    inline for (config.protocols) |P| comptime assertProtocolInterface(P);

    return struct {
        transports: TransportTuple(config.transports),  // concrete tuple, no vtable
        peer_store: PeerStore,
        conn_manager: ConnManager,
        io: *Io,

        pub fn listen(self, addr_str: []const u8) !void {
            const ma = try Multiaddr.parse(addr_str);
            inline for (config.transports, 0..) |T, i| {
                if (T.matchesMultiaddr(ma)) {
                    const listener = try self.transports[i].listen(self.io, ma);
                    try self.io.async(acceptLoop, .{ self, listener });
                    return;
                }
            }
            return error.NoMatchingTransport;
        }

        fn handleInboundStream(self, stream: anytype) !void {
            const proto_id = try Multistream.negotiateInbound(self.io, stream, self.supportedIds());
            inline for (config.protocols) |P| {
                if (std.mem.eql(u8, proto_id, P.id)) {
                    try P.handleInbound(self.io, stream, self.protocolCtx());
                    return;
                }
            }
        }
    };
}
```

### 6. One Runtime VTable: AnyStream

The only place runtime dispatch remains — when the application needs to store streams from different transports in one collection:

```zig
pub const AnyStream = struct {
    ptr: *anyopaque,
    readFn: *const fn (*anyopaque, *Io, []u8) anyerror!usize,
    writeFn: *const fn (*anyopaque, *Io, []const u8) anyerror!usize,
    closeFn: *const fn (*anyopaque, *Io) void,

    pub fn wrap(comptime StreamT: type, stream: *StreamT) AnyStream { ... }
};
```

### 7. Concrete Error Sets (no anyerror)

Every transport/protocol declares typed error sets:
```zig
pub const DialError = error{ AddressResolutionFailed, ConnectionRefused, TlsHandshakeFailed, PeerIdMismatch, Timeout } || Allocator.Error;
```

## Reusable Components from Current Codebase

| Component | Current Path | Reuse Strategy |
|-----------|-------------|----------------|
| KeyPair / Identity | `src/identity.zig` | Port directly, move to `src/security/identity.zig` |
| TLS cert gen/verify | `src/security/tls.zig` | Port cert functions, remove threadlocal, use `ea_verify_cert` |
| secp256k1 context | `src/secp_context.zig` | Port directly |
| Protobuf types | `src/proto/*.proto.zig` | Keep |
| Gremlin code gen | `build.zig` ProtoGenStep | Keep |
| LinearFifo | `src/linear_fifo.zig` | Port to `src/util/` |
| Gossipsub router logic | `src/protocols/pubsub/routers/gossipsub.zig` | Port mesh/fanout/heartbeat logic, adapt I/O |
| Message cache | `src/protocols/pubsub/routers/mcache.zig` | Port directly |
| Multiaddr parsing | dependency: `multiaddr-zig` | Keep dependency |
| Protobuf parsing | dependency: `gremlin` | Keep dependency |
| Cache | dependency: `cache.zig` | Keep dependency |

## Implementation Phases

### Phase 1: Foundation
- Set up Zig 0.16.0-dev build, update `build.zig.zon`
- Remove libxev dependency, add `std.Io.Evented` initialization
- Port `identity.zig`, `secp_context.zig`
- Port `tls.zig` cert gen/verify functions (remove threadlocal, reorganize for `ea_verify_cert`)
- Implement comptime interface assertions (`transport.zig`, `security.zig`, `protocol.zig`)
- Implement `multistream.zig` as comptime generic

### Phase 2: QUIC Transport
- Upgrade lsquic to build with Zig master (from `anshalshukla/zig-0.15.2-upgrade` branch)
- Implement `QuicEngine` with `Io.Queue` bridge pattern (evented I/O, not threaded)
- Implement `QuicTransport`, `QuicConnection`, `QuicStream`
- Implement TLS via `ea_verify_cert`/`ea_verify_ctx` (no threadlocal)
- UDP I/O via `std.Io.Evented`
- Integration tests: QUIC dial, listen, stream I/O

### Phase 3: Core Protocols
- Port `PingProtocol` to comptime generic shape
- Port `IdentifyProtocol`
- Port `GossipsubProtocol` (v1.1)
- Test all over QUIC

### Phase 4: Switch + Peer Management
- Implement comptime `Switch(config)` with transport dispatch
- Peer store and connection manager
- Accept loops, dial paths, protocol dispatch
- End-to-end: Switch + QUIC + ping + identify + gossipsub

### Phase 5: Interop Testing (priority — advanced)
- Update interop tests (Docker, Redis coordination)
- Cross-implementation interop with go-libp2p and rust-libp2p
- Test all protocol combinations: ping, identify, gossipsub
- Performance benchmarking vs current implementation

### Phase 6: Peer Discovery
- Implement Kademlia DHT basics
- Implement mDNS discovery
- Integrate with Switch and peer store

### Phase 7 (deferred): TCP Transport
- Implement `TcpTransport` with `std.Io.Evented` TCP ops
- Implement Noise XX handshake (`noise.zig`)
- Implement Yamux stream multiplexer (`yamux.zig`)
- Compose: `TcpTransport(.{ .security = NoiseProtocol, .muxer = YamuxMuxer })`

### Phase 8 (deferred): WebTransport
- Implement libp2p WebTransport (layered on QUIC + HTTP/3)
- Cross-transport testing

## Verification

1. **Build**: `zig build` succeeds with Zig 0.16.0-dev using `std.Io.Evented`
2. **Unit tests**: `zig build test` — test each module in isolation using mock streams
3. **Integration tests**: QUIC loopback dial/listen/ping/identify/gossipsub
4. **Interop tests**: Docker-based testing against go-libp2p and rust-libp2p (existing framework, advanced early)
5. **TLS verification**: Confirm `ea_verify_cert` works without threadlocal — verify PeerId extraction from cert chain
6. **Compile-time checks**: Intentionally mis-configured `Switch()` produces clear comptime errors
7. **Platform tests**: Verify `std.Io.Evented` works on both macOS (kqueue) and Linux (io_uring)

## Dependencies Update

| Dependency | Action |
|-----------|--------|
| libxev | **Remove** (replaced by std.Io) |
| lsquic | **Upgrade** to Zig master (fork from `anshalshukla/zig-0.15.2-upgrade`, adapt build.zig) |
| boringssl | Keep (via lsquic) |
| gremlin | Keep |
| zmultiformats | Keep |
| multiaddr | Keep |
| secp256k1 | Keep |
| cache | Keep |
