# Switch-as-Swarm Design

## Problem

The Switch is a thin stream dispatcher. Tests manually create engines, bind sockets,
connect, accept, spawn fibers, and clean up 10+ resources. The ping test is 62%
boilerplate, the gossipsub test is 45% boilerplate. Every error path duplicates an
8-line cleanup block.

In go-libp2p/rust-libp2p, the Swarm/Host manages all of this internally. The public
API is `listen`, `dial`, `newStream`, `close`.

## Chosen Approach: Full Swarm

The Switch owns transports, engines, connections, and peer lifecycle. Single QUIC
engine for both server and client (lsquic supports bidirectional engines).

## Target API

```zig
var sw = try Node.init(allocator, .{ .host_key = key }, .{gossipsub.Handler{ .svc = svc }});
defer sw.deinit();

try sw.listen(io, listen_addr);
const peer_id = try sw.dial(io, remote_addr);
try sw.newStream(io, peer_id, gossipsub.Handler);

sw.close(io);
```

## Architecture

```
Switch
├── allocator
├── handlers: HandlerTuple
├── engine: ?*QuicEngine  (single engine, server+client)
├── connections: StringHashMap(*QuicConnection)  (peer_id -> connection)
├── background: Io.Group  (accept loops, connection tasks)
└── config: EngineConfig
```

### Single Engine

lsquic supports bidirectional engines. One engine handles both accepting inbound
connections and initiating outbound connections. One UDP socket. One set of
background loops (receive + timer).

- `listen(io, addr)` -- create engine if needed, bind to addr, start accept loop
- `dial(io, addr)` -- create engine if needed (bind ephemeral), connect, register
- Engine is created lazily on first `listen` or `dial`

### Connection Tracking

`connections: StringHashMap(*QuicConnection)` maps peer_id bytes to engine-level
connections.

- `handleConnectionTask` registers on entry, deregisters on exit (via defer)
- `newStream` looks up connection by peer_id
- `notifyPeerDisconnected` fires on deregistration (existing mechanism)

### Public Methods

| Method | What it does |
|---|---|
| `init(allocator, engine_config, handlers)` | Creates Switch. No engine yet. |
| `listen(io, multiaddr)` | Creates engine if needed, binds, starts accept loop |
| `dial(io, multiaddr)` | Creates engine if needed, connects, spawns connection task, returns peer_id |
| `newStream(io, peer_id, P)` | Looks up connection, opens stream, negotiates protocol, runs handleOutbound |
| `listenAddrs()` | Returns bound addresses (for tests with port 0) |
| `close(io)` | Stops engine, cancels background fibers, notifies all peers disconnected |
| `deinit()` | Frees allocator resources |

### Kept Methods (internal or advanced use)

| Method | Status |
|---|---|
| `dispatchStream` | Kept public -- used by handleStreamTask and useful for testing |
| `getHandler` | Kept public -- access handler state |
| `notifyPeerDisconnected` | Kept public -- used by close() and tests |
| `handleConnectionTask` | Private -- spawned by listen/dial |
| `handleStreamTask` | Private -- spawned by handleConnectionTask |
| `serve` | Removed -- replaced by listen() |
| `openStream` | Renamed to newStream with peer_id-based lookup |

### What the gossipsub test becomes

```zig
test "Switch gossipsub subscription over QUIC" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const key1 = tls.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(key1);
    const key2 = tls.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(key2);

    const svc1 = gossipsub.Service.init(allocator, .{}) catch return;
    defer svc1.deinit();
    const svc2 = gossipsub.Service.init(allocator, .{}) catch return;
    defer svc2.deinit();
    svc2.subscribe("test-topic") catch return;

    var sw1 = Node.init(allocator, .{ .host_key = key1 }, .{gossipsub.Handler{ .svc = svc1 }});
    defer sw1.deinit();
    sw1.listen(io, listen_addr) catch return;

    var sw2 = Node.init(allocator, .{ .host_key = key2 }, .{gossipsub.Handler{ .svc = svc2 }});
    defer sw2.deinit();
    const peer_id = sw2.dial(io, sw1.listenAddrs()) catch return;
    sw2.newStream(io, peer_id, gossipsub.Handler) catch return;

    sleep(io, 100);

    const events = svc1.drainEvents() catch return;
    defer allocator.free(events);
    // verify subscription_changed...

    sw2.close(io);
    sw1.close(io);
}
```

~30 lines vs ~200 lines. No manual engine setup, socket binding, connection
establishment, fiber spawning, or error-cleanup blocks.

## QuicTransport Changes

The current `QuicTransport` creates one engine per `dial()`. For the Swarm:

- Remove `QuicTransport` as a standalone struct -- the Switch manages the engine
  directly
- OR: refactor `QuicTransport` to accept a shared engine

For simplicity, the Switch can manage the `QuicEngine` directly since we only have
one transport (QUIC). `SwitchConfig.transports` remains for compile-time validation.
When TCP is added later, we can generalize.

## Non-Goals

- Multi-connection per peer tracking (`remaining_established`) -- deferred
- Connection deduplication (simultaneous dial) -- deferred
- Peer discovery integration -- Phase 6
- TCP transport -- Phase 7
