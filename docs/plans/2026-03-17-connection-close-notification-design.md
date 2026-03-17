# Connection-Close Peer Notification Design

## Problem

`handleInbound` in gossipsub's Service currently has `defer self.removePeer(peer_id)`,
directly coupling stream reading with peer lifecycle management. This matches none of
the major libp2p implementations:

| Implementation | Trigger | Who notifies gossipsub |
|---|---|---|
| rust-libp2p | Connection close | Swarm calls `on_connection_closed` (`remaining_established == 0`) |
| jvm-libp2p | Stream close | Netty `channelUnregistered` -> `P2PService.onPeerDisconnected()` |
| nim-libp2p | Both | Stream close -> mesh removal; Connection close -> full `unsubscribePeer` |
| cpp-libp2p | Stream I/O error | `Connectivity.banOrForget` -> `GossipCore.onPeerConnection(false)` |

All implementations route through an intermediary layer (Swarm, P2PService,
Connectivity, ConnManager). None have handleInbound directly call removePeer.

## Chosen Approach: rust-libp2p (Connection-Level via Switch)

The Switch owns the full connection lifecycle:

1. Extracts `peer_id` from `conn.remotePeerId()` (TLS-verified identity)
2. Passes `peer_id` in ctx to all stream handlers
3. Calls `onPeerDisconnected(peer_id)` on handlers when the connection closes

This matches rust-libp2p's `FromSwarm::ConnectionClosed` pattern where the Swarm
notifies `NetworkBehaviour` implementations of connection lifecycle events.

## Design

### Protocol Interface

`onPeerDisconnected` is optional. Checked with `@hasDecl` at comptime — not enforced
by `assertProtocolInterface`. Protocols that don't care about connection lifecycle
(ping, identify) simply don't implement it.

```zig
// In gossipsub Handler:
pub fn onPeerDisconnected(self: *Handler, peer_id: []const u8) void {
    self.svc.removePeer(peer_id);
}
```

### Switch.handleConnectionTask

Extracts peer_id from the connection, passes it in ctx to all stream handlers,
and notifies handlers when the connection closes via defer:

```zig
fn handleConnectionTask(self: *Self, io: Io, conn: anytype) void {
    var mutable_conn = conn;
    defer mutable_conn.close(io);

    const peer_id = mutable_conn.remotePeerId();
    defer self.notifyPeerDisconnected(peer_id);

    var stream_group: Io.Group = .init;
    while (true) {
        const s = mutable_conn.acceptStream(io) catch return;
        stream_group.async(io, Self.handleStreamTask, .{
            self, io, s, .{ .peer_id = peer_id },
        });
    }
}
```

### Switch.notifyPeerDisconnected

Comptime iteration over handlers. Only calls handlers that declare
`onPeerDisconnected`:

```zig
fn notifyPeerDisconnected(self: *Self, peer_id: anytype) void {
    inline for (config.protocols, 0..) |P, i| {
        if (@hasDecl(P, "onPeerDisconnected")) {
            if (peer_id) |pid| {
                self.handlers[i].onPeerDisconnected(pid);
            }
        }
    }
}
```

### Service.handleInbound

Remove `defer self.removePeer(peer_id)`. Becomes a pure reader — reads until
stream closes, then returns silently. Peer cleanup is the Switch's responsibility.

### Test Changes

Remove manual `svc.removePeer("test-client")` from the gossipsub Switch test.
The test's server fiber uses `dispatchStream` manually (not `handleConnectionTask`),
so it needs to call `sw.notifyPeerDisconnected` explicitly in the cleanup section —
simulating what `handleConnectionTask` would do with `defer`.

## Non-Goals

- Multi-connection tracking (`remaining_established` counter) — deferred until
  we support multiple connections per peer
- Stream-level notifications (nim-libp2p dual pattern) — QUIC connection close
  reliably propagates to all streams, so connection-level is sufficient
- Event bus / channel indirection (go-libp2p pattern) — unnecessary with
  single-threaded cooperative fibers
