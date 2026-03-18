# Interop Testing Design (Phase 5)

Date: 2026-03-18

## Goal

Port the existing interop test binary (`interop/transport/main.zig`) from the old API (Zig 0.15.2 + libxev) to the new API (Zig 0.16.0-dev + std.Io + comptime Switch). Test QUIC-v1 + ping against go-libp2p and rust-libp2p via the official `libp2p/test-plans` framework.

## Background

The project already has a working interop binary and Dockerfile from before the rewrite. It follows the standard `libp2p/test-plans` transport interop protocol:

- **Env vars**: `TRANSPORT`, `IS_DIALER`, `REDIS_ADDR`, `IP`, `TEST_TIMEOUT_SECONDS`, etc.
- **Redis coordination**: Listener publishes `listenerAddr` via RPUSH, dialer reads via BLPOP.
- **JSON output**: Dialer prints `{"handshakePlusOneRTTMillis": <float>, "pingRTTMilllis": <float>}` to stdout.
- **Capability**: `quic-v1` transport only, no muxer, TLS security (native to QUIC).

eth-p2p-z is already registered in the test-plans `versionsInput.json` as `eth-p2p-z-v0.0.1`.

## What Changes

### 1. `interop/transport/main.zig` (~593 lines)

Port all imports and API calls from old to new:

| Old API | New API |
|---------|---------|
| `libp2p.thread_event_loop` | `std.Io` (entry point via `Io.run`) |
| `libp2p.swarm.Switch` | Comptime `Switch(.{ .transports = ..., .protocols = ... })` |
| `libp2p.transport.quic` | `src/transport/quic/quic.zig` |
| `libp2p.protocols.ping` | `src/protocol/ping.zig` (self-contained Handler) |
| `std.net.tcpConnectToHost` | `std.Io.net` TCP connect |
| `std.net.Stream` | `std.Io`-based read/write |
| `std.time.Timer` | `std.c.mach_absolute_time` / `clock_gettime` |

The Redis client (~150 lines) needs the biggest rewrite since `std.net` is removed in Zig 0.16. Replace with `Io.net.TcpConnection` or raw socket via Io.

### 2. `interop/transport/Dockerfile`

| Old | New |
|-----|-----|
| Zig 0.15.2 tarball | Zig 0.16.0-dev tarball (linux-x86_64) |
| `-Dlibxev-backend=epoll` | Remove (no libxev) |
| `zig build -Doptimize=ReleaseFast` | `zig build transport-interop -Doptimize=ReleaseFast` |

### 3. `build.zig`

Re-add the `transport-interop` build step that was removed during the rewrite. The executable imports from the main library module.

## What Stays the Same

- Environment variable protocol (same env var names and semantics)
- Redis coordination protocol (RPUSH/BLPOP `listenerAddr`)
- JSON output format
- Only `quic-v1` transport supported
- Ping protocol for the test
- Identify included alongside ping (rust-libp2p requires it)

## Key Challenge: Redis Client Without std.net

The existing Redis client uses `std.net.tcpConnectToHost` which no longer exists. Options:

1. **Use `std.Io.net` TCP** — matches our Io-based architecture, but the interop binary runs as a standalone executable (not inside the Switch's Io loop). We need to set up our own `Io.run` entry point.
2. **Use `std.c` POSIX sockets** — simpler for a one-off TCP connection, avoids Io complexity for Redis. The actual QUIC test still uses Io via the Switch.

Recommended: Use `std.c` POSIX sockets for Redis (simple, synchronous), and `Io.run` for the Switch/QUIC portion. This matches the existing architecture where Redis is just coordination plumbing.

## Test Flow

```
Listener:                          Dialer:
  1. Parse env vars                  1. Parse env vars
  2. Create Switch(ping)             2. Create Switch(ping)
  3. sw.listen(quic_addr)            3. BLPOP listenerAddr from Redis
  4. RPUSH listenerAddr to Redis     4. sw.dial(listener_addr) -> peer_id
  5. Sleep forever                   5. sw.newStream(peer_id, Ping)
                                     6. Print JSON {RTT metrics}
                                     7. Exit 0
```

## Validation

1. Build interop binary: `zig build transport-interop`
2. Local self-test: listener + dialer + local Redis
3. Docker build: `docker build -t eth-p2p-z-interop interop/transport/`
4. Cross-impl: Run via `libp2p/test-plans` orchestrator against go-libp2p-v0.38 and rust-libp2p-v0.54
