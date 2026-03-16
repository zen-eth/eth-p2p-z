# Protocol Handler Instances + GossipSub Service Design

**Date:** 2026-03-16

**Goal:** Refactor protocols from free functions to handler instances (like go-libp2p's method values), then build a GossipSub service layer that manages per-peer stream lifecycle on top of Switch.

## Problem

1. **Protocol context**: `Switch.dispatchStream` hardcodes `.{ .allocator = self.allocator }` as context. Identify needs `.identify = Config`, gossipsub needs router state, peer maps, etc. Each protocol has different state requirements.

2. **Gossipsub is bidirectional**: Unlike ping/identify (single-stream request-response), gossipsub uses two long-lived streams per peer (inbound for reading, outbound for writing). The current `handleInbound`/`handleOutbound` protocol interface doesn't fit.

3. **No integration tests**: Only ping has a Switch + QUIC integration test. Identify and gossipsub have no end-to-end tests.

## Prior Art

Both go-libp2p and rust-libp2p follow the same pattern:
- **The Swarm/Host is a dumb dispatcher** — it knows protocol IDs and routes streams.
- **Protocol handlers are long-lived objects** that hold their own config and state.
- go-libp2p: `idService` struct with `Host.SetStreamHandler(ID, ids.handleIdentifyRequest)` — method value captures receiver.
- rust-libp2p: `NetworkBehaviour` holds config, creates per-connection `Handler` instances.

## Design

### Part 1: Handler Instance Protocol Interface

Replace free function protocols with handler structs. Each protocol type becomes a struct with methods:

```zig
// Before (free functions):
pub fn handleInbound(io: Io, stream: anytype, ctx: anytype) !void {
    const allocator = ctx.allocator;
    const config = ctx.identify;
    ...
}

// After (handler instance):
pub const Handler = struct {
    allocator: Allocator,
    config: Config,

    pub const id = "/ipfs/id/1.0.0";

    pub fn handleInbound(self: *Handler, io: Io, stream: anytype) !void {
        // self.config, self.allocator available directly
        ...
    }

    pub fn handleOutbound(self: *Handler, io: Io, stream: anytype, ctx: anytype) !void {
        ...
    }
};
```

**Key changes:**
- `handleInbound(io, stream, ctx)` → `handleInbound(self: *Self, io, stream)` — self holds all context
- `handleOutbound(io, stream, ctx)` → `handleOutbound(self: *Self, io, stream, ctx)` — ctx is for per-call data only (e.g., ping payload)
- Protocol interface validation updated to check for self parameter

### Part 2: Switch Stores Handler Tuple

The Switch struct holds an instance of each protocol handler:

```zig
pub fn Switch(comptime config: SwitchConfig) type {
    return struct {
        allocator: Allocator,
        handlers: HandlerTuple,  // tuple of handler instances

        const HandlerTuple = comptime blk: {
            // Generate struct with one field per protocol
            // Field names derived from protocol IDs or indices
        };

        pub fn init(allocator: Allocator, handlers: HandlerTuple) Self {
            return .{ .allocator = allocator, .handlers = handlers };
        }

        pub fn dispatchStream(self: *Self, io: Io, s: anytype) !void {
            const proto_id = try multistream.negotiateInbound(io, s, &supported_protocol_ids);
            inline for (config.protocols, 0..) |P, i| {
                if (std.mem.eql(u8, proto_id, P.id)) {
                    // Call handleInbound on the handler INSTANCE
                    self.handlers[i].handleInbound(io, s);
                    return;
                }
            }
        }

        pub fn openStream(self: *Self, io: Io, conn: anytype, comptime P: type, ctx: anytype) !void {
            // Find handler index, call P.handleOutbound on the instance
            inline for (config.protocols, 0..) |Proto, i| {
                if (Proto == P) {
                    var s = try conn.openStream(io);
                    const stream = streamRef(&s);
                    _ = try multistream.negotiateOutbound(io, stream, &.{P.id});
                    self.handlers[i].handleOutbound(io, stream, ctx);
                    return;
                }
            }
        }
    };
}
```

### Part 3: Ping and Identify as Handler Instances

**Ping** (stateless — trivial wrapper):
```zig
pub const Handler = struct {
    pub const id = "/ipfs/ping/1.0.0";

    pub fn handleInbound(_: *Handler, io: Io, stream: anytype) !void {
        var buf: [payload_length]u8 = undefined;
        readExact(io, stream, &buf) catch return error.UnexpectedEof;
        writeAll(io, stream, &buf) catch return error.UnexpectedEof;
    }

    pub fn handleOutbound(_: *Handler, io: Io, stream: anytype, ctx: anytype) !void {
        const payload = ctx.payload;
        writeAll(io, stream, payload) catch return error.UnexpectedEof;
        var response: [payload_length]u8 = undefined;
        readExact(io, stream, &response) catch return error.UnexpectedEof;
        if (!std.mem.eql(u8, payload, &response)) return error.PayloadMismatch;
    }
};
```

**Identify** (holds config):
```zig
pub const Handler = struct {
    allocator: Allocator,
    config: Config,

    pub const id = "/ipfs/id/1.0.0";

    pub fn handleInbound(self: *Handler, io: Io, stream: anytype) !void {
        // Uses self.allocator, self.config to encode and send identity
    }

    pub fn handleOutbound(self: *Handler, io: Io, stream: anytype, _: anytype) !void {
        // Uses self.allocator to read and decode remote identity
    }
};
```

### Part 4: GossipSub Service Layer

GossipSub does **not** fit the simple `handleInbound`/`handleOutbound` pattern. It needs a **service layer** that:
1. Holds the Router and per-peer stream maps
2. Implements `handleInbound` for Switch dispatch (starts read loop per inbound stream)
3. Implements the Router's `Handler.sendRpc` to open/reuse outbound streams
4. Manages heartbeat timer via cooperative fiber

```zig
pub fn Service(comptime config: gossipsub.Config) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        router: Router(Self),
        // Per-peer outbound stream: peer opens stream to us = inbound,
        // we open stream to peer = outbound. Semi-duplex model.
        outbound_streams: PeerStreamMap,
        connections: PeerConnMap,

        pub const id = config.protocol_id;  // "/meshsub/1.2.0"

        /// Called by Switch.dispatchStream when a peer opens a gossipsub stream to us.
        /// Starts a long-lived read loop that feeds frames into the Router.
        pub fn handleInbound(self: *Self, io: Io, stream: anytype) !void {
            const peer_id = ...; // from connection context
            var decoder = codec.FrameDecoder.init(self.allocator);
            defer decoder.deinit();

            while (true) {
                var buf: [4096]u8 = undefined;
                const n = stream.read(io, &buf) catch break;
                if (n == 0) break;
                decoder.feed(buf[0..n]);
                while (decoder.next()) |frame| {
                    self.router.handleRpc(peer_id, frame);
                }
            }
            self.router.removePeer(peer_id);
        }

        /// Router's Handler interface: send RPC bytes to a peer.
        /// Opens outbound stream lazily, writes pre-encoded frame.
        pub fn sendRpc(self: *Self, peer: []const u8, data: []const u8) bool {
            const stream = self.outbound_streams.get(peer) orelse {
                // Open new outbound stream, negotiate multistream
                // Cache in outbound_streams
                ...
            };
            stream.write(self.io, data) catch return false;
            return true;
        }

        /// Periodic heartbeat via cooperative fiber.
        pub fn startHeartbeat(self: *Self, io: Io) void {
            // Io.Timer or loop with Io.sleep
            while (true) {
                io.sleep(config.heartbeat_interval_ms);
                self.router.heartbeat();
            }
        }
    };
}
```

### Part 5: Integration Tests

1. **Identify over QUIC**: Same pattern as ping integration test. Server dispatches identify inbound (sends its identity), client opens stream and reads identity.

2. **GossipSub over QUIC**: Two nodes, subscribe to same topic, one publishes, other receives via Router event handler. Tests the full semi-duplex stream lifecycle.

## Implementation Order

1. Refactor protocol interface: handler instances with `self: *Self`
2. Update ping to Handler struct
3. Update identify to Handler struct
4. Update Switch to store handler tuple, dispatch to instances
5. Update protocol interface validation (`assertProtocolInterface`)
6. Add identify integration test (Switch + QUIC)
7. Implement gossipsub Service wrapper
8. Add gossipsub integration test (Switch + QUIC)
9. Final verification

## Non-Goals

- No changes to the Router itself — it stays transport-agnostic
- No changes to the codec — it already has io passthrough
- No multi-protocol negotiation (each stream negotiates one protocol)
- No connection-level lifecycle hooks in Switch (gossipsub manages this in its Service)
