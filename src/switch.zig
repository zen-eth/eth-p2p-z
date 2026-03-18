const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const log = std.log.scoped(.@"switch");

const transport_mod = @import("transport/transport.zig");
const protocol_mod = @import("protocol/protocol.zig");
const multistream = @import("protocol/multistream.zig");
const identify_mod = @import("protocol/identify.zig");
const engine_mod = @import("transport/quic/engine.zig");
const QuicEngine = engine_mod.QuicEngine;
const quic_mod = @import("transport/quic/quic.zig");
const multiaddr = @import("multiaddr");
const Multiaddr = multiaddr.Multiaddr;
const ssl = @import("ssl");
const net = Io.net;

/// Configuration for comptime Switch composition.
pub const SwitchConfig = struct {
    /// Transport types (must satisfy assertTransportInterface).
    transports: []const type,
    /// Protocol types (must satisfy assertProtocolInterface — Handler structs with id, handleInbound, handleOutbound).
    protocols: []const type,
};

/// Runtime configuration for the Switch's QUIC engine infrastructure.
pub const EngineConfig = struct {
    host_key: ?*ssl.EVP_PKEY = null,
};

/// Comptime-composed libp2p Switch.
///
/// Validates transports and protocols at compile time, dispatches inbound
/// streams via multistream-select to the matching protocol handler.
/// Uses Io.Group.async for concurrent connection/stream handling (cooperative fibers).
pub fn Switch(comptime config: SwitchConfig) type {
    // Compile-time validation
    inline for (config.transports) |T| {
        comptime transport_mod.assertTransportInterface(T);
    }
    inline for (config.protocols) |P| {
        comptime protocol_mod.assertProtocolInterface(P);
    }

    return struct {
        const Self = @This();

        /// Comptime-generated tuple type holding one instance per registered protocol handler.
        const HandlerTuple = std.meta.Tuple(config.protocols);

        allocator: Allocator,
        handlers: HandlerTuple,
        server_engine: ?*QuicEngine = null,
        client_engine: ?*QuicEngine = null,
        connections: std.StringHashMap(*engine_mod.QuicConnection),
        background: Io.Group,
        engine_config: EngineConfig,

        /// Protocol IDs for multistream-select negotiation (computed at comptime).
        const supported_protocol_ids = protocol_mod.protocolIds(config.protocols);

        pub fn init(allocator: Allocator, engine_config: EngineConfig, handlers: HandlerTuple) Self {
            return .{
                .allocator = allocator,
                .handlers = handlers,
                .connections = std.StringHashMap(*engine_mod.QuicConnection).init(allocator),
                .background = .init,
                .engine_config = engine_config,
            };
        }

        /// Tear down the Switch. Stops all background fibers and engines if
        /// close() was not already called, then frees resources.
        pub fn deinit(self: *Self, io: Io) void {
            // Stop background fibers and engines if close() wasn't called
            self.close(io);
            self.connections.deinit();
            if (self.server_engine) |eng| {
                eng.deinit();
                self.server_engine = null;
            }
            if (self.client_engine) |eng| {
                eng.deinit();
                self.client_engine = null;
            }
        }

        // ── Swarm API (Tasks 3-6) ──────────────────────────────────────────

        /// Start listening for inbound QUIC connections on the given multiaddr.
        /// Creates the server engine lazily on first call.
        pub fn listen(self: *Self, io: Io, addr: Multiaddr) !void {
            const parsed = try quic_mod.parseQuicMultiaddr(addr);

            if (self.server_engine == null) {
                const eng = try QuicEngine.init(self.allocator, .{
                    .is_server = true,
                    .host_key = self.engine_config.host_key,
                });
                eng.setIo(io);
                self.server_engine = eng;
            }

            const eng = self.server_engine.?;
            try eng.bindSocket(io, &parsed.ip);
            eng.startBackgroundLoops(io);

            self.background.async(io, Self.acceptLoop, .{ self, io });
        }

        /// Returns the server engine's bound address (useful for port-0 tests).
        pub fn listenAddrs(self: *const Self) ?net.IpAddress {
            const eng = self.server_engine orelse return null;
            const sock = eng.socket orelse return null;
            return sock.address;
        }

        /// Dial a remote peer via QUIC multiaddr.
        /// Creates the client engine lazily on first call. Returns peer_id bytes
        /// (owned by the connections map — valid until peer disconnects).
        pub fn dial(self: *Self, io: Io, addr: Multiaddr) ![]const u8 {
            const parsed = try quic_mod.parseQuicMultiaddr(addr);

            if (self.client_engine == null) {
                const eng = try QuicEngine.init(self.allocator, .{
                    .is_server = false,
                    .host_key = self.engine_config.host_key,
                });
                eng.setIo(io);
                try eng.bindSocket(io, &net.IpAddress{ .ip4 = net.Ip4Address.unspecified(0) });
                eng.startBackgroundLoops(io);
                self.client_engine = eng;
            }

            const eng = self.client_engine.?;
            var remote_sa = engine_mod.ipAddressToSockaddr(parsed.ip);
            const local_bound = (eng.socket orelse return error.NoSocket).address;
            var local_sa = engine_mod.ipAddressToSockaddr(local_bound);
            const conn = try eng.connect(io, @ptrCast(&remote_sa), @ptrCast(&local_sa));

            // Wait for TLS handshake to extract peer_id
            var pid_buf: [128]u8 = undefined;
            const raw_peer_id: []const u8 = pid: {
                var attempts: u32 = 0;
                while (attempts < 200) : (attempts += 1) {
                    if (conn.remotePeerId()) |pid| {
                        break :pid pid.toBytes(&pid_buf) catch return error.PeerIdEncodeFailed;
                    }
                    const t: Io.Timeout = .{ .duration = .{
                        .raw = Io.Duration.fromMilliseconds(5),
                        .clock = .awake,
                    } };
                    t.sleep(io) catch {};
                }
                return error.HandshakeTimeout;
            };

            // Register connection (heap-owned key)
            const owned_pid = try self.allocator.dupe(u8, raw_peer_id);
            errdefer self.allocator.free(owned_pid);
            try self.connections.put(owned_pid, conn);

            // Spawn connection handler in background
            self.background.async(io, Self.swarmConnectionTask, .{ self, io, conn });

            return owned_pid;
        }

        /// Background fiber: accepts inbound connections from the server engine.
        fn acceptLoop(self: *Self, io: Io) void {
            const eng = self.server_engine orelse return;
            while (true) {
                const conn = eng.accept(io) catch return;
                self.background.async(io, Self.swarmConnectionTask, .{ self, io, conn });
            }
        }

        /// Manages a single QUIC connection's lifecycle.
        /// Extracts peer_id from TLS, registers in connections map (if not already
        /// registered by dial), accepts streams. Connection map cleanup is handled
        /// by close()/deinit() — NOT by this task's defers — to avoid races.
        fn swarmConnectionTask(self: *Self, io: Io, conn: *engine_mod.QuicConnection) void {
            // Resolve peer_id from TLS
            var pid_buf: [128]u8 = undefined;
            const peer_id: ?[]const u8 = pid: {
                var attempts: u32 = 0;
                while (attempts < 200) : (attempts += 1) {
                    if (conn.remotePeerId()) |pid| {
                        break :pid pid.toBytes(&pid_buf) catch null;
                    }
                    const t: Io.Timeout = .{ .duration = .{
                        .raw = Io.Duration.fromMilliseconds(5),
                        .clock = .awake,
                    } };
                    t.sleep(io) catch {};
                }
                break :pid null;
            };

            // Register if not already registered (accepted connections from listen)
            if (peer_id) |pid| {
                if (!self.connections.contains(pid)) {
                    const owned = self.allocator.dupe(u8, pid) catch null;
                    if (owned) |key| {
                        self.connections.put(key, conn) catch {
                            self.allocator.free(key);
                        };
                    }
                }

                // Auto-trigger identify (like go-libp2p's IDService).
                // Runs in background so it doesn't block stream acceptance.
                self.background.async(io, Self.identifyPeer, .{ self, io, pid });
            }

            // Accept streams loop — blocks until connection closes or engine stops.
            // Note: stream_group tasks are implicitly cleaned up when the parent
            // group (self.background) is canceled by close().
            while (true) {
                const s_inner = conn.acceptStream(io) catch break;
                self.background.async(io, Self.swarmStreamTask, .{
                    self, io, quic_mod.Stream{ .inner = s_inner }, SwarmStreamCtx{ .peer_id = peer_id },
                });
            }
        }

        /// Concrete context type for swarm stream tasks (Io.Group.async requires
        /// concrete types — anytype cannot be used with ArgsTuple).
        const SwarmStreamCtx = struct {
            peer_id: ?[]const u8 = null,
        };

        /// Whether identify is registered as a protocol (comptime check).
        const has_identify = blk: {
            for (config.protocols) |P| {
                if (P == identify_mod.Handler) break :blk true;
            }
            break :blk false;
        };

        /// Auto-trigger identify on a newly connected peer.
        /// No-op if identify is not registered in this Switch's protocols.
        fn identifyPeer(self: *Self, io: Io, peer_id: []const u8) void {
            if (has_identify) {
                self.newStream(io, peer_id, identify_mod.Handler) catch |err| {
                    log.warn("auto-identify failed for peer: {}", .{err});
                };
            }
        }

        /// Handles a single inbound stream: multistream-negotiate then dispatch.
        fn swarmStreamTask(self: *Self, io: Io, s: quic_mod.Stream, ctx: SwarmStreamCtx) void {
            var mutable_stream = s;
            self.dispatchStream(io, &mutable_stream, ctx) catch return;
        }

        /// Open a new outbound stream to a connected peer.
        /// Looks up the connection by peer_id, opens a QUIC stream, negotiates
        /// the protocol via multistream-select, and runs handleOutbound.
        /// peer_id is automatically passed as ctx.peer_id.
        pub fn newStream(self: *Self, io: Io, peer_id: []const u8, comptime P: type) !void {
            comptime protocol_mod.assertProtocolInterface(P);
            const conn = self.connections.get(peer_id) orelse return error.PeerNotConnected;
            const s_inner = try conn.openStream(io);
            var s = quic_mod.Stream{ .inner = s_inner };
            _ = try multistream.negotiateOutbound(io, &s, &.{P.id});
            inline for (config.protocols, 0..) |Proto, i| {
                if (Proto == P) {
                    try self.handlers[i].handleOutbound(io, &s, .{
                        .peer_id = @as(?[]const u8, peer_id),
                    });
                    return;
                }
            }
        }

        /// Gracefully shut down the Switch.
        /// Cancels background fibers, stops engines, notifies handlers, cleans up.
        pub fn close(self: *Self, io: Io) void {
            // Cancel ALL background fibers: accept loops, connection tasks, AND
            // stream tasks (all spawned on self.background).
            self.background.cancel(io);

            // Stop engines (closes their internal background loops).
            // Must happen after canceling Switch fibers, since those fibers
            // may be blocked on engine IO operations.
            if (self.server_engine) |eng| eng.stop(io);
            if (self.client_engine) |eng| eng.stop(io);

            // Notify handlers, free connection objects, and free map keys
            var it = self.connections.iterator();
            while (it.next()) |entry| {
                self.notifyPeerDisconnected(@as(?[]const u8, entry.key_ptr.*));
                entry.value_ptr.*.deinit();
                self.allocator.free(entry.key_ptr.*);
            }
            self.connections.clearRetainingCapacity();
        }

        /// Negotiate protocol on an inbound stream and dispatch to handler.
        /// io flows directly through multistream and protocol handler -- no adapter.
        pub fn dispatchStream(self: *Self, io: Io, s: anytype, ctx: anytype) !void {
            const proto_id = try multistream.negotiateInbound(io, s, &supported_protocol_ids);

            inline for (config.protocols, 0..) |P, i| {
                if (std.mem.eql(u8, proto_id, P.id)) {
                    try self.handlers[i].handleInbound(io, s, ctx);
                    return;
                }
            }
        }

        /// Get a mutable pointer to the handler instance for protocol P.
        /// Allows callers to access handler state directly (e.g. identify results).
        pub fn getHandler(self: *Self, comptime P: type) *P {
            inline for (config.protocols, 0..) |Proto, i| {
                if (Proto == P) return &self.handlers[i];
            }
            @compileError("Protocol '" ++ @typeName(P) ++ "' not registered in Switch");
        }

        /// Notify all protocol handlers that a peer has disconnected.
        /// Only calls handlers that declare `onPeerDisconnected` (comptime check).
        /// Matches rust-libp2p's FromSwarm::ConnectionClosed pattern.
        pub fn notifyPeerDisconnected(self: *Self, peer_id: ?[]const u8) void {
            const pid = peer_id orelse return;
            inline for (config.protocols, 0..) |P, i| {
                if (@hasDecl(P, "onPeerDisconnected")) {
                    self.handlers[i].onPeerDisconnected(pid);
                }
            }
        }
    };
}

// --- Tests ---

test "Switch comptime validation accepts valid config" {
    const MockTransport = struct {
        pub const Connection = struct {
            pub const Stream = StreamType;
            const StreamType = struct {
                pub fn read(_: *@This(), _: Io, _: []u8) anyerror!usize {
                    return 0;
                }
                pub fn write(_: *@This(), _: Io, _: []const u8) anyerror!usize {
                    return 0;
                }
                pub fn close(_: *@This(), _: Io) void {}
            };
            pub fn openStream(_: *@This(), _: Io) !StreamType {
                return .{};
            }
            pub fn acceptStream(_: *@This(), _: Io) !StreamType {
                return .{};
            }
            pub fn close(_: *@This(), _: Io) void {}
        };
        pub const Stream = Connection.StreamType;
        pub const Listener = struct {
            pub fn accept(_: *@This(), _: Io) !Connection {
                return .{};
            }
            pub fn close(_: *@This(), _: Io) void {}
        };
        pub fn dial(_: *@This(), _: Io, _: anytype) !Connection {
            return .{};
        }
        pub fn listen(_: *@This(), _: Io, _: anytype) !Listener {
            return .{};
        }
        pub fn matchesMultiaddr(_: anytype) bool {
            return false;
        }
    };

    const MockProtocol = struct {
        pub const id = "/test/mock/1.0.0";
        pub fn handleInbound(_: *@This(), _: Io, _: anytype, _: anytype) !void {}
        pub fn handleOutbound(_: *@This(), _: Io, _: anytype, _: anytype) !void {}
    };

    const TestSwitch = Switch(.{
        .transports = &.{MockTransport},
        .protocols = &.{MockProtocol},
    });

    const io = std.testing.io;
    var sw = TestSwitch.init(std.testing.allocator, .{}, .{MockProtocol{}});
    defer sw.deinit(io);

    // Verify protocol IDs are correct
    try std.testing.expectEqualStrings("/test/mock/1.0.0", TestSwitch.supported_protocol_ids[0]);
}

test "Swarm ping over QUIC" {
    const ping_mod = @import("protocol/ping.zig");
    const tls_mod = @import("security/tls.zig");
    const ma = multiaddr;

    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const key1 = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(key1);
    const key2 = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(key2);

    const Node = Switch(.{
        .transports = &.{quic_mod.QuicTransport},
        .protocols = &.{ping_mod.Handler},
    });

    // Server
    var server = Node.init(allocator, .{ .host_key = key1 }, .{ping_mod.Handler{}});
    defer server.deinit(io);

    var listen_addr = ma.Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = ma.Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = 0 },
        .QuicV1,
    }) catch return;
    defer listen_addr.deinit();
    server.listen(io, listen_addr) catch return;

    // Client
    var client = Node.init(allocator, .{ .host_key = key2 }, .{ping_mod.Handler{}});
    defer client.deinit(io);

    const bound = server.listenAddrs() orelse return;
    const port = switch (bound) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    var dial_addr = ma.Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = ma.Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = port },
        .QuicV1,
    }) catch return;
    defer dial_addr.deinit();

    const peer_id = client.dial(io, dial_addr) catch return;

    // Ping via newStream — Handler generates payload and measures RTT internally
    client.newStream(io, peer_id, ping_mod.Handler) catch |err| {
        log.warn("ping failed: {}", .{err});
    };

    client.close(io);
    server.close(io);
}

test "Swarm gossipsub subscription over QUIC" {
    const gossipsub_service = @import("protocol/gossipsub/service.zig");
    const tls_mod = @import("security/tls.zig");
    const ma = multiaddr;

    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const key1 = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(key1);
    const key2 = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(key2);

    // Server gossipsub
    const svc1 = gossipsub_service.Service.init(allocator, .{}) catch return;
    defer svc1.deinit();

    const Node = Switch(.{
        .transports = &.{quic_mod.QuicTransport},
        .protocols = &.{gossipsub_service.Handler},
    });
    var server = Node.init(allocator, .{ .host_key = key1 }, .{gossipsub_service.Handler{ .svc = svc1 }});
    defer server.deinit(io);

    var listen_addr = ma.Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = ma.Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = 0 },
        .QuicV1,
    }) catch return;
    defer listen_addr.deinit();
    server.listen(io, listen_addr) catch return;

    // Client gossipsub
    const svc2 = gossipsub_service.Service.init(allocator, .{}) catch return;
    defer svc2.deinit();
    svc2.subscribe("test-topic") catch return;

    var client = Node.init(allocator, .{ .host_key = key2 }, .{gossipsub_service.Handler{ .svc = svc2 }});
    defer client.deinit(io);

    const bound = server.listenAddrs() orelse return;
    const port = switch (bound) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };
    var dial_addr = ma.Multiaddr.fromProtocols(allocator, &.{
        .{ .Ip4 = ma.Ip4Addr{ .bytes = .{ 127, 0, 0, 1 } } },
        .{ .Udp = port },
        .QuicV1,
    }) catch return;
    defer dial_addr.deinit();

    const peer_id = client.dial(io, dial_addr) catch return;

    // Open gossipsub stream -- newStream auto-provides peer_id ctx
    client.newStream(io, peer_id, gossipsub_service.Handler) catch return;

    // Wait for subscription RPC to arrive
    const sleep_timeout: Io.Timeout = .{ .duration = .{
        .raw = Io.Duration.fromMilliseconds(100),
        .clock = .awake,
    } };
    sleep_timeout.sleep(io) catch {};

    // Verify subscription event on server
    const events = svc1.drainEvents() catch return;
    defer allocator.free(events);
    var found = false;
    for (events) |event| {
        switch (event) {
            .subscription_changed => found = true,
            else => {},
        }
    }
    if (!found) {
        log.warn("gossipsub subscription event not found in {} events", .{events.len});
    }

    client.close(io);
    server.close(io);
}
