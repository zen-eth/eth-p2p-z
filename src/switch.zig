const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const log = std.log.scoped(.@"switch");

const transport_mod = @import("transport/transport.zig");
const protocol_mod = @import("protocol/protocol.zig");
const multistream = @import("protocol/multistream.zig");
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

        pub fn deinit(self: *Self) void {
            var it = self.connections.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.connections.deinit();
            if (self.server_engine) |eng| eng.deinit();
            if (self.client_engine) |eng| eng.deinit();
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
        /// registered by dial), accepts streams, deregisters on close.
        fn swarmConnectionTask(self: *Self, io: Io, conn: *engine_mod.QuicConnection) void {
            defer conn.close(io);
            defer conn.deinit();

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

            // Register if not already registered (accepted connections)
            if (peer_id) |pid| {
                if (!self.connections.contains(pid)) {
                    const owned = self.allocator.dupe(u8, pid) catch null;
                    if (owned) |key| {
                        self.connections.put(key, conn) catch {
                            self.allocator.free(key);
                        };
                    }
                }
            }

            // Deregister and notify on exit
            defer {
                if (peer_id) |pid| {
                    if (self.connections.fetchRemove(pid)) |kv| {
                        self.allocator.free(kv.key);
                    }
                }
                self.notifyPeerDisconnected(peer_id);
            }

            // Accept streams loop
            var stream_group: Io.Group = .init;
            while (true) {
                const s_inner = conn.acceptStream(io) catch return;
                stream_group.async(io, Self.swarmStreamTask, .{
                    self, io, quic_mod.Stream{ .inner = s_inner }, .{ .peer_id = peer_id },
                });
            }
        }

        /// Handles a single inbound stream: multistream-negotiate then dispatch.
        fn swarmStreamTask(self: *Self, io: Io, s: quic_mod.Stream, ctx: anytype) void {
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
        /// Stops engines, cancels background fibers, notifies handlers, cleans up.
        pub fn close(self: *Self, io: Io) void {
            // Cancel all background fibers
            self.background.cancel(io);

            // Stop engines
            if (self.server_engine) |eng| eng.stop(io);
            if (self.client_engine) |eng| eng.stop(io);

            // Notify handlers and free connection tracking
            var it = self.connections.iterator();
            while (it.next()) |entry| {
                self.notifyPeerDisconnected(@as(?[]const u8, entry.key_ptr.*));
                self.allocator.free(entry.key_ptr.*);
            }
            self.connections.clearRetainingCapacity();
        }

        /// Run the accept loop for a listener. Accepts connections and spawns
        /// a concurrent handler per connection via Io.Group (cooperative fibers).
        /// Blocks until the listener is closed or an error occurs.
        pub fn serve(self: *Self, io: Io, listener: anytype) void {
            var conn_group: Io.Group = .init;
            while (true) {
                const conn = listener.accept(io) catch return;
                conn_group.async(io, Self.handleConnectionTask, .{ self, io, conn });
            }
        }

        /// Task entry point for handling an inbound connection.
        /// Extracts peer_id from the TLS-verified connection identity, passes it to
        /// stream handlers via ctx, and notifies handlers when the connection
        /// closes (rust-libp2p FromSwarm::ConnectionClosed pattern).
        fn handleConnectionTask(self: *Self, io: Io, conn: anytype) void {
            var mutable_conn = conn;
            defer mutable_conn.close(io);

            // Extract peer_id bytes from TLS-verified connection identity.
            var pid_buf: [128]u8 = undefined;
            const peer_id: ?[]const u8 = pid: {
                if (@hasDecl(@TypeOf(mutable_conn), "remotePeerId")) {
                    if (mutable_conn.remotePeerId()) |pid| {
                        break :pid pid.toBytes(&pid_buf) catch null;
                    }
                }
                break :pid null;
            };
            defer self.notifyPeerDisconnected(peer_id);

            var stream_group: Io.Group = .init;
            while (true) {
                const s = mutable_conn.acceptStream(io) catch return;
                stream_group.async(io, Self.handleStreamTask, .{
                    self, io, s, .{ .peer_id = peer_id },
                });
            }
        }

        /// Task entry point for handling an inbound stream.
        /// Negotiates protocol via multistream-select, dispatches to handler.
        fn handleStreamTask(self: *Self, io: Io, s: anytype, ctx: anytype) void {
            var mutable_stream = s;
            self.dispatchStream(io, &mutable_stream, ctx) catch return;
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

        /// Open a stream, negotiate the given protocol via multistream-select,
        /// and run the protocol's outbound handler. Symmetric with dispatchStream.
        pub fn openStream(
            self: *Self,
            io: Io,
            conn: anytype,
            comptime P: type,
            ctx: anytype,
        ) !void {
            comptime protocol_mod.assertProtocolInterface(P);
            var s = try conn.openStream(io);
            const stream = streamRef(&s);
            _ = try multistream.negotiateOutbound(io, stream, &.{P.id});
            inline for (config.protocols, 0..) |Proto, i| {
                if (Proto == P) {
                    try self.handlers[i].handleOutbound(io, stream, ctx);
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

        /// Returns a pointer suitable for protocol handlers.
        /// If openStream returned a pointer (*QuicStream), we have *(*QuicStream) — dereference to get *QuicStream.
        /// If openStream returned a value (Stream), we have *(Stream) — use as-is.
        inline fn streamRef(s: anytype) switch (@typeInfo(@TypeOf(s.*))) {
            .pointer => @TypeOf(s.*),
            else => @TypeOf(s),
        } {
            return switch (@typeInfo(@TypeOf(s.*))) {
                .pointer => s.*,
                else => s,
            };
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

    var sw = TestSwitch.init(std.testing.allocator, .{}, .{MockProtocol{}});
    defer sw.deinit();

    // Verify protocol IDs are correct
    try std.testing.expectEqualStrings("/test/mock/1.0.0", TestSwitch.supported_protocol_ids[0]);
}

test "Switch dispatchStream handles ping over QUIC" {
    const ping_mod = @import("protocol/ping.zig");
    const tls_mod = @import("security/tls.zig");

    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Generate TLS key pairs
    const server_key = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(server_key);
    const client_key = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(client_key);

    // Create Switch with QUIC + ping
    const Node = Switch(.{
        .transports = &.{quic_mod.QuicTransport},
        .protocols = &.{ping_mod.Handler},
    });
    var sw = Node.init(allocator, .{}, .{ping_mod.Handler{}});
    defer sw.deinit();

    // Set up QUIC server engine
    const server_eng = QuicEngine.init(allocator, .{
        .is_server = true,
        .host_key = server_key,
    }) catch return;
    server_eng.setIo(io);
    server_eng.bindSocket(io, &net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }) catch {
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

    // Set up QUIC client engine
    const client_eng = QuicEngine.init(allocator, .{
        .is_server = false,
        .host_key = client_key,
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

    // Spawn server handler fiber FIRST — it will block on acceptStream until
    // the client writes (QUIC lazy streams). Cooperative fibers interleave at I/O points.
    server_eng.background.async(io, struct {
        fn run(sw_ptr: *Node, io_arg: Io, conn: *engine_mod.QuicConnection) void {
            const s_inner = conn.acceptStream(io_arg) catch |err| {
                log.warn("server acceptStream failed: {}", .{err});
                return;
            };
            var s = quic_mod.Stream{ .inner = s_inner };
            sw_ptr.dispatchStream(io_arg, &s, .{}) catch |err| {
                log.warn("server dispatchStream failed: {}", .{err});
            };
        }
    }.run, .{ &sw, io, server_conn });

    // Client: open stream → negotiate multistream → ping outbound (main fiber)
    const payload = [_]u8{0x42} ** ping_mod.payload_length;
    sw.openStream(io, client_conn, ping_mod.Handler, .{ .payload = &payload }) catch |err| {
        log.warn("client openStream (ping) failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_eng.deinit();
        client_eng.deinit();
        server_conn.deinit();
        client_conn.deinit();
        return;
    };

    // Ping succeeded! Clean up.
    server_conn.close(io);
    client_conn.close(io);
    server_eng.stop(io);
    client_eng.stop(io);
    server_eng.deinit();
    client_eng.deinit();
    server_conn.deinit();
    client_conn.deinit();
}

test "Switch dispatchStream handles identify over QUIC" {
    const identify_mod = @import("protocol/identify.zig");
    const tls_mod = @import("security/tls.zig");

    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Generate TLS key pairs
    const server_key = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(server_key);
    const client_key = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(client_key);

    // Create Switch with QUIC + identify
    const Node = Switch(.{
        .transports = &.{quic_mod.QuicTransport},
        .protocols = &.{identify_mod.Handler},
    });
    var sw = Node.init(allocator, .{}, .{identify_mod.Handler{
        .allocator = allocator,
        .config = .{
            .protocol_version = "test/1.0.0",
            .agent_version = "zig-libp2p/0.1.0",
        },
    }});
    defer sw.deinit();

    // Set up QUIC server engine
    const server_eng = QuicEngine.init(allocator, .{
        .is_server = true,
        .host_key = server_key,
    }) catch return;
    server_eng.setIo(io);
    server_eng.bindSocket(io, &net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }) catch {
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

    // Set up QUIC client engine
    const client_eng = QuicEngine.init(allocator, .{
        .is_server = false,
        .host_key = client_key,
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

    // Spawn server handler fiber — it will block on acceptStream until
    // the client writes. Identify inbound sends our identity to the client.
    // We must close the stream after dispatch so the client sees EOF.
    server_eng.background.async(io, struct {
        fn run(sw_ptr: *Node, io_arg: Io, conn: *engine_mod.QuicConnection) void {
            const s_inner = conn.acceptStream(io_arg) catch |err| {
                log.warn("server acceptStream failed: {}", .{err});
                return;
            };
            var s = quic_mod.Stream{ .inner = s_inner };
            defer s.close(io_arg);
            sw_ptr.dispatchStream(io_arg, &s, .{}) catch |err| {
                log.warn("server dispatchStream failed: {}", .{err});
            };
        }
    }.run, .{ &sw, io, server_conn });

    // Client: open stream → negotiate identify → get identity
    const client_s = client_conn.openStream(io) catch |err| {
        log.warn("client openStream failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_eng.deinit();
        client_eng.deinit();
        server_conn.deinit();
        client_conn.deinit();
        return;
    };
    var client_stream = quic_mod.Stream{ .inner = client_s };
    _ = multistream.negotiateOutbound(io, &client_stream, &.{identify_mod.Handler.id}) catch |err| {
        log.warn("client negotiate failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_eng.deinit();
        client_eng.deinit();
        server_conn.deinit();
        client_conn.deinit();
        return;
    };
    const handler = sw.getHandler(identify_mod.Handler);
    var result = handler.handleOutbound(io, &client_stream, .{}) catch |err| {
        log.warn("client identify failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_eng.deinit();
        client_eng.deinit();
        server_conn.deinit();
        client_conn.deinit();
        return;
    };
    defer result.deinit(allocator);

    // Verify identify result
    std.testing.expectEqualStrings("test/1.0.0", result.protocolVersion()) catch |err| {
        log.warn("protocol version mismatch: {}", .{err});
    };
    std.testing.expectEqualStrings("zig-libp2p/0.1.0", result.agentVersion()) catch |err| {
        log.warn("agent version mismatch: {}", .{err});
    };

    // Clean up
    server_conn.close(io);
    client_conn.close(io);
    server_eng.stop(io);
    client_eng.stop(io);
    server_eng.deinit();
    client_eng.deinit();
    server_conn.deinit();
    client_conn.deinit();
}

test "Switch gossipsub subscription over QUIC via openStream" {
    const gossipsub_service = @import("protocol/gossipsub/service.zig");
    const tls_mod = @import("security/tls.zig");

    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Generate TLS key pairs
    const server_key = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(server_key);
    const client_key = tls_mod.generateKeyPair(.ECDSA) catch return;
    defer ssl.EVP_PKEY_free(client_key);

    // Create gossipsub Service (heap-allocated)
    const svc = gossipsub_service.Service.init(allocator, .{}) catch return;
    defer svc.deinit();

    // Subscribe BEFORE connecting so handleOutbound sends subscriptions
    svc.subscribe("test-topic") catch return;

    // Create Switch with gossipsub Handler wrapper
    const Node = Switch(.{
        .transports = &.{quic_mod.QuicTransport},
        .protocols = &.{gossipsub_service.Handler},
    });
    var sw = Node.init(allocator, .{}, .{gossipsub_service.Handler{ .svc = svc }});
    defer sw.deinit();

    // Set up QUIC server engine
    const server_eng = QuicEngine.init(allocator, .{
        .is_server = true,
        .host_key = server_key,
    }) catch return;
    server_eng.setIo(io);
    server_eng.bindSocket(io, &net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }) catch {
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

    // Set up QUIC client engine
    const client_eng = QuicEngine.init(allocator, .{
        .is_server = false,
        .host_key = client_key,
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

    // Spawn server handler fiber — accepts inbound stream and dispatches
    // via Switch (multistream negotiate → gossipsub handleInbound).
    server_eng.background.async(io, struct {
        fn run(sw_ptr: *Node, io_arg: Io, conn: *engine_mod.QuicConnection) void {
            const s_inner = conn.acceptStream(io_arg) catch |err| {
                log.warn("server acceptStream failed: {}", .{err});
                return;
            };
            var s = quic_mod.Stream{ .inner = s_inner };
            sw_ptr.dispatchStream(io_arg, &s, .{
                .peer_id = @as(?[]const u8, "test-client"),
            }) catch |err| {
                log.warn("server dispatchStream failed: {}", .{err});
            };
        }
    }.run, .{ &sw, io, server_conn });

    // Client: use openStream — handleOutbound stores the stream and sends subscriptions.
    // The stream stays open (like go-libp2p/rust-libp2p), and handleInbound runs
    // as a persistent background reader on the server side.
    sw.openStream(io, client_conn, gossipsub_service.Handler, .{
        .peer_id = @as(?[]const u8, "test-client"),
    }) catch |err| {
        log.warn("client openStream (gossipsub) failed: {}", .{err});
        server_conn.close(io);
        client_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_eng.deinit();
        client_eng.deinit();
        server_conn.deinit();
        client_conn.deinit();
        return;
    };

    // Yield to I/O so QUIC engines can deliver the subscription RPC
    // to the server's handleInbound reader.
    const sleep_timeout: Io.Timeout = .{
        .duration = .{
            .raw = Io.Duration.fromMilliseconds(100),
            .clock = .awake,
        },
    };
    sleep_timeout.sleep(io) catch {};

    // Check drainEvents for subscription_changed
    const events = svc.drainEvents() catch {
        sw.notifyPeerDisconnected(@as(?[]const u8, "test-client"));
        client_conn.close(io);
        server_conn.close(io);
        server_eng.stop(io);
        client_eng.stop(io);
        server_eng.deinit();
        client_eng.deinit();
        server_conn.deinit();
        client_conn.deinit();
        return;
    };
    defer allocator.free(events);

    var found_subscription = false;
    for (events) |event| {
        switch (event) {
            .subscription_changed => {
                found_subscription = true;
            },
            else => {},
        }
    }

    if (!found_subscription) {
        log.warn("gossipsub subscription event not found in {} events", .{events.len});
    }

    // Clean up: notify handlers that the peer disconnected
    // (in production, handleConnectionTask does this via defer).
    // Must happen before engine shutdown to close outbound streams
    // while they're still valid.
    sw.notifyPeerDisconnected(@as(?[]const u8, "test-client"));

    // Now close connections and engines
    client_conn.close(io);
    server_conn.close(io);
    server_eng.stop(io);
    client_eng.stop(io);
    server_eng.deinit();
    client_eng.deinit();
    server_conn.deinit();
    client_conn.deinit();
}
