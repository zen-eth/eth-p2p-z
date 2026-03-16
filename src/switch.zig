const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const log = std.log.scoped(.@"switch");

const transport_mod = @import("transport/transport.zig");
const protocol_mod = @import("protocol/protocol.zig");
const multistream = @import("protocol/multistream.zig");

/// Configuration for comptime Switch composition.
pub const SwitchConfig = struct {
    /// Transport types (must satisfy assertTransportInterface).
    transports: []const type,
    /// Protocol types (must satisfy assertProtocolInterface — Handler structs with id, handleInbound, handleOutbound).
    protocols: []const type,
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

        /// Protocol IDs for multistream-select negotiation (computed at comptime).
        const supported_protocol_ids = protocol_mod.protocolIds(config.protocols);

        pub fn init(allocator: Allocator, handlers: HandlerTuple) Self {
            return .{ .allocator = allocator, .handlers = handlers };
        }

        pub fn deinit(self: *Self) void {
            _ = self;
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
        /// Accepts streams concurrently via Io.Group. Closes connection on exit.
        fn handleConnectionTask(self: *Self, io: Io, conn: anytype) void {
            var mutable_conn = conn;
            defer mutable_conn.close(io);

            var stream_group: Io.Group = .init;
            while (true) {
                const s = mutable_conn.acceptStream(io) catch return;
                stream_group.async(io, Self.handleStreamTask, .{ self, io, s });
            }
        }

        /// Task entry point for handling an inbound stream.
        /// Negotiates protocol via multistream-select, dispatches to handler.
        fn handleStreamTask(self: *Self, io: Io, s: anytype) void {
            var mutable_stream = s;
            self.dispatchStream(io, &mutable_stream) catch return;
        }

        /// Negotiate protocol on an inbound stream and dispatch to handler.
        /// io flows directly through multistream and protocol handler -- no adapter.
        pub fn dispatchStream(self: *Self, io: Io, s: anytype) !void {
            const proto_id = try multistream.negotiateInbound(io, s, &supported_protocol_ids);

            inline for (config.protocols, 0..) |P, i| {
                if (std.mem.eql(u8, proto_id, P.id)) {
                    try self.handlers[i].handleInbound(io, s);
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
        pub fn handleInbound(_: *@This(), _: Io, _: anytype) !void {}
        pub fn handleOutbound(_: *@This(), _: Io, _: anytype, _: anytype) !void {}
    };

    const TestSwitch = Switch(.{
        .transports = &.{MockTransport},
        .protocols = &.{MockProtocol},
    });

    var sw = TestSwitch.init(std.testing.allocator, .{MockProtocol{}});
    defer sw.deinit();

    // Verify protocol IDs are correct
    try std.testing.expectEqualStrings("/test/mock/1.0.0", TestSwitch.supported_protocol_ids[0]);
}

test "Switch dispatchStream handles ping over QUIC" {
    const quic_mod = @import("transport/quic/quic.zig");
    const engine_mod = @import("transport/quic/engine.zig");
    const QuicEngine = engine_mod.QuicEngine;
    const ping_mod = @import("protocol/ping.zig");
    const tls_mod = @import("security/tls.zig");
    const ssl = @import("ssl");
    const net = Io.net;

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
    var sw = Node.init(allocator, .{ping_mod.Handler{}});
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
            sw_ptr.dispatchStream(io_arg, &s) catch |err| {
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
