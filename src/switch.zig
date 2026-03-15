const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const transport_mod = @import("transport/transport.zig");
const protocol_mod = @import("protocol/protocol.zig");
const multistream = @import("protocol/multistream.zig");

/// Configuration for comptime Switch composition.
pub const SwitchConfig = struct {
    /// Transport types (must satisfy assertTransportInterface).
    transports: []const type,
    /// Protocol types (must have id, handleInbound, handleOutbound).
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

        allocator: Allocator,

        /// Protocol IDs for multistream-select negotiation (computed at comptime).
        const supported_protocol_ids = protocol_mod.protocolIds(config.protocols);

        pub fn init(allocator: Allocator) Self {
            return .{ .allocator = allocator };
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

            inline for (config.protocols) |P| {
                if (std.mem.eql(u8, proto_id, P.id)) {
                    try P.handleInbound(io, s, .{ .allocator = self.allocator });
                    return;
                }
            }
        }

        /// Open a stream on a connection and negotiate the given protocol (outbound).
        /// Returns the transport stream after successful negotiation.
        pub fn negotiateOutbound(
            self: *Self,
            io: Io,
            conn: anytype,
            proto_id: []const u8,
        ) !@TypeOf(conn.*).Stream {
            _ = self;
            var s = try conn.openStream(io);
            _ = try multistream.negotiateOutbound(io, &s, &.{proto_id});
            return s;
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
        pub fn handleInbound(_: Io, _: anytype, _: anytype) !void {}
        pub fn handleOutbound(_: Io, _: anytype, _: anytype) !void {}
    };

    const TestSwitch = Switch(.{
        .transports = &.{MockTransport},
        .protocols = &.{MockProtocol},
    });

    var sw = TestSwitch.init(std.testing.allocator);
    defer sw.deinit();

    // Verify protocol IDs are correct
    try std.testing.expectEqualStrings("/test/mock/1.0.0", TestSwitch.supported_protocol_ids[0]);
}
