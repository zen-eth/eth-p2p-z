const std = @import("std");
const peer_id = @import("peer_id");

pub const multiaddr = struct {
    pub const Protocol = enum {
        ip4,
        ip6,
        dns4,
        dns6,
        udp,
        quic,
        quic_v1,
        p2p,
    };

    pub const IpUdp = struct {
        address: std.Io.net.IpAddress,
        peer_id: ?peer_id.PeerId,
    };

    pub const Multiaddr = struct {
        bytes: []const u8,

        pub fn fromString(allocator: std.mem.Allocator, value: []const u8) !Multiaddr {
            return .{ .bytes = try allocator.dupe(u8, value) };
        }

        pub fn deinit(self: *Multiaddr, allocator: std.mem.Allocator) void {
            allocator.free(self.bytes);
            self.* = .{ .bytes = &.{} };
        }

        pub fn toString(self: Multiaddr, allocator: std.mem.Allocator) ![]u8 {
            return try allocator.dupe(u8, self.bytes);
        }

        pub fn asSlice(self: Multiaddr) []const u8 {
            return self.bytes;
        }

        pub fn parseIpUdp(self: Multiaddr, allocator: std.mem.Allocator) !IpUdp {
            return self.parseIpUdpInternal(allocator, null);
        }

        pub fn resolveIpUdp(self: Multiaddr, allocator: std.mem.Allocator, io: std.Io) !IpUdp {
            return self.parseIpUdpInternal(allocator, io);
        }

        fn parseIpUdpInternal(self: Multiaddr, allocator: std.mem.Allocator, io: ?std.Io) !IpUdp {
            var parts = std.mem.splitScalar(u8, self.bytes, '/');
            _ = parts.next();
            const host_kind = parts.next() orelse return error.InvalidMultiaddr;
            const host_value = parts.next() orelse return error.InvalidMultiaddr;
            const udp_kind = parts.next() orelse return error.InvalidMultiaddr;
            const udp_value = parts.next() orelse return error.InvalidMultiaddr;

            if (!std.mem.eql(u8, udp_kind, "udp")) return error.InvalidMultiaddr;
            const port = try std.fmt.parseInt(u16, udp_value, 10);

            const address = if (std.mem.eql(u8, host_kind, "ip4"))
                try std.Io.net.IpAddress.parseIp4(host_value, port)
            else if (std.mem.eql(u8, host_kind, "ip6"))
                try std.Io.net.IpAddress.parseIp6(host_value, port)
            else if (std.mem.eql(u8, host_kind, "dns4"))
                try resolveDns(host_value, port, .ip4, io orelse return error.UnresolvedHost)
            else if (std.mem.eql(u8, host_kind, "dns6"))
                try resolveDns(host_value, port, .ip6, io orelse return error.UnresolvedHost)
            else
                return error.InvalidMultiaddr;

            var saw_quic = false;
            var parsed_peer_id: ?peer_id.PeerId = null;
            while (parts.next()) |proto| {
                if (std.mem.eql(u8, proto, "quic-v1") or std.mem.eql(u8, proto, "quic")) {
                    saw_quic = true;
                    continue;
                }
                if (std.mem.eql(u8, proto, "p2p")) {
                    const peer_text = parts.next() orelse return error.InvalidMultiaddr;
                    parsed_peer_id = try peer_id.PeerId.fromString(allocator, peer_text);
                    continue;
                }
                return error.InvalidMultiaddr;
            }
            if (!saw_quic) return error.InvalidMultiaddr;

            return .{ .address = address, .peer_id = parsed_peer_id };
        }
    };

    fn resolveDns(host: []const u8, port: u16, family: std.Io.net.IpAddress.Family, io: std.Io) !std.Io.net.IpAddress {
        const host_name = try std.Io.net.HostName.init(host);
        var result_storage: [16]std.Io.net.HostName.LookupResult = undefined;
        var results = std.Io.Queue(std.Io.net.HostName.LookupResult).init(&result_storage);

        try std.Io.net.HostName.lookup(host_name, io, &results, .{
            .port = port,
            .family = family,
        });

        while (true) {
            const result = results.getOne(io) catch |err| switch (err) {
                error.Closed => break,
                else => |e| return e,
            };
            switch (result) {
                .address => |address| return address,
                .canonical_name => {},
            }
        }
        return error.NoAddressReturned;
    }
};

test "parse ip4 udp multiaddr" {
    var addr = try multiaddr.Multiaddr.fromString(std.testing.allocator, "/ip4/127.0.0.1/udp/9000/quic-v1");
    defer addr.deinit(std.testing.allocator);

    const parsed = try addr.parseIpUdp(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 9000), parsed.address.getPort());
}

test "parse ip udp quic multiaddr with p2p peer id" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 1, 2, 3 };
    var pk = peer_id.keys.PublicKey{ .type = .ED25519, .data = &data };
    const id = try peer_id.PeerId.fromPublicKey(allocator, &pk);
    const id_text = try id.toString(allocator);
    defer allocator.free(id_text);

    const text = try std.fmt.allocPrint(allocator, "/ip4/127.0.0.1/udp/9000/quic-v1/p2p/{s}", .{id_text});
    defer allocator.free(text);
    var addr = try multiaddr.Multiaddr.fromString(allocator, text);
    defer addr.deinit(allocator);

    const parsed = try addr.parseIpUdp(allocator);
    try std.testing.expectEqual(@as(u16, 9000), parsed.address.getPort());
    try std.testing.expect(parsed.peer_id.?.eql(&id));
}

test "resolve dns4 udp multiaddr" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var addr = try multiaddr.Multiaddr.fromString(allocator, "/dns4/localhost/udp/9000/quic-v1");
    defer addr.deinit(allocator);

    const parsed = try addr.resolveIpUdp(allocator, threaded.io());
    try std.testing.expectEqual(@as(u16, 9000), parsed.address.getPort());
}
