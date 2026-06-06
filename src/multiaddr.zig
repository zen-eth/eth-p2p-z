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

    /// Multiaddr protocol codes (the multiformats "multiaddr" table). A binary
    /// multiaddr is a sequence of [uvarint(code)][value] tuples; these are the
    /// codes for the transport protocols we speak, matching go-multiaddr /
    /// rust-multiaddr / zen-eth multiformats-zig exactly. Confirmed against all
    /// three references.
    const code_ip4: u64 = 4; // value: 4 raw IPv4 bytes
    const code_ip6: u64 = 41; // value: 16 raw IPv6 bytes
    const code_tcp: u64 = 6; // value: 2-byte big-endian port
    const code_udp: u64 = 273; // value: 2-byte big-endian port
    const code_quic: u64 = 460; // value: none (0 bytes)
    const code_quic_v1: u64 = 461; // value: none (0 bytes)

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

        /// Encode the string-form multiaddr (`self.bytes`, e.g.
        /// `/ip4/127.0.0.1/udp/9000/quic-v1`) into the libp2p BINARY multiaddr:
        /// a concatenation of [uvarint(protocol_code)][value] tuples. This is the
        /// on-the-wire form go-libp2p / rust-libp2p emit for signed peer-record
        /// and identify listen addresses; the string remains the canonical
        /// in-memory form (only the wire boundary uses these bytes). Caller owns
        /// the returned slice. Supports the transport protocols ip4/ip6/tcp/udp/
        /// quic/quic-v1; any other component is rejected with `InvalidMultiaddr`.
        pub fn toBytes(self: Multiaddr, allocator: std.mem.Allocator) ![]u8 {
            var out: std.ArrayList(u8) = .empty;
            defer out.deinit(allocator);

            var parts = std.mem.splitScalar(u8, self.bytes, '/');
            // A multiaddr string starts with '/', so the first split component is
            // the empty string before it.
            const leading = parts.next() orelse return error.InvalidMultiaddr;
            if (leading.len != 0) return error.InvalidMultiaddr;

            while (parts.next()) |proto| {
                if (proto.len == 0) return error.InvalidMultiaddr;
                if (std.mem.eql(u8, proto, "ip4")) {
                    const value = parts.next() orelse return error.InvalidMultiaddr;
                    const ip = (try std.Io.net.IpAddress.parseIp4(value, 0)).ip4;
                    try appendCode(allocator, &out, code_ip4);
                    try out.appendSlice(allocator, &ip.bytes);
                } else if (std.mem.eql(u8, proto, "ip6")) {
                    const value = parts.next() orelse return error.InvalidMultiaddr;
                    const ip = (try std.Io.net.IpAddress.parseIp6(value, 0)).ip6;
                    try appendCode(allocator, &out, code_ip6);
                    try out.appendSlice(allocator, &ip.bytes);
                } else if (std.mem.eql(u8, proto, "tcp") or std.mem.eql(u8, proto, "udp")) {
                    const value = parts.next() orelse return error.InvalidMultiaddr;
                    const port = std.fmt.parseInt(u16, value, 10) catch return error.InvalidMultiaddr;
                    try appendCode(allocator, &out, if (proto[0] == 't') code_tcp else code_udp);
                    var port_be: [2]u8 = undefined;
                    std.mem.writeInt(u16, &port_be, port, .big);
                    try out.appendSlice(allocator, &port_be);
                } else if (std.mem.eql(u8, proto, "quic")) {
                    try appendCode(allocator, &out, code_quic);
                } else if (std.mem.eql(u8, proto, "quic-v1")) {
                    try appendCode(allocator, &out, code_quic_v1);
                } else {
                    // Unknown / unsupported component (e.g. /p2p, /dns*). The
                    // transport addresses we emit never carry these; reject rather
                    // than guess an encoding.
                    return error.InvalidMultiaddr;
                }
            }

            return out.toOwnedSlice(allocator);
        }

        /// Decode a libp2p BINARY multiaddr (`[uvarint(code)][value]` tuples) back
        /// into the STRING form and return it as a `Multiaddr` (string stored in
        /// `.bytes`, so the rest of the stack — `Switch.dial`, `parseIpUdp` — keeps
        /// working on strings). Inverse of `toBytes`. Validates each tuple's length;
        /// an unknown code or a truncated value returns `InvalidMultiaddr`. Caller
        /// owns the returned `Multiaddr` (free with `deinit`).
        pub fn fromBytes(allocator: std.mem.Allocator, binary: []const u8) !Multiaddr {
            var str: std.ArrayList(u8) = .empty;
            defer str.deinit(allocator);

            var i: usize = 0;
            while (i < binary.len) {
                const decoded = readUvarint(binary[i..]) catch return error.InvalidMultiaddr;
                i += decoded.len;
                switch (decoded.value) {
                    code_ip4 => {
                        if (i + 4 > binary.len) return error.InvalidMultiaddr;
                        const b = binary[i..][0..4];
                        i += 4;
                        try str.print(allocator, "/ip4/{d}.{d}.{d}.{d}", .{ b[0], b[1], b[2], b[3] });
                    },
                    code_ip6 => {
                        if (i + 16 > binary.len) return error.InvalidMultiaddr;
                        // Reuse the std canonical (RFC-5952) IPv6 formatter; its
                        // `Unresolved` renders the bare address with no brackets or
                        // port, which is the multiaddr string form.
                        const unresolved: std.Io.net.Ip6Address.Unresolved = .{
                            .bytes = binary[i..][0..16].*,
                            .interface_name = null,
                        };
                        i += 16;
                        try str.print(allocator, "/ip6/{f}", .{unresolved});
                    },
                    code_tcp, code_udp => {
                        if (i + 2 > binary.len) return error.InvalidMultiaddr;
                        const port = std.mem.readInt(u16, binary[i..][0..2], .big);
                        i += 2;
                        const name = if (decoded.value == code_tcp) "tcp" else "udp";
                        try str.print(allocator, "/{s}/{d}", .{ name, port });
                    },
                    code_quic => try str.appendSlice(allocator, "/quic"),
                    code_quic_v1 => try str.appendSlice(allocator, "/quic-v1"),
                    else => return error.InvalidMultiaddr,
                }
            }

            return .{ .bytes = try str.toOwnedSlice(allocator) };
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

    /// Append `code` to `out` as an unsigned LEB128 varint (the protocol-code
    /// prefix of a binary multiaddr tuple).
    fn appendCode(allocator: std.mem.Allocator, out: *std.ArrayList(u8), code: u64) !void {
        var v = code;
        while (v >= 0x80) {
            try out.append(allocator, @as(u8, @truncate(v)) | 0x80);
            v >>= 7;
        }
        try out.append(allocator, @truncate(v));
    }

    /// Read one unsigned LEB128 varint from the front of `buf`, returning its
    /// value and the number of bytes consumed. Errors on truncation or a varint
    /// wider than 64 bits.
    fn readUvarint(buf: []const u8) !struct { value: u64, len: usize } {
        var value: u64 = 0;
        var shift: u6 = 0;
        var i: usize = 0;
        while (i < buf.len) : (i += 1) {
            const byte = buf[i];
            value |= @as(u64, byte & 0x7f) << shift;
            if (byte & 0x80 == 0) return .{ .value = value, .len = i + 1 };
            shift = std.math.add(u6, shift, 7) catch return error.VarintTooLong;
        }
        return error.Truncated;
    }

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

test "binary codec round-trips ip4 udp quic-v1" {
    const allocator = std.testing.allocator;
    var addr = try multiaddr.Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9000/quic-v1");
    defer addr.deinit(allocator);

    const binary = try addr.toBytes(allocator);
    defer allocator.free(binary);

    var back = try multiaddr.Multiaddr.fromBytes(allocator, binary);
    defer back.deinit(allocator);
    try std.testing.expectEqualStrings("/ip4/127.0.0.1/udp/9000/quic-v1", back.bytes);
}

test "binary codec round-trips ip6 and tcp forms" {
    const allocator = std.testing.allocator;
    const cases = [_][]const u8{
        "/ip6/::1/udp/4001/quic-v1",
        "/ip4/192.168.0.10/tcp/4242",
        "/ip6/2001:db8::1/tcp/80",
        "/ip4/10.0.0.5/udp/4002/quic",
    };
    for (cases) |want| {
        var addr = try multiaddr.Multiaddr.fromString(allocator, want);
        defer addr.deinit(allocator);
        const binary = try addr.toBytes(allocator);
        defer allocator.free(binary);
        var back = try multiaddr.Multiaddr.fromBytes(allocator, binary);
        defer back.deinit(allocator);
        try std.testing.expectEqualStrings(want, back.bytes);
    }
}

test "toBytes matches the go/rust binary multiaddr byte-for-byte" {
    const allocator = std.testing.allocator;
    var addr = try multiaddr.Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9000/quic-v1");
    defer addr.deinit(allocator);

    const binary = try addr.toBytes(allocator);
    defer allocator.free(binary);

    // Worked out from the multiformats multiaddr spec (and confirmed against
    // go-multiaddr v0.14.0 + rust multiaddr 0.18.2):
    //   uvarint(4)=04 || 127.0.0.1 = 7f 00 00 01   (ip4)
    //   uvarint(273)=91 02 || 9000 big-endian = 23 28   (udp)
    //   uvarint(461)=cd 03   (quic-v1, no value)
    const expected = [_]u8{ 0x04, 0x7f, 0x00, 0x00, 0x01, 0x91, 0x02, 0x23, 0x28, 0xcd, 0x03 };
    try std.testing.expectEqualSlices(u8, &expected, binary);

    // The reverse direction: that exact byte sequence decodes to the string.
    var back = try multiaddr.Multiaddr.fromBytes(allocator, &expected);
    defer back.deinit(allocator);
    try std.testing.expectEqualStrings("/ip4/127.0.0.1/udp/9000/quic-v1", back.bytes);
}

test "fromBytes rejects truncation and unknown codes" {
    const allocator = std.testing.allocator;
    // ip4 code with only 3 of 4 address bytes.
    try std.testing.expectError(error.InvalidMultiaddr, multiaddr.Multiaddr.fromBytes(allocator, &[_]u8{ 0x04, 0x7f, 0x00, 0x00 }));
    // An unsupported protocol code (dns4 = 54) we do not decode.
    try std.testing.expectError(error.InvalidMultiaddr, multiaddr.Multiaddr.fromBytes(allocator, &[_]u8{0x36}));
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
