const std = @import("std");
const quiche = @import("quiche").c;
const ssl = @import("ssl").c;

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const max_odcid_len: usize = quiche.QUICHE_MAX_CONN_ID_LEN;

// Canonical peer-address encoding: a 1-byte family tag followed by the raw
// address bytes (4 for IPv4, 16 for IPv6). See `encodePeerAddr`.
const addr_tag_len: usize = 1;
const ipv4_encoded_len: usize = addr_tag_len + 4;
const ipv6_encoded_len: usize = addr_tag_len + 16;
const max_encoded_addr_len: usize = ipv6_encoded_len;

/// Domain separator mixed into HMAC input so the key cannot collide with any
/// other HMAC use that might be added later.
const domain_tag = "eth-p2p-z/quic-retry-token/v1";

/// Wire format of a minted token (opaque to the peer; we authenticate it on
/// every retried Initial):
///
///   [8 bytes timestamp_ns_be]
///   [1 byte odcid_len]
///   [odcid_len bytes odcid]          (≤ QUICHE_MAX_CONN_ID_LEN)
///   [1 byte retry_scid_len]
///   [retry_scid_len bytes retry_scid] (≤ QUICHE_MAX_CONN_ID_LEN)
///   [32 bytes HMAC-SHA256]
///
/// Client IP is bound by HMAC input but is *not* embedded in the token bytes
/// — we recover it from the source address of the retried packet, so an
/// attacker who tries to replay the token from a different IP fails the MAC
/// check.
pub const max_token_len: usize = 8 + 1 + max_odcid_len + 1 + max_odcid_len + HmacSha256.mac_length;
pub const min_token_len: usize = 8 + 1 + 0 + 1 + 0 + HmacSha256.mac_length;

pub const ValidatedCid = struct {
    bytes: [max_odcid_len]u8,
    len: usize,

    pub fn slice(self: *const ValidatedCid) []const u8 {
        return self.bytes[0..self.len];
    }
};

pub const ValidatedToken = struct {
    original_dcid: ValidatedCid,
    retry_scid: ValidatedCid,
};

pub const Store = struct {
    key: [HmacSha256.key_length]u8,

    pub fn init() error{RandomFailed}!Store {
        var store: Store = undefined;
        if (ssl.RAND_bytes(&store.key, store.key.len) != 1) return error.RandomFailed;
        return store;
    }

    /// Mint a fresh retry token bound to (peer_addr, odcid, retry_scid,
    /// timestamp_ns). The retried Initial must echo `retry_scid` as its DCID.
    /// `out` must be at least `max_token_len` bytes.
    pub fn mint(
        self: *const Store,
        out: []u8,
        peer_addr: std.Io.net.IpAddress,
        odcid: []const u8,
        retry_scid: []const u8,
        timestamp_ns: u64,
    ) usize {
        std.debug.assert(odcid.len <= max_odcid_len);
        std.debug.assert(retry_scid.len <= max_odcid_len);
        std.debug.assert(out.len >= 8 + 1 + odcid.len + 1 + retry_scid.len + HmacSha256.mac_length);

        std.mem.writeInt(u64, out[0..8], timestamp_ns, .big);
        out[8] = @intCast(odcid.len);
        @memcpy(out[9..][0..odcid.len], odcid);

        var offset: usize = 9 + odcid.len;
        out[offset] = @intCast(retry_scid.len);
        offset += 1;
        @memcpy(out[offset..][0..retry_scid.len], retry_scid);
        offset += retry_scid.len;

        const mac_offset = offset;
        computeMac(&self.key, peer_addr, out[0..mac_offset], out[mac_offset..][0..HmacSha256.mac_length]);
        return mac_offset + HmacSha256.mac_length;
    }

    /// Validate a retry token and recover the original DCID plus the
    /// server-issued retry SCID. Returns null when the token fails any check
    /// (length, MAC, or expiry).
    pub fn validate(
        self: *const Store,
        token: []const u8,
        peer_addr: std.Io.net.IpAddress,
        now_ns: u64,
        max_age_ns: u64,
    ) ?ValidatedToken {
        if (token.len < min_token_len or token.len > max_token_len) return null;
        const odcid_len: usize = @intCast(token[8]);
        if (odcid_len > max_odcid_len) return null;

        var offset: usize = 9 + odcid_len;
        if (offset >= token.len) return null;
        const retry_scid_len: usize = @intCast(token[offset]);
        if (retry_scid_len > max_odcid_len) return null;
        offset += 1;
        if (token.len != offset + retry_scid_len + HmacSha256.mac_length) return null;

        const mac_offset = offset + retry_scid_len;
        var expected_mac: [HmacSha256.mac_length]u8 = undefined;
        computeMac(&self.key, peer_addr, token[0..mac_offset], &expected_mac);
        if (!std.crypto.timing_safe.eql([HmacSha256.mac_length]u8, expected_mac, token[mac_offset..][0..HmacSha256.mac_length].*)) {
            return null;
        }

        const issued_ns = std.mem.readInt(u64, token[0..8], .big);
        if (now_ns < issued_ns) return null;
        if (now_ns - issued_ns > max_age_ns) return null;

        var original_dcid: ValidatedCid = .{ .bytes = undefined, .len = odcid_len };
        @memcpy(original_dcid.bytes[0..odcid_len], token[9..][0..odcid_len]);
        var retry_scid: ValidatedCid = .{ .bytes = undefined, .len = retry_scid_len };
        @memcpy(retry_scid.bytes[0..retry_scid_len], token[offset..][0..retry_scid_len]);
        return .{
            .original_dcid = original_dcid,
            .retry_scid = retry_scid,
        };
    }
};

fn computeMac(
    key: *const [HmacSha256.key_length]u8,
    peer_addr: std.Io.net.IpAddress,
    body: []const u8,
    out: *[HmacSha256.mac_length]u8,
) void {
    var hmac = HmacSha256.init(key);
    hmac.update(domain_tag);
    var addr_buf: [max_encoded_addr_len]u8 = undefined;
    const addr_bytes = encodePeerAddr(peer_addr, &addr_buf);
    hmac.update(addr_bytes);
    hmac.update(body);
    hmac.final(out);
}

/// Serialize the peer address into a fixed canonical form (family tag + raw
/// bytes). Port is intentionally excluded — RFC 9000 §8.1.4 binds the token
/// to "the address" and most NATs preserve the IP across rebinds, so binding
/// to IP-only avoids breaking address validation when the source port shifts.
fn encodePeerAddr(addr: std.Io.net.IpAddress, buf: *[max_encoded_addr_len]u8) []const u8 {
    return switch (addr) {
        .ip4 => |ip4| blk: {
            buf[0] = 0;
            @memcpy(buf[1..ipv4_encoded_len], &ip4.bytes);
            break :blk buf[0..ipv4_encoded_len];
        },
        .ip6 => |ip6| blk: {
            buf[0] = 1;
            @memcpy(buf[1..ipv6_encoded_len], &ip6.bytes);
            break :blk buf[0..ipv6_encoded_len];
        },
    };
}

test "mint and validate round-trip" {
    const store = try Store.init();
    const peer: std.Io.net.IpAddress = .{ .ip4 = .{ .port = 1234, .bytes = .{ 192, 168, 1, 1 } } };
    const odcid = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const retry_scid = [_]u8{0xa5} ** 20;
    var token_buf: [max_token_len]u8 = undefined;
    const len = store.mint(&token_buf, peer, &odcid, &retry_scid, 1_000_000_000);
    const result = store.validate(token_buf[0..len], peer, 1_000_000_000 + 1_000_000, 60 * std.time.ns_per_s);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, &odcid, result.?.original_dcid.slice());
    try std.testing.expectEqualSlices(u8, &retry_scid, result.?.retry_scid.slice());
}

test "ip mismatch fails validation" {
    const store = try Store.init();
    const peer: std.Io.net.IpAddress = .{ .ip4 = .{ .port = 1234, .bytes = .{ 192, 168, 1, 1 } } };
    const other_peer: std.Io.net.IpAddress = .{ .ip4 = .{ .port = 1234, .bytes = .{ 192, 168, 1, 2 } } };
    const odcid = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const retry_scid = [_]u8{0xa5} ** 20;
    var token_buf: [max_token_len]u8 = undefined;
    const len = store.mint(&token_buf, peer, &odcid, &retry_scid, 1_000_000_000);
    try std.testing.expect(store.validate(token_buf[0..len], other_peer, 1_000_000_000, 60 * std.time.ns_per_s) == null);
}

test "expired token fails validation" {
    const store = try Store.init();
    const peer: std.Io.net.IpAddress = .{ .ip4 = .{ .port = 1234, .bytes = .{ 192, 168, 1, 1 } } };
    const odcid = [_]u8{ 1, 2, 3, 4 };
    const retry_scid = [_]u8{0xa5} ** 20;
    var token_buf: [max_token_len]u8 = undefined;
    const len = store.mint(&token_buf, peer, &odcid, &retry_scid, 1_000_000_000);
    const too_late = 1_000_000_000 + 60 * std.time.ns_per_s + 1;
    try std.testing.expect(store.validate(token_buf[0..len], peer, too_late, 60 * std.time.ns_per_s) == null);
}

test "tampered mac fails validation" {
    const store = try Store.init();
    const peer: std.Io.net.IpAddress = .{ .ip4 = .{ .port = 1234, .bytes = .{ 192, 168, 1, 1 } } };
    const odcid = [_]u8{ 1, 2, 3, 4 };
    const retry_scid = [_]u8{0xa5} ** 20;
    var token_buf: [max_token_len]u8 = undefined;
    const len = store.mint(&token_buf, peer, &odcid, &retry_scid, 1_000_000_000);
    token_buf[len - 1] ^= 0x01;
    try std.testing.expect(store.validate(token_buf[0..len], peer, 1_000_000_000, 60 * std.time.ns_per_s) == null);
}

test "tampered retry scid fails validation" {
    const store = try Store.init();
    const peer: std.Io.net.IpAddress = .{ .ip4 = .{ .port = 1234, .bytes = .{ 192, 168, 1, 1 } } };
    const odcid = [_]u8{ 1, 2, 3, 4 };
    const retry_scid = [_]u8{0xa5} ** 20;
    var token_buf: [max_token_len]u8 = undefined;
    const len = store.mint(&token_buf, peer, &odcid, &retry_scid, 1_000_000_000);
    token_buf[9 + odcid.len + 1] ^= 0x01;
    try std.testing.expect(store.validate(token_buf[0..len], peer, 1_000_000_000, 60 * std.time.ns_per_s) == null);
}

test "ipv6 round-trip" {
    const store = try Store.init();
    const peer: std.Io.net.IpAddress = .{ .ip6 = .{
        .port = 4321,
        .flow = 0,
        .bytes = .{ 0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
        .interface = .{ .index = 0 },
    } };
    const odcid = [_]u8{0xab} ** 20;
    const retry_scid = [_]u8{0xcd} ** 20;
    var token_buf: [max_token_len]u8 = undefined;
    const len = store.mint(&token_buf, peer, &odcid, &retry_scid, 5_000_000_000);
    const result = store.validate(token_buf[0..len], peer, 5_000_000_000, 60 * std.time.ns_per_s);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, &odcid, result.?.original_dcid.slice());
    try std.testing.expectEqualSlices(u8, &retry_scid, result.?.retry_scid.slice());
}
