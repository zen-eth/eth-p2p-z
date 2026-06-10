const std = @import("std");
const builtin = @import("builtin");
const address = @import("../address.zig");

const linux = std.os.linux;

pub const control_buffer_align = @alignOf(linux.cmsghdr);

const cmsg_header_len = cmsgAlign(@sizeOf(linux.cmsghdr));
const cmsg_space_udp_gro = cmsgSpace(@sizeOf(u16));
const cmsg_space_ip_pktinfo = cmsgSpace(@sizeOf(std.posix.in_pktinfo));
const cmsg_space_ip6_pktinfo = cmsgSpace(@sizeOf(std.posix.in6_pktinfo));
const cmsg_space_sockaddr = cmsgSpace(@sizeOf(std.posix.sockaddr.storage));
const cmsg_space_timespec = cmsgSpace(@sizeOf(linux.timespec));

pub const recv_control_buffer_len: usize =
    cmsg_space_udp_gro +
    cmsg_space_ip_pktinfo +
    cmsg_space_ip6_pktinfo +
    2 * cmsg_space_sockaddr +
    cmsg_space_timespec;

pub const send_control_buffer_len: usize = @max(cmsg_space_ip_pktinfo, cmsg_space_ip6_pktinfo);

pub const Options = struct {
    enable_udp_gro: bool = true,
    enable_pktinfo: bool = false,
    enable_orig_dst: bool = false,
    enable_rx_timestamps: bool = true,
    socket_mark: ?u32 = null,
    /// Restrict the socket to basic `sendmsg`/`recvmsg` with no ancillary control
    /// data, for the Shadow network simulator. When set, none of the cmsg sockopts
    /// (UDP GRO, `IP_PKTINFO`/`IPV6_RECVPKTINFO`, `IP_RECVORIGDSTADDR`,
    /// `SO_TIMESTAMPNS`) and no socket mark are applied, yielding the same all-false
    /// `Capabilities` that macOS already produces. The toggles above are ignored.
    shadow_compatible: bool = false,
};

pub const Capabilities = struct {
    udp_gro: bool = false,
    // Packet-info socket options are negotiated per address family; dual-stack
    // sockets can support source control for one family and reject the other.
    pktinfo_v4: bool = false,
    pktinfo_v6: bool = false,
    orig_dst_v4: bool = false,
    orig_dst_v6: bool = false,
    rx_timestamps: bool = false,
    socket_mark: bool = false,
};

pub const ConfigureError = std.posix.SetSockOptError;

/// IPV6_V6ONLY's optname (`std.posix.IPV6` is `void` on macOS, so the numeric
/// values live here): 26 on Linux, 27 on the BSDs/macOS.
const ipv6_v6only_optname: u32 = if (builtin.os.tag == .linux) 26 else 27;

/// Whether an AF_INET6 UDP socket accepts IPv4-mapped traffic
/// (IPV6_V6ONLY == 0). Read-only and therefore safe AFTER bind — the option
/// itself can only be changed before bind, which is why the bind path relies
/// on the OS default (dual-stack on both Linux and macOS) and verifies it
/// here instead of setting anything. Returns null when the platform cannot
/// report the option.
pub fn dualStackEnabled(socket: *const std.Io.net.Socket) ?bool {
    var value: c_int = -1;
    var len: std.posix.socklen_t = @sizeOf(c_int);
    const rc = std.c.getsockopt(
        socket.handle,
        std.posix.IPPROTO.IPV6,
        ipv6_v6only_optname,
        @ptrCast(&value),
        &len,
    );
    if (rc != 0) return null;
    return value == 0;
}

pub fn sourceControlEnabled(caps: Capabilities, from: std.Io.net.IpAddress) bool {
    return switch (from) {
        .ip4 => caps.pktinfo_v4,
        .ip6 => caps.pktinfo_v6,
    };
}

pub const ParsedControl = struct {
    pktinfo_to: ?std.Io.net.IpAddress = null,
    orig_dst_to: ?std.Io.net.IpAddress = null,
    gro_segment_size: ?u16 = null,
    rx_system_ns: ?u64 = null,
};

pub const OutgoingMeta = struct {
    caps: Capabilities = .{},
    from: ?std.Io.net.IpAddress = null,
};

pub const ParseIncomingControlError = error{
    PayloadTruncated,
    ControlTruncated,
};

pub fn configureUdpSocket(socket: *const std.Io.net.Socket, options: Options) ConfigureError!Capabilities {
    var caps: Capabilities = .{};
    if (builtin.os.tag != .linux) return caps;
    // Shadow mode: skip every cmsg sockopt and the socket mark so the socket
    // behaves exactly like the macOS path (which sets none of these and still
    // passes QUIC interop). All capabilities stay false, so the send path emits
    // no source-address cmsg and the receive loop takes the plain `recvmsg`.
    if (options.shadow_compatible) return caps;

    const fd = socket.handle;
    const one: c_int = 1;

    if (options.enable_udp_gro) {
        caps.udp_gro = trySetSockOpt(fd, std.posix.IPPROTO.UDP, linux.UDP.GRO, c_int, one);
    }
    if (options.enable_pktinfo) {
        caps.pktinfo_v4 = trySetSockOpt(fd, linux.SOL.IP, linux.IP.PKTINFO, c_int, one);
        caps.pktinfo_v6 = trySetSockOpt(fd, linux.SOL.IPV6, linux.IPV6.RECVPKTINFO, c_int, one);
    }
    if (options.enable_orig_dst) {
        caps.orig_dst_v4 = trySetSockOpt(fd, linux.SOL.IP, linux.IP.RECVORIGDSTADDR, c_int, one);
        caps.orig_dst_v6 = trySetSockOpt(fd, linux.SOL.IPV6, linux.IPV6.RECVORIGDSTADDR, c_int, one);
    }
    if (options.enable_rx_timestamps) {
        caps.rx_timestamps =
            trySetSockOpt(fd, linux.SOL.SOCKET, linux.SO.TIMESTAMPNS_NEW, c_int, one) or
            trySetSockOpt(fd, linux.SOL.SOCKET, linux.SO.TIMESTAMPNS_OLD, c_int, one);
    }

    if (options.socket_mark) |mark| {
        try setSockOpt(fd, linux.SOL.SOCKET, linux.SO.MARK, u32, mark);
        caps.socket_mark = true;
    }
    return caps;
}

pub fn receiveOneTimeout(
    socket: *const std.Io.net.Socket,
    io: std.Io,
    data_buffer: []u8,
    control_buffer: []u8,
    timeout: std.Io.Timeout,
) std.Io.net.Socket.ReceiveTimeoutError!std.Io.net.IncomingMessage {
    var messages = [_]std.Io.net.IncomingMessage{.{
        .from = undefined,
        .data = undefined,
        .control = control_buffer,
        .flags = undefined,
    }};
    const maybe_err, const n = socket.receiveManyTimeout(io, &messages, data_buffer, .{}, timeout);
    if (maybe_err) |err| return err;
    std.debug.assert(n == 1);
    return messages[0];
}

pub fn parseIncomingControl(
    message: *const std.Io.net.IncomingMessage,
    port_source: std.Io.net.IpAddress,
) ParseIncomingControlError!ParsedControl {
    if (message.flags.trunc) return error.PayloadTruncated;
    if (message.flags.ctrunc) return error.ControlTruncated;

    var meta: ParsedControl = .{};
    if (builtin.os.tag != .linux) return meta;

    var it = ControlIterator.init(message.control);
    while (it.next()) |cmsg| {
        if (isUdpControl(cmsg.header, linux.UDP.GRO)) {
            if (readNative(u16, cmsg.data)) |segment_size| {
                if (segment_size != 0) meta.gro_segment_size = segment_size;
            }
            continue;
        }

        if (isIpControl(cmsg.header, linux.IP.PKTINFO)) {
            if (parseIpv4Pktinfo(cmsg.data, port_source)) |to| {
                if (!isUnspecified(to)) meta.pktinfo_to = to;
            }
            continue;
        }

        if (isIpv6Control(cmsg.header, linux.IPV6.PKTINFO)) {
            if (parseIpv6Pktinfo(cmsg.data, port_source)) |to| {
                if (!isUnspecified(to)) meta.pktinfo_to = to;
            }
            continue;
        }

        if (isIpControl(cmsg.header, linux.IP.ORIGDSTADDR) or
            isIpv6Control(cmsg.header, linux.IPV6.ORIGDSTADDR))
        {
            if (parseSockaddr(cmsg.data)) |to| {
                if (!isUnspecified(to)) meta.orig_dst_to = to;
            }
            continue;
        }

        if (isSocketControl(cmsg.header, linux.SO.TIMESTAMPNS_NEW) or
            isSocketControl(cmsg.header, linux.SO.TIMESTAMPNS_OLD))
        {
            if (parseTimespecNs(cmsg.data)) |ns| meta.rx_system_ns = ns;
        }
    }
    return meta;
}

pub fn encodeOutgoingControl(buffer: []u8, meta: OutgoingMeta) []const u8 {
    if (builtin.os.tag != .linux) return &.{};

    var offset: usize = 0;
    if (meta.from) |from| switch (from) {
        .ip4 => |ip4| {
            if (meta.caps.pktinfo_v4) {
                const info = std.posix.in_pktinfo{
                    .ifindex = 0,
                    .spec_dst = @bitCast(ip4.bytes),
                    .addr = 0,
                };
                std.debug.assert(appendCmsg(buffer, &offset, linux.SOL.IP, linux.IP.PKTINFO, std.mem.asBytes(&info)));
            }
        },
        .ip6 => |ip6| {
            if (meta.caps.pktinfo_v6) {
                const ifindex: i32 = if (ip6.interface.index <= @as(u32, @intCast(std.math.maxInt(i32))))
                    @intCast(ip6.interface.index)
                else
                    0;
                const info = std.posix.in6_pktinfo{
                    .addr = ip6.bytes,
                    .ifindex = ifindex,
                };
                std.debug.assert(appendCmsg(buffer, &offset, linux.SOL.IPV6, linux.IPV6.PKTINFO, std.mem.asBytes(&info)));
            }
        },
    };

    return buffer[0..offset];
}

fn setSockOpt(
    fd: std.posix.socket_t,
    level: i32,
    optname: u32,
    comptime T: type,
    value: T,
) ConfigureError!void {
    var raw = value;
    try std.posix.setsockopt(fd, level, optname, std.mem.asBytes(&raw));
}

fn trySetSockOpt(
    fd: std.posix.socket_t,
    level: i32,
    optname: u32,
    comptime T: type,
    value: T,
) bool {
    setSockOpt(fd, level, optname, T, value) catch return false;
    return true;
}

const ControlMessage = struct {
    header: linux.cmsghdr,
    data: []const u8,
};

const ControlIterator = struct {
    control: []const u8,
    offset: usize = 0,

    fn init(control: []const u8) ControlIterator {
        return .{ .control = control };
    }

    fn next(it: *ControlIterator) ?ControlMessage {
        while (it.offset + cmsg_header_len <= it.control.len) {
            const header_offset = it.offset;
            const header: *const linux.cmsghdr = @ptrCast(@alignCast(it.control.ptr + header_offset));
            if (header.len < cmsg_header_len or header_offset + header.len > it.control.len) return null;

            it.offset = header_offset + cmsgAlign(header.len);
            const data_start = header_offset + cmsg_header_len;
            const data_end = header_offset + header.len;
            return .{
                .header = header.*,
                .data = it.control[data_start..data_end],
            };
        }
        return null;
    }
};

fn appendCmsg(buffer: []u8, offset: *usize, level: i32, typ: u32, data: []const u8) bool {
    const len = cmsg_header_len + data.len;
    const next = cmsgAlign(len);
    if (offset.* + next > buffer.len) return false;

    const header: *linux.cmsghdr = @ptrCast(@alignCast(buffer.ptr + offset.*));
    header.* = .{
        .len = len,
        .level = level,
        .type = @intCast(typ),
    };
    @memcpy(buffer[offset.* + cmsg_header_len ..][0..data.len], data);
    if (next > len) @memset(buffer[offset.* + len .. offset.* + next], 0);
    offset.* += next;
    return true;
}

fn cmsgAlign(len: usize) usize {
    const alignment: usize = @sizeOf(usize);
    return (len + alignment - 1) & ~(alignment - 1);
}

fn cmsgSpace(data_len: usize) usize {
    return cmsgAlign(cmsg_header_len + data_len);
}

fn isUdpControl(header: linux.cmsghdr, typ: u32) bool {
    return header.level == std.posix.IPPROTO.UDP and header.type == @as(i32, @intCast(typ));
}

fn isIpControl(header: linux.cmsghdr, typ: u32) bool {
    return header.level == linux.SOL.IP and header.type == @as(i32, @intCast(typ));
}

fn isIpv6Control(header: linux.cmsghdr, typ: u32) bool {
    return header.level == linux.SOL.IPV6 and header.type == @as(i32, @intCast(typ));
}

fn isSocketControl(header: linux.cmsghdr, typ: u32) bool {
    return header.level == linux.SOL.SOCKET and header.type == @as(i32, @intCast(typ));
}

fn readNative(comptime T: type, data: []const u8) ?T {
    if (data.len < @sizeOf(T)) return null;
    return std.mem.bytesToValue(T, data[0..@sizeOf(T)]);
}

fn parseIpv4Pktinfo(data: []const u8, fallback_to: std.Io.net.IpAddress) ?std.Io.net.IpAddress {
    const info = readNative(std.posix.in_pktinfo, data) orelse return null;
    const raw_addr = if (info.spec_dst != 0) info.spec_dst else info.addr;
    return .{ .ip4 = .{
        .bytes = @bitCast(raw_addr),
        .port = ipPort(fallback_to),
    } };
}

fn parseIpv6Pktinfo(data: []const u8, fallback_to: std.Io.net.IpAddress) ?std.Io.net.IpAddress {
    const info = readNative(std.posix.in6_pktinfo, data) orelse return null;
    return .{ .ip6 = .{
        .bytes = info.addr,
        .port = ipPort(fallback_to),
        .flow = 0,
        .interface = .{ .index = if (info.ifindex <= 0) 0 else @intCast(info.ifindex) },
    } };
}

fn ipPort(ip: std.Io.net.IpAddress) u16 {
    return switch (ip) {
        .ip4 => |ip4| ip4.port,
        .ip6 => |ip6| ip6.port,
    };
}

fn isUnspecified(ip: std.Io.net.IpAddress) bool {
    return switch (ip) {
        .ip4 => |ip4| std.mem.allEqual(u8, &ip4.bytes, 0),
        .ip6 => |ip6| std.mem.allEqual(u8, &ip6.bytes, 0),
    };
}

fn parseSockaddr(data: []const u8) ?std.Io.net.IpAddress {
    var storage: std.posix.sockaddr.storage = std.mem.zeroes(std.posix.sockaddr.storage);
    const copy_len = @min(data.len, @sizeOf(std.posix.sockaddr.storage));
    @memcpy(std.mem.asBytes(&storage)[0..copy_len], data[0..copy_len]);
    return address.fromSockaddrStorage(&storage) catch null;
}

fn parseTimespecNs(data: []const u8) ?u64 {
    const ts = readNative(linux.timespec, data) orelse return null;
    if (ts.sec < 0 or ts.nsec < 0) return null;
    const sec: u64 = @intCast(ts.sec);
    const nsec: u64 = @intCast(ts.nsec);
    if (sec > std.math.maxInt(u64) / std.time.ns_per_s) return null;
    return sec * std.time.ns_per_s + nsec;
}

test "shadow_compatible disables all cmsg capabilities" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var addr = std.Io.net.IpAddress{ .ip4 = .loopback(0) };
    const socket = try std.Io.net.IpAddress.bind(&addr, io, .{ .mode = .dgram });
    defer socket.close(io);

    // Even with every optimization toggle on, shadow mode must produce the
    // all-false capabilities the macOS path already yields — no GRO, no pktinfo,
    // no orig-dst, no timestamps, no socket mark.
    const caps = try configureUdpSocket(&socket, .{
        .enable_udp_gro = true,
        .enable_pktinfo = true,
        .enable_orig_dst = true,
        .enable_rx_timestamps = true,
        .socket_mark = 0x42,
        .shadow_compatible = true,
    });
    try std.testing.expectEqual(Capabilities{}, caps);
}

test "parseIncomingControl extracts GRO original destination and timestamp" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const fallback_to: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 9999 } };
    const original_to: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 203, 0, 113, 8 }, .port = 4433 } };
    var original_storage: address.PosixAddress = undefined;
    const original_len = address.addressToPosix(&original_to, &original_storage);
    const gro: u16 = 1200;
    const ts = linux.timespec{ .sec = 12, .nsec = 345 };

    var control: [recv_control_buffer_len]u8 align(control_buffer_align) = undefined;
    var offset: usize = 0;
    try std.testing.expect(appendCmsg(&control, &offset, std.posix.IPPROTO.UDP, linux.UDP.GRO, std.mem.asBytes(&gro)));
    try std.testing.expect(appendCmsg(&control, &offset, linux.SOL.IP, linux.IP.ORIGDSTADDR, std.mem.asBytes(&original_storage.in)[0..original_len]));
    try std.testing.expect(appendCmsg(&control, &offset, linux.SOL.SOCKET, linux.SO.TIMESTAMPNS_NEW, std.mem.asBytes(&ts)));

    const message = std.Io.net.IncomingMessage{
        .from = .{ .ip4 = .{ .bytes = .{ 192, 0, 2, 1 }, .port = 5555 } },
        .data = &.{},
        .control = control[0..offset],
        .flags = .{ .eor = false, .trunc = false, .ctrunc = false, .oob = false, .errqueue = false },
    };
    const meta = try parseIncomingControl(&message, fallback_to);
    try std.testing.expectEqual(@as(?u16, 1200), meta.gro_segment_size);
    const original = meta.orig_dst_to orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(original_to.ip4.port, original.ip4.port);
    try std.testing.expectEqualSlices(u8, &original_to.ip4.bytes, &original.ip4.bytes);
    try std.testing.expectEqual(@as(?u64, 12 * std.time.ns_per_s + 345), meta.rx_system_ns);
}

test "parseIncomingControl parses packet info destination" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const fallback_to: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 9999 } };
    const info = std.posix.in_pktinfo{
        .ifindex = 0,
        .spec_dst = 0,
        .addr = @bitCast([4]u8{ 198, 51, 100, 7 }),
    };

    var control: [recv_control_buffer_len]u8 align(control_buffer_align) = undefined;
    var offset: usize = 0;
    try std.testing.expect(appendCmsg(&control, &offset, linux.SOL.IP, linux.IP.PKTINFO, std.mem.asBytes(&info)));
    const message = std.Io.net.IncomingMessage{
        .from = .{ .ip4 = .{ .bytes = .{ 192, 0, 2, 1 }, .port = 5555 } },
        .data = &.{},
        .control = control[0..offset],
        .flags = .{ .eor = false, .trunc = false, .ctrunc = false, .oob = false, .errqueue = false },
    };

    const meta = try parseIncomingControl(&message, fallback_to);
    const to = meta.pktinfo_to orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u16, 9999), to.ip4.port);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 198, 51, 100, 7 }, &to.ip4.bytes);
}

test "parseIncomingControl preserves fallback port across address families" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const fallback_to: std.Io.net.IpAddress = .{ .ip6 = .{ .bytes = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, .port = 9443 } };
    const info = std.posix.in_pktinfo{
        .ifindex = 0,
        .spec_dst = 0,
        .addr = @bitCast([4]u8{ 127, 0, 0, 1 }),
    };

    var control: [recv_control_buffer_len]u8 align(control_buffer_align) = undefined;
    var offset: usize = 0;
    try std.testing.expect(appendCmsg(&control, &offset, linux.SOL.IP, linux.IP.PKTINFO, std.mem.asBytes(&info)));
    const message = std.Io.net.IncomingMessage{
        .from = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 5555 } },
        .data = &.{},
        .control = control[0..offset],
        .flags = .{ .eor = false, .trunc = false, .ctrunc = false, .oob = false, .errqueue = false },
    };

    const meta = try parseIncomingControl(&message, fallback_to);
    const to = meta.pktinfo_to orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u16, 9443), to.ip4.port);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 127, 0, 0, 1 }, &to.ip4.bytes);
}

test "parseIncomingControl rejects truncated receives" {
    const fallback_to: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 9999 } };
    var message = std.Io.net.IncomingMessage{
        .from = .{ .ip4 = .{ .bytes = .{ 192, 0, 2, 1 }, .port = 5555 } },
        .data = &.{},
        .control = &.{},
        .flags = .{ .eor = false, .trunc = true, .ctrunc = false, .oob = false, .errqueue = false },
    };

    try std.testing.expectError(error.PayloadTruncated, parseIncomingControl(&message, fallback_to));
    message.flags.trunc = false;
    message.flags.ctrunc = true;
    try std.testing.expectError(error.ControlTruncated, parseIncomingControl(&message, fallback_to));
}

test "parseIncomingControl keeps original destination and packet info separate" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const fallback_to: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 9999 } };
    const original_to: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 203, 0, 113, 8 }, .port = 4433 } };
    var original_storage: address.PosixAddress = undefined;
    const original_len = address.addressToPosix(&original_to, &original_storage);
    const info = std.posix.in_pktinfo{
        .ifindex = 0,
        .spec_dst = 0,
        .addr = @bitCast([4]u8{ 198, 51, 100, 7 }),
    };

    var control: [recv_control_buffer_len]u8 align(control_buffer_align) = undefined;
    var offset: usize = 0;
    try std.testing.expect(appendCmsg(&control, &offset, linux.SOL.IP, linux.IP.ORIGDSTADDR, std.mem.asBytes(&original_storage.in)[0..original_len]));
    try std.testing.expect(appendCmsg(&control, &offset, linux.SOL.IP, linux.IP.PKTINFO, std.mem.asBytes(&info)));
    const message = std.Io.net.IncomingMessage{
        .from = .{ .ip4 = .{ .bytes = .{ 192, 0, 2, 1 }, .port = 5555 } },
        .data = &.{},
        .control = control[0..offset],
        .flags = .{ .eor = false, .trunc = false, .ctrunc = false, .oob = false, .errqueue = false },
    };

    const meta = try parseIncomingControl(&message, fallback_to);
    const pktinfo_to = meta.pktinfo_to orelse return error.TestUnexpectedResult;
    const orig_dst_to = meta.orig_dst_to orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, &[_]u8{ 198, 51, 100, 7 }, &pktinfo_to.ip4.bytes);
    try std.testing.expectEqualSlices(u8, &original_to.ip4.bytes, &orig_dst_to.ip4.bytes);
    try std.testing.expectEqual(original_to.ip4.port, orig_dst_to.ip4.port);
}

test "encodeOutgoingControl emits source packet info" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const source: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 42 }, .port = 4242 } };
    var control: [send_control_buffer_len]u8 align(control_buffer_align) = undefined;
    const encoded = encodeOutgoingControl(&control, .{
        .caps = .{ .pktinfo_v4 = true },
        .from = source,
    });

    var saw_pktinfo = false;
    var it = ControlIterator.init(encoded);
    while (it.next()) |cmsg| {
        if (isIpControl(cmsg.header, linux.IP.PKTINFO)) {
            saw_pktinfo = true;
            const info = readNative(std.posix.in_pktinfo, cmsg.data) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualSlices(u8, &source.ip4.bytes, &@as([4]u8, @bitCast(info.spec_dst)));
        }
    }
    try std.testing.expect(saw_pktinfo);
}

test "encodeOutgoingControl respects packet info capability family" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var control: [send_control_buffer_len]u8 align(control_buffer_align) = undefined;
    const v4_source: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 42 }, .port = 4242 } };
    const v6_source: std.Io.net.IpAddress = .{ .ip6 = .{
        .bytes = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
        .port = 4242,
        .flow = 0,
        .interface = .{ .index = 0 },
    } };

    try std.testing.expectEqual(@as(usize, 0), encodeOutgoingControl(&control, .{
        .caps = .{ .pktinfo_v4 = true },
        .from = v6_source,
    }).len);
    try std.testing.expect(sourceControlEnabled(.{ .pktinfo_v4 = true }, v4_source));
    try std.testing.expect(!sourceControlEnabled(.{ .pktinfo_v4 = true }, v6_source));
}
