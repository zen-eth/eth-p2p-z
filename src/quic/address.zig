const std = @import("std");

pub const PosixAddress = extern union {
    any: std.posix.sockaddr,
    in: std.posix.sockaddr.in,
    in6: std.posix.sockaddr.in6,
};

pub fn addressToPosix(a: *const std.Io.net.IpAddress, storage: *PosixAddress) std.posix.socklen_t {
    return switch (a.*) {
        .ip4 => |ip4| {
            storage.in = .{
                .port = std.mem.nativeToBig(u16, ip4.port),
                .addr = @bitCast(ip4.bytes),
            };
            return @sizeOf(std.posix.sockaddr.in);
        },
        .ip6 => |ip6| {
            storage.in6 = .{
                .port = std.mem.nativeToBig(u16, ip6.port),
                .flowinfo = ip6.flow,
                .addr = ip6.bytes,
                .scope_id = ip6.interface.index,
            };
            return @sizeOf(std.posix.sockaddr.in6);
        },
    };
}

pub const FromSockaddrError = error{AddressInvalid};

/// Decode a POSIX `sockaddr_storage` into a `std.Io.net.IpAddress`.
///
/// The pointer must reference at least the bytes corresponding to the discriminated
/// `family` value. Callers holding a layout-compatible storage from another binding
/// (for example `quiche.struct_sockaddr_storage`) should `@ptrCast` first so the
/// alignment requirement is encoded in the type system rather than relying on
/// `@alignCast` here.
pub fn fromSockaddrStorage(storage: *const std.posix.sockaddr.storage) FromSockaddrError!std.Io.net.IpAddress {
    return switch (storage.family) {
        std.posix.AF.INET => blk: {
            const addr: *const std.posix.sockaddr.in = @ptrCast(storage);
            const bytes: [4]u8 = @bitCast(addr.addr);
            break :blk .{ .ip4 = .{
                .port = std.mem.bigToNative(u16, addr.port),
                .bytes = bytes,
            } };
        },
        std.posix.AF.INET6 => blk: {
            const addr: *const std.posix.sockaddr.in6 = @ptrCast(storage);
            break :blk .{ .ip6 = .{
                .port = std.mem.bigToNative(u16, addr.port),
                .flow = addr.flowinfo,
                .bytes = addr.addr,
                .interface = .{ .index = addr.scope_id },
            } };
        },
        else => error.AddressInvalid,
    };
}

pub fn sameEndpoint(a: std.Io.net.IpAddress, b: std.Io.net.IpAddress) bool {
    return switch (a) {
        .ip4 => |a4| switch (b) {
            .ip4 => |b4| a4.port == b4.port and std.mem.eql(u8, &a4.bytes, &b4.bytes),
            .ip6 => false,
        },
        .ip6 => |a6| switch (b) {
            .ip4 => false,
            .ip6 => |b6| a6.port == b6.port and
                a6.flow == b6.flow and
                a6.interface.index == b6.interface.index and
                std.mem.eql(u8, &a6.bytes, &b6.bytes),
        },
    };
}

