const std = @import("std");
const AtomicRc = @import("ref_count").AtomicRc;
const channel = @import("channel.zig");
const connection_waitset = @import("waitset.zig");

pub const max_udp_payload_len = 65_535;
const assumed_packet_budget = 1500;
pub const default_packet_queue_len = 2048;

pub const EnqueueResult = enum {
    queued,
    dropped,
};

pub const RoutedPacket = struct {
    allocator: std.mem.Allocator,
    from: std.Io.net.IpAddress,
    to: std.Io.net.IpAddress,
    rx_mono_ns: ?u64 = null,
    rx_system_ns: ?u64 = null,
    gro_segment_size: ?u16 = null,
    data: []u8,

    pub const Meta = struct {
        rx_mono_ns: ?u64 = null,
        rx_system_ns: ?u64 = null,
        gro_segment_size: ?u16 = null,
    };

    pub fn init(allocator: std.mem.Allocator, data: []const u8, from: std.Io.net.IpAddress, to: std.Io.net.IpAddress) std.mem.Allocator.Error!?RoutedPacket {
        return initWithMeta(allocator, data, from, to, .{});
    }

    pub fn initWithMeta(
        allocator: std.mem.Allocator,
        data: []const u8,
        from: std.Io.net.IpAddress,
        to: std.Io.net.IpAddress,
        meta: Meta,
    ) std.mem.Allocator.Error!?RoutedPacket {
        if (data.len > max_udp_payload_len) return null;
        const owned = try allocator.dupe(u8, data);
        return .{
            .allocator = allocator,
            .from = from,
            .to = to,
            .rx_mono_ns = meta.rx_mono_ns,
            .rx_system_ns = meta.rx_system_ns,
            .gro_segment_size = meta.gro_segment_size,
            .data = owned,
        };
    }

    pub fn deinit(p: *RoutedPacket) void {
        if (p.data.len > 0) p.allocator.free(p.data);
        p.data = &.{};
    }

    pub fn disarm(p: *RoutedPacket) void {
        p.data = &.{};
    }

    pub fn take(p: *RoutedPacket) RoutedPacket {
        const moved = p.*;
        p.disarm();
        return moved;
    }

    pub fn bytes(p: *RoutedPacket) []u8 {
        return p.data;
    }

    pub fn constBytes(p: *const RoutedPacket) []const u8 {
        return p.data;
    }

    fn byteCost(p: *const RoutedPacket) usize {
        return p.data.len;
    }
};

fn dropRoutedPacket(packet: *RoutedPacket, _: std.Io) void {
    packet.deinit();
}

fn routedPacketCost(packet: *const RoutedPacket) usize {
    return packet.byteCost();
}

const PacketChannel = channel.Bounded(RoutedPacket, dropRoutedPacket, routedPacketCost);

pub const IncomingPacketChannel = struct {
    queue: *PacketChannel.State,
    notify_mutex: std.Io.Mutex = .init,
    waitset: ?*connection_waitset.WaitSet = null,
    rc: AtomicRc = .{},

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        capacity_bytes: usize,
        capacity_packets: usize,
    ) std.mem.Allocator.Error!*IncomingPacketChannel {
        const inbox = try allocator.create(IncomingPacketChannel);
        errdefer allocator.destroy(inbox);
        const byte_capacity = @max(@as(usize, assumed_packet_budget), capacity_bytes);
        inbox.* = .{ .queue = try PacketChannel.State.init(allocator, io, capacity_packets, byte_capacity) };
        return inbox;
    }

    pub fn setWaitSet(inbox: *IncomingPacketChannel, io: std.Io, waitset: ?*connection_waitset.WaitSet) void {
        inbox.notify_mutex.lockUncancelable(io);
        inbox.waitset = waitset;
        inbox.notify_mutex.unlock(io);
    }

    pub fn sender(inbox: *IncomingPacketChannel) IncomingPacketSender {
        return .{ .inbox = inbox };
    }

    pub fn receiver(inbox: *IncomingPacketChannel) IncomingPacketReceiver {
        return .{ .inbox = inbox };
    }

    pub fn close(inbox: *IncomingPacketChannel, io: std.Io) void {
        inbox.queue.close(io);
        // Drop queued payloads at close time instead of waiting for the last
        // route-map retain to be released.
        inbox.queue.discardQueued(io);

        inbox.notify_mutex.lockUncancelable(io);
        inbox.waitset = null;
        inbox.notify_mutex.unlock(io);
    }

    pub fn discardQueued(inbox: *IncomingPacketChannel, io: std.Io) void {
        inbox.queue.discardQueued(io);
    }

    pub fn retain(inbox: *IncomingPacketChannel) void {
        inbox.rc.retainChecked();
    }

    pub fn release(inbox: *IncomingPacketChannel) void {
        if (!inbox.rc.releaseChecked()) return;

        const allocator = inbox.queue.allocator;
        inbox.queue.release();
        allocator.destroy(inbox);
    }

    fn notifyPacketsReady(inbox: *IncomingPacketChannel, io: std.Io) void {
        inbox.notify_mutex.lockUncancelable(io);
        defer inbox.notify_mutex.unlock(io);
        if (inbox.waitset) |set| set.notify(io, .{ .inbound_packets = true });
    }
};

pub const IncomingPacketReceiver = struct {
    inbox: *IncomingPacketChannel,

    pub fn tryRecv(rx: IncomingPacketReceiver, io: std.Io, out: *RoutedPacket) bool {
        return rx.inbox.queue.receiver().tryRecv(io, out);
    }
};

pub const IncomingPacketSender = struct {
    inbox: *IncomingPacketChannel,

    pub fn enqueue(s: IncomingPacketSender, io: std.Io, packet: *RoutedPacket) EnqueueResult {
        const moved = packet.take();
        if (!s.inbox.queue.sender().trySend(io, moved)) return .dropped;
        s.inbox.notifyPacketsReady(io);
        return .queued;
    }
};

test "incoming packet byte budget counts payload bytes" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var first_storage: [1000]u8 = undefined;
    var second_storage: [assumed_packet_budget - first_storage.len]u8 = undefined;
    var third_storage: [1]u8 = .{'x'};
    @memset(&first_storage, 'a');
    @memset(&second_storage, 'b');
    const first_data = first_storage[0..];
    const second_data = second_storage[0..];
    const third_data = third_storage[0..];
    const packets = try IncomingPacketChannel.init(allocator, io, assumed_packet_budget, 3);
    defer packets.release();
    defer packets.close(io);

    const tx = packets.sender();
    const rx = packets.receiver();
    const from: std.Io.net.IpAddress = .{ .ip4 = .loopback(9000) };
    const to: std.Io.net.IpAddress = .{ .ip4 = .loopback(9001) };
    var first = (try RoutedPacket.init(allocator, first_data, from, to)) orelse return error.TestUnexpectedResult;
    var second = (try RoutedPacket.init(allocator, second_data, from, to)) orelse return error.TestUnexpectedResult;
    var third = (try RoutedPacket.init(allocator, third_data, from, to)) orelse return error.TestUnexpectedResult;

    try std.testing.expect(tx.enqueue(io, &first) == .queued);
    try std.testing.expect(tx.enqueue(io, &second) == .queued);
    try std.testing.expect(tx.enqueue(io, &third) == .dropped);

    var out: RoutedPacket = undefined;
    try std.testing.expect(rx.tryRecv(io, &out));
    defer out.deinit();
    try std.testing.expectEqualSlices(u8, first_data, out.constBytes());
    try std.testing.expectEqual(@as(u16, 9000), out.from.getPort());
    try std.testing.expectEqual(@as(u16, 9001), out.to.getPort());

    var second_out: RoutedPacket = undefined;
    try std.testing.expect(rx.tryRecv(io, &second_out));
    defer second_out.deinit();
    try std.testing.expectEqualSlices(u8, second_data, second_out.constBytes());
}

test "incoming packet slots are independent from byte budget" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const packets = try IncomingPacketChannel.init(allocator, io, assumed_packet_budget, 4);
    defer packets.release();
    defer packets.close(io);

    const tx = packets.sender();
    const rx = packets.receiver();
    const from: std.Io.net.IpAddress = .{ .ip4 = .loopback(9000) };
    const to: std.Io.net.IpAddress = .{ .ip4 = .loopback(9001) };

    var i: usize = 0;
    while (i < 4) : (i += 1) {
        var packet = (try RoutedPacket.init(allocator, "x", from, to)) orelse return error.TestUnexpectedResult;
        try std.testing.expect(tx.enqueue(io, &packet) == .queued);
    }

    var out: RoutedPacket = undefined;
    i = 0;
    while (i < 4) : (i += 1) {
        try std.testing.expect(rx.tryRecv(io, &out));
        out.deinit();
    }
}

test "closed incoming packet channel drops packets while route retain lives" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const packets = try IncomingPacketChannel.init(allocator, io, assumed_packet_budget, 1);
    const tx = packets.sender();
    packets.retain();

    packets.close(io);
    packets.release();
    defer packets.release();

    const from: std.Io.net.IpAddress = .{ .ip4 = .loopback(9000) };
    const to: std.Io.net.IpAddress = .{ .ip4 = .loopback(9001) };
    var packet = (try RoutedPacket.init(allocator, "late packet", from, to)) orelse return error.TestUnexpectedResult;

    try std.testing.expect(tx.enqueue(io, &packet) == .dropped);
    try std.testing.expectEqual(@as(usize, 0), packet.data.len);
}
