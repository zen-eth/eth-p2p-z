pub const gossipsub = @import("./routers/gossipsub.zig");
const std = @import("std");
const libp2p = @import("../../root.zig");
const rpc = libp2p.protobuf.rpc;
const PeerId = @import("peer_id").PeerId;
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;

pub const RPC = struct {
    rpc_reader: rpc.RPCReader,
    from: PeerId,

    pub fn deinit(self: *RPC) void {
        self.rpc_reader.deinit();
    }
};

pub const PubSubVTable = struct {
    handleRPCFn: *const fn (instance: *anyopaque, rpc_message: *const RPC) anyerror!void,
    addPeerFn: *const fn (self: *anyopaque, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void,
    removePeerFn: *const fn (self: *anyopaque, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void,
};

/// This is a generic PubSub interface that uses the VTable pattern to provide a type-erased
/// interface for handling PubSub messages. It contains a pointer to the underlying
/// PubSub implementation and a pointer to the VTable that defines the interface
/// for that implementation.
pub const PubSub = struct {
    instance: *anyopaque,
    vtable: *const PubSubVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn handleRPC(self: Self, rpc_message: *const RPC) anyerror!void {
        return self.vtable.handleRPCFn(self.instance, rpc_message);
    }

    pub fn addPeer(self: Self, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        self.vtable.addPeerFn(self.instance, peer, callback_ctx, callback);
    }

    pub fn removePeer(self: Self, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        self.vtable.removePeerFn(self.instance, peer, callback_ctx, callback);
    }
};

pub const MessageIdFn = *const fn (allocator: std.mem.Allocator, message: *const rpc.Message) anyerror![]const u8;

/// Computes the default message ID by concatenating `msg.from` and `msg.seqno`.
/// The caller must ensure that `dest` is allocated with at least `msg.from.len + msg.seqno.len` bytes.
pub fn defaultMsgId(allocator: std.mem.Allocator, msg: *const rpc.Message) ![]const u8 {
    if (msg.from == null and msg.seqno == null) {
        return error.BothFromAndSeqNoNull;
    }

    return std.mem.concat(allocator, u8, &.{ msg.from orelse "", msg.seqno orelse "" });
}
