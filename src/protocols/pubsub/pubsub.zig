pub const gossipsub = @import("./routers/gossipsub.zig");
const std = @import("std");
const libp2p = @import("../../root.zig");
const rpc = libp2p.protobuf.rpc;
const PeerId = @import("peer_id").PeerId;
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;

pub fn deinitRPCMessage(message: *rpc.RPC, allocator: std.mem.Allocator) void {
    if (message.subscriptions) |subs| {
        for (subs) |sub| {
            if (sub) |s| {
                if (s.topicid) |topic_id| {
                    allocator.free(topic_id);
                }
            }
        }
        allocator.free(subs);
    }

    if (message.publish) |pubs| {
        for (pubs) |p| {
            if (p) |m| {
                if (m.topic) |topic_id| {
                    allocator.free(topic_id);
                }
                if (m.data) |data| {
                    allocator.free(data);
                }
                if (m.seqno) |seqno| {
                    allocator.free(seqno);
                }
                if (m.signature) |sig| {
                    allocator.free(sig);
                }
                if (m.key) |key| {
                    allocator.free(key);
                }
                if (m.from) |from| {
                    allocator.free(from);
                }
            }
        }
        allocator.free(pubs);
    }

    if (message.control) |control| {
        // internal slice should be freed in `deinitControl` not here
        if (control.graft) |graft| {
            allocator.free(graft);
        }
        if (control.prune) |prune| {
            allocator.free(prune);
        }
    }
}

pub fn deinitControl(control: *?rpc.ControlMessage, allocator: std.mem.Allocator) void {
    if (control.*) |*ctrl| {
        if (ctrl.graft) |graft| {
            for (graft) |item| {
                if (item) |g| {
                    if (g.topic_i_d) |topic_id| {
                        allocator.free(topic_id);
                    }
                }
            }
            allocator.free(graft);
        }

        if (ctrl.prune) |prune| {
            for (prune) |item| {
                if (item) |p| {
                    if (p.topic_i_d) |topic_id| {
                        allocator.free(topic_id);
                    }
                    if (p.peers) |peers| {
                        for (peers) |peer| {
                            if (peer) |p_id| {
                                if (p_id.peer_i_d) |id| {
                                    allocator.free(id);
                                }
                                if (p_id.signed_peer_record) |r| {
                                    allocator.free(r);
                                }
                            }
                        }
                        allocator.free(peers);
                    }
                }
            }
            allocator.free(prune);
        }

        if (ctrl.ihave) |ihave| {
            for (ihave) |item| {
                if (item) |h| {
                    if (h.topic_i_d) |topic_id| {
                        allocator.free(topic_id);
                    }
                    if (h.message_i_ds) |msg_ids| {
                        for (msg_ids) |msg_id| {
                            if (msg_id) |m_id| {
                                allocator.free(m_id);
                            }
                        }
                        allocator.free(msg_ids);
                    }
                }
            }
            allocator.free(ihave);
        }

        if (ctrl.iwant) |iwant| {
            for (iwant) |item| {
                if (item) |w| {
                    if (w.message_i_ds) |msg_ids| {
                        for (msg_ids) |msg_id| {
                            if (msg_id) |m_id| {
                                allocator.free(m_id);
                            }
                        }
                        allocator.free(msg_ids);
                    }
                }
            }
            allocator.free(iwant);
        }

        if (ctrl.idontwant) |idontwant| {
            for (idontwant) |item| {
                if (item) |d| {
                    if (d.message_i_ds) |msg_ids| {
                        for (msg_ids) |msg_id| {
                            if (msg_id) |m_id| {
                                allocator.free(m_id);
                            }
                        }
                        allocator.free(msg_ids);
                    }
                }
            }
            allocator.free(idontwant);
        }
    }
}

pub const PublishPolicy = enum {
    signing,
    // TODO: implement other policies
    //author,
    //random_author,
    anonymous,
};

pub const RPC = struct {
    rpc_reader: rpc.RPCReader,
    from: PeerId,

    pub fn deinit(self: *RPC) void {
        self.rpc_reader.deinit();
    }
};

pub const PubSubVTable = struct {
    handleRPCFn: *const fn (instance: *anyopaque, arena: std.mem.Allocator, rpc_message: *const RPC) anyerror!void,
    addPeerFn: *const fn (self: *anyopaque, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void,
    removePeerFn: *const fn (self: *anyopaque, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void,
    subscribeFn: *const fn (self: *anyopaque, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void,
    unsubscribeFn: *const fn (self: *anyopaque, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void,
    publishFn: *const fn (self: *anyopaque, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void,
    publishOwnedFn: *const fn (self: *anyopaque, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void,
    startHeartbeatFn: *const fn (self: *anyopaque) void,
    stopHeartbeatFn: *const fn (self: *anyopaque) void,
    getAllocatorFn: *const fn (self: *anyopaque) std.mem.Allocator,
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

    pub fn handleRPC(self: Self, arena: std.mem.Allocator, rpc_message: *const RPC) anyerror!void {
        return self.vtable.handleRPCFn(self.instance, arena, rpc_message);
    }

    pub fn addPeer(self: Self, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        self.vtable.addPeerFn(self.instance, peer, callback_ctx, callback);
    }

    pub fn removePeer(self: Self, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        self.vtable.removePeerFn(self.instance, peer, callback_ctx, callback);
    }

    pub fn subscribe(self: Self, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        self.vtable.subscribeFn(self.instance, topic, callback_ctx, callback);
    }

    pub fn startHeartbeat(self: Self) void {
        self.vtable.startHeartbeatFn(self.instance);
    }

    pub fn stopHeartbeat(self: Self) void {
        self.vtable.stopHeartbeatFn(self.instance);
    }
    pub fn unsubscribe(self: Self, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        self.vtable.unsubscribeFn(self.instance, topic, callback_ctx, callback);
    }
    pub fn publish(self: Self, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
        self.vtable.publishFn(self.instance, topic, data, callback_ctx, callback);
    }

    pub fn allocator(self: Self) std.mem.Allocator {
        return self.vtable.getAllocatorFn(self.instance);
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
