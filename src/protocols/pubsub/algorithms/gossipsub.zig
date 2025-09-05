const std = @import("std");
const libp2p = @import("../../../root.zig");
const ProtocolId = libp2p.protocols.ProtocolId;

pub const mcache = @import("mcache.zig");
pub const v1_id: ProtocolId = "/meshsub/1.0.0";
pub const v1_1_id: ProtocolId = "/meshsub/1.1.0";

pub const protocols: []const ProtocolId = &.{ v1_id, v1_1_id };
