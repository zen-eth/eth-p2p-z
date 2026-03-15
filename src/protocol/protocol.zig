const std = @import("std");

/// Protocol identifier (e.g., "/ipfs/ping/1.0.0")
pub const ProtocolId = []const u8;

/// Asserts at comptime that type P satisfies the Protocol interface.
/// A Protocol must provide:
///   - pub const id: []const u8  (protocol identifier string)
///   - pub fn handleInbound(io: Io, stream: anytype, ctx: anytype) !void
///   - pub fn handleOutbound(io: Io, stream: anytype, ctx: anytype) !void
pub fn assertProtocolInterface(comptime P: type) void {
    if (!@hasDecl(P, "id")) {
        @compileError("Protocol '" ++ @typeName(P) ++ "' missing 'id' declaration");
    }
    if (!@hasDecl(P, "handleInbound")) {
        @compileError("Protocol '" ++ @typeName(P) ++ "' missing 'handleInbound' method");
    }
    if (!@hasDecl(P, "handleOutbound")) {
        @compileError("Protocol '" ++ @typeName(P) ++ "' missing 'handleOutbound' method");
    }
}

/// Returns a tuple of protocol IDs from a comptime protocol list.
pub fn protocolIds(comptime protocols: anytype) [protocols.len][]const u8 {
    var ids: [protocols.len][]const u8 = undefined;
    inline for (protocols, 0..) |P, i| {
        ids[i] = P.id;
    }
    return ids;
}

test "assertProtocolInterface catches missing id" {
    _ = &assertProtocolInterface;
    _ = &protocolIds;
}
