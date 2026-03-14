const std = @import("std");

/// Asserts at comptime that type S satisfies the Security interface.
/// A Security must provide:
///   - pub const protocol_id: []const u8
///   - pub fn upgrade(io: *std.Io, stream: anytype, keypair: KeyPair, role: Role) !SecuredStream
pub fn assertSecurityInterface(comptime S: type) void {
    if (!@hasDecl(S, "protocol_id")) {
        @compileError("Security '" ++ @typeName(S) ++ "' missing 'protocol_id'");
    }
    if (!@hasDecl(S, "upgrade")) {
        @compileError("Security '" ++ @typeName(S) ++ "' missing 'upgrade' method");
    }
}

pub const Role = enum { initiator, responder };

test "assertSecurityInterface exists" {
    _ = &assertSecurityInterface;
}
