const std = @import("std");

pub const plain = @import("security/plain.zig");
pub const insecure = @import("security/insecure.zig");
pub const tls = @import("security/tls.zig");

pub const Session = struct {
    local_id: []const u8,
    remote_id: []const u8,
    remote_public_key: []const u8,
};
