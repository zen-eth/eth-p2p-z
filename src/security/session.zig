const std = @import("std");

pub const Session = struct {
    local_id: []const u8,
    remote_id: []const u8,
    remote_public_key: []const u8,
};
