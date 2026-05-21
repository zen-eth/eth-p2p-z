const std = @import("std");
const ssl = @import("ssl").c;
const quiche = @import("quiche").c;
const address = @import("../address.zig");
const cid_gen = @import("../connection/cid_gen.zig");
const connection_setup = @import("../connection/setup.zig");

pub const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    ssl_ctx: *ssl.SSL_CTX,
    quiche_config: *quiche.quiche_config,

    pub fn sslCtx(ctx: Context) *ssl.SSL_CTX {
        return ctx.ssl_ctx;
    }

    pub fn quicheConfig(ctx: Context) *quiche.quiche_config {
        return ctx.quiche_config;
    }
};

pub fn createPendingConnection(
    endpoint: Context,
    local_addr: std.Io.net.IpAddress,
    peer_addr: std.Io.net.IpAddress,
    is_server: bool,
    server_scid: ?[*]const u8,
    server_scid_len: usize,
    odcid: ?[*]const u8,
    odcid_len: usize,
    params: connection_setup.ActorParams,
) (error{HandshakeFailed} || std.mem.Allocator.Error)!connection_setup.PendingConnection {
    const conn = try createQuicheConn(endpoint, local_addr, peer_addr, is_server, server_scid, server_scid_len, odcid, odcid_len);
    return connection_setup.createPending(.{
        .allocator = endpoint.allocator,
        .io = endpoint.io,
        .conn = conn,
        .actor = params,
    }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
    };
}

fn createQuicheConn(
    endpoint: Context,
    local_addr: std.Io.net.IpAddress,
    peer_addr: std.Io.net.IpAddress,
    is_server: bool,
    server_scid: ?[*]const u8,
    server_scid_len: usize,
    odcid: ?[*]const u8,
    odcid_len: usize,
) !*quiche.quiche_conn {
    const ssl_conn = ssl.SSL_new(endpoint.sslCtx()) orelse return error.HandshakeFailed;
    errdefer ssl.SSL_free(ssl_conn);

    var generated_scid: cid_gen.LocalCid = undefined;
    if (server_scid == null) generated_scid = cid_gen.randomLocalCid() catch return error.HandshakeFailed;
    const scid_ptr = server_scid orelse generated_scid[0..].ptr;
    const scid_len = if (server_scid != null) server_scid_len else generated_scid.len;

    var local_storage: address.PosixAddress = undefined;
    var peer_storage: address.PosixAddress = undefined;
    const local_len = address.addressToPosix(&local_addr, &local_storage);
    const peer_len = address.addressToPosix(&peer_addr, &peer_storage);

    return quiche.quiche_conn_new_with_tls(
        scid_ptr,
        scid_len,
        odcid,
        odcid_len,
        @ptrCast(&local_storage.any),
        local_len,
        @ptrCast(&peer_storage.any),
        peer_len,
        endpoint.quicheConfig(),
        ssl_conn,
        is_server,
    ) orelse error.HandshakeFailed;
}
