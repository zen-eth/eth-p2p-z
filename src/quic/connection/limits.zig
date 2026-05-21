const quiche = @import("quiche").c;

pub fn maxDatagramPayload(conn: *const quiche.quiche_conn) usize {
    const len = quiche.quiche_conn_dgram_max_writable_len(conn);
    if (len > 0) return @intCast(len);
    return 0;
}
