const std = @import("std");
const coro = @import("coro");
const aio = @import("aio");

pub const Options = struct {
    backlog: u31,
};

pub const ZigAIOTransport = struct {
    options: Options,

    pub fn listen(self: *ZigAIOTransport, address: std.net.Address, server_socket: *std.posix.socket_t) !void {
        try coro.io.single(aio.Socket{
            .domain = std.posix.AF.INET,
            .flags = std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC,
            .protocol = std.posix.IPPROTO.TCP,
            .out_socket = server_socket,
        });

        try std.posix.setsockopt(server_socket.*, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        if (@hasDecl(std.posix.SO, "REUSEPORT")) {
            try std.posix.setsockopt(server_socket.*, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
        }
        try std.posix.bind(server_socket.*, &address.any, address.getOsSockLen());
        try std.posix.listen(server_socket.*, self.options.backlog);
    }
};

test "ZigAIOTransport" {
    var transport = ZigAIOTransport{
        .options = Options{
            .backlog = 128,
        },
    };
    const address = try std.net.Address.parseIp("127.0.0.1", 3131);
    var server_socket: std.posix.socket_t = undefined;
    try transport.listen(address, &server_socket);
    // std.os.close(server_socket);
}
