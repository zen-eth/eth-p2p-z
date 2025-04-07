const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const testing = std.testing;
const net = std.net;
const posix = std.posix;
const Thread = std.Thread;
const STDReadError = std.net.Stream.ReadError;
const STDWriteError = std.net.Stream.WriteError;

/// GenericConn provides a generic connection interface that implements read, write, and close methods
/// It follows the pattern of GenericReader and GenericWriter in the standard library
pub fn GenericConn(
    comptime Context: type,
    comptime ReadE: type,
    comptime WriteE: type,
    comptime readFn: fn (context: Context, buffer: []u8) ReadE!usize,
    comptime writeFn: fn (context: Context, buffer: []const u8) WriteE!usize,
    comptime closeFn: fn (context: Context) void,
) type {
    return struct {
        context: Context,

        pub const ReadError = ReadE;
        pub const WriteError = WriteE;

        const Self = @This();

        pub inline fn read(self: Self, buffer: []u8) ReadError!usize {
            return readFn(self.context, buffer);
        }

        pub inline fn write(self: Self, buffer: []const u8) WriteError!usize {
            return writeFn(self.context, buffer);
        }

        pub inline fn close(self: Self) void {
            return closeFn(self.context);
        }

        pub inline fn reader(self: Self) std.io.GenericReader(
            Context,
            ReadError,
            readFn,
        ) {
            return .{ .context = self.context };
        }

        pub inline fn writer(self: Self) std.io.GenericWriter(
            Context,
            WriteError,
            writeFn,
        ) {
            return .{ .context = self.context };
        }

        pub inline fn any(self: *const Self) AnyConn {
            return .{
                .context = @ptrCast(&self.context),
                .readFn = typeErasedReadFn,
                .writeFn = typeErasedWriteFn,
                .closeFn = typeErasedCloseFn,
            };
        }

        fn typeErasedReadFn(context: *const anyopaque, buffer: []u8) anyerror!usize {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return readFn(ptr.*, buffer);
        }

        fn typeErasedWriteFn(context: *const anyopaque, buffer: []const u8) anyerror!usize {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return writeFn(ptr.*, buffer);
        }

        fn typeErasedCloseFn(context: *const anyopaque) void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return closeFn(ptr.*);
        }
    };
}

/// AnyConn provides a type-erased interface for any connection that can read, write, and close
pub const AnyConn = struct {
    context: *const anyopaque,
    readFn: *const fn (context: *const anyopaque, buffer: []u8) anyerror!usize,
    writeFn: *const fn (context: *const anyopaque, buffer: []const u8) anyerror!usize,
    closeFn: *const fn (context: *const anyopaque) void,

    const Self = @This();
    pub const Error = anyerror;

    pub fn read(self: Self, buffer: []u8) Error!usize {
        return self.readFn(self.context, buffer);
    }

    pub fn write(self: Self, buffer: []const u8) Error!usize {
        return self.writeFn(self.context, buffer);
    }

    pub fn close(self: Self) Error!void {
        return self.closeFn(self.context);
    }

    pub fn reader(self: Self) std.io.AnyReader {
        return .{
            .context = self.context,
            .readFn = self.readFn,
        };
    }

    pub fn writer(self: Self) std.io.AnyWriter {
        return .{
            .context = self.context,
            .writeFn = self.writeFn,
        };
    }
};

/// Adapter for std.net.Stream
pub fn stdStreamToConn(stream: std.net.Stream) GenericConn(
    std.net.Stream,
    STDReadError,
    STDWriteError,
    stdStreamRead,
    stdStreamWrite,
    stdStreamClose,
) {
    return .{ .context = stream };
}

fn stdStreamRead(stream: std.net.Stream, buffer: []u8) STDReadError!usize {
    return stream.read(buffer);
}

fn stdStreamWrite(stream: std.net.Stream, buffer: []const u8) STDWriteError!usize {
    return stream.write(buffer);
}

fn stdStreamClose(stream: std.net.Stream) void {
    stream.close();
}

/// A connection based on pipe file descriptors, useful for testing connection interfaces
pub const PipeConn = struct {
    reader_fd: posix.fd_t,
    writer_fd: posix.fd_t,
    closed: bool = false,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        if (!self.closed) {
            posix.close(self.reader_fd);
            posix.close(self.writer_fd);
            self.closed = true;
        }
    }

    pub fn read(self: *Self, buffer: []u8) posix.ReadError!usize {
        return posix.read(self.reader_fd, buffer);
    }

    pub fn write(self: *Self, buffer: []const u8) posix.WriteError!usize {
        return posix.write(self.writer_fd, buffer);
    }

    pub fn close(self: *Self) void {
        self.deinit();
    }

    /// Convert to GenericConn interface
    pub fn conn(self: *Self) GenericConn(
        *Self,
        posix.ReadError,
        posix.WriteError,
        read,
        write,
        close,
    ) {
        return .{ .context = self };
    }
};

/// Shared structure for thread communication
const ServerContext = struct {
    socket: posix.socket_t,
    should_exit: std.atomic.Value(bool),
};

/// Echo server that uses an existing bound server socket
fn runEchoServer(context: *ServerContext) !void {
    while (!context.should_exit.load(.acquire)) {
        var client_addr: net.Address = undefined;
        var addr_len: posix.socklen_t = @sizeOf(net.Address);

        const client_socket = posix.accept(
            context.socket,
            &client_addr.any,
            &addr_len,
            0,
        ) catch |err| {
            if (err == error.WouldBlock) {
                std.time.sleep(10 * std.time.ns_per_ms);
                continue;
            }
            if (context.should_exit.load(.acquire)) return;
            return err;
        };

        // Handle client in same thread for simplicity
        handleClient(client_socket);
    }
}

fn handleClient(client_socket: posix.socket_t) void {
    var client_stream = net.Stream{ .handle = client_socket };
    defer client_stream.close(); // This will close the socket safely

    var buffer: [1024]u8 = undefined;
    while (true) {
        const bytes_read = client_stream.read(&buffer) catch break;
        if (bytes_read == 0) break; // Client closed connection

        _ = client_stream.write(buffer[0..bytes_read]) catch break;
    }
}

test "GenericConn with std.net.Stream" {
    const localhost = try net.Address.parseIp("127.0.0.1", 0);

    const server_socket = try posix.socket(localhost.any.family, posix.SOCK.STREAM, posix.IPPROTO.TCP);

    defer posix.close(server_socket);

    try posix.setsockopt(server_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    try posix.bind(server_socket, &localhost.any, localhost.getOsSockLen());
    try posix.listen(server_socket, 128);

    var actual_addr: net.Address = undefined;
    var addr_len: posix.socklen_t = @sizeOf(net.Address);
    try posix.getsockname(server_socket, &actual_addr.any, &addr_len);

    var server_context = ServerContext{
        .socket = server_socket,
        .should_exit = std.atomic.Value(bool).init(false),
    };

    const server_thread = try Thread.spawn(.{}, runEchoServer, .{&server_context});

    defer {
        // Signal thread to exit
        server_context.should_exit.store(true, .release);
        // Allow time for the thread to notice
        std.time.sleep(10 * std.time.ns_per_ms);
        // Join thread
        server_thread.join();
    }

    var client_stream = try net.tcpConnectToAddress(actual_addr);

    {
        var generic_client = stdStreamToConn(client_stream);

        const test_message = "Hello, network stream!";
        const bytes_written = try generic_client.write(test_message);
        try testing.expectEqual(test_message.len, bytes_written);

        var read_buffer: [128]u8 = undefined;
        const bytes_read = try generic_client.read(&read_buffer);

        try testing.expectEqual(test_message.len, bytes_read);
        try testing.expectEqualStrings(test_message, read_buffer[0..bytes_read]);

        var any_client = generic_client.any();
        const any_message = "AnyConn test";
        _ = try any_client.write(any_message);

        const any_read = try any_client.read(&read_buffer);
        try testing.expectEqual(any_message.len, any_read);
        try testing.expectEqualStrings(any_message, read_buffer[0..any_read]);

        var reader = generic_client.reader();
        var writer = generic_client.writer();

        try writer.writeAll("Line test\n");

        var line_buffer: [20]u8 = undefined;
        const line = try reader.readUntilDelimiter(&line_buffer, '\n');
        try testing.expectEqualStrings("Line test", line);
    }

    client_stream.close();
}

/// Creates a pair of connected PipeConn instances
pub fn createPipeConnPair() !struct { client: PipeConn, server: PipeConn } {
    const fds1 = try posix.pipe();
    errdefer {
        posix.close(fds1[0]);
        posix.close(fds1[1]);
    }

    const fds2 = try posix.pipe();
    errdefer {
        posix.close(fds2[0]);
        posix.close(fds2[1]);
    }

    // First pipe: client reads from fds1[0], server writes to fds1[1]
    // Second pipe: server reads from fds2[0], client writes to fds2[1]
    return .{
        .client = .{
            .reader_fd = fds1[0],
            .writer_fd = fds2[1],
        },
        .server = .{
            .reader_fd = fds2[0],
            .writer_fd = fds1[1],
        },
    };
}

// test "PipeConn direct usage" {
//     var pipes = try createPipeConnPair();
//     defer {
//         pipes.client.deinit();
//         pipes.server.deinit();
//     }
//
//     const message = "Hello through pipe!";
//     try testing.expectEqual(message.len, try pipes.client.write(message));
//
//     var buffer: [128]u8 = undefined;
//     const bytes_read = try pipes.server.read(&buffer);
//     try testing.expectEqual(message.len, bytes_read);
//     try testing.expectEqualStrings(message, buffer[0..bytes_read]);
// }
//
// test "PipeConn with GenericConn" {
//     var pipes = try createPipeConnPair();
//     defer {
//         pipes.client.deinit();
//         pipes.server.deinit();
//     }
//
//     var client_conn = pipes.client.conn();
//     var server_conn = pipes.server.conn();
//
//     const message = "Hello GenericConn!";
//     try testing.expectEqual(message.len, try client_conn.write(message));
//
//     var buffer: [128]u8 = undefined;
//     const bytes_read = try server_conn.read(&buffer);
//     try testing.expectEqual(message.len, bytes_read);
//     try testing.expectEqualStrings(message, buffer[0..bytes_read]);
//
//     // Test reader/writer interfaces
//     var client_writer = client_conn.writer();
//     var server_reader = server_conn.reader();
//
//     try client_writer.writeAll("Line test\n");
//
//     var line_buffer: [20]u8 = undefined;
//     const line = try server_reader.readUntilDelimiter(&line_buffer, '\n');
//     try testing.expectEqualStrings("Line test", line);
// }
//
// test "PipeConn with AnyConn" {
//     var pipes = try createPipeConnPair();
//     defer {
//         pipes.client.deinit();
//         pipes.server.deinit();
//     }
//
//     var client_conn = pipes.client.conn();
//     var server_conn = pipes.server.conn();
//
//     var any_client = client_conn.any();
//     var any_server = server_conn.any();
//
//     const message = "AnyConn test";
//     try testing.expectEqual(message.len, try any_client.write(message));
//
//     var buffer: [128]u8 = undefined;
//     const bytes_read = try any_server.read(&buffer);
//     try testing.expectEqual(message.len, bytes_read);
//     try testing.expectEqualStrings(message, buffer[0..bytes_read]);
// }
//
// test "PipeConn bidirectional communication" {
//     var pipes = try createPipeConnPair();
//     defer {
//         pipes.client.deinit();
//         pipes.server.deinit();
//     }
//
//     var client_conn = pipes.client.conn();
//     var server_conn = pipes.server.conn();
//
//     // Client to server
//     _ = try client_conn.write("Hello server");
//
//     var buffer: [128]u8 = undefined;
//     const server_read = try server_conn.read(&buffer);
//     try testing.expectEqualStrings("Hello server", buffer[0..server_read]);
//
//     // Server to client
//     _ = try server_conn.write("Hello client");
//
//     const client_read = try client_conn.read(&buffer);
//     try testing.expectEqualStrings("Hello client", buffer[0..client_read]);
// }
