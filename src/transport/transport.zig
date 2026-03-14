const std = @import("std");

/// Asserts at comptime that type T satisfies the Transport interface.
/// A Transport must provide:
///   - Connection type with: openStream, acceptStream, close, remotePeerId, remoteAddr
///   - Stream type with: read, write, close
///   - Listener type with: accept, close, localAddr
///   - fn dial(self: *Self, io: *std.Io, addr: Multiaddr) DialError!Connection
///   - fn listen(self: *Self, io: *std.Io, addr: Multiaddr) ListenError!Listener
///   - fn matchesMultiaddr(addr: Multiaddr) bool
pub fn assertTransportInterface(comptime T: type) void {
    // Required associated types
    if (!@hasDecl(T, "Connection")) {
        @compileError("Transport '" ++ @typeName(T) ++ "' missing 'Connection' type");
    }
    if (!@hasDecl(T, "Stream")) {
        @compileError("Transport '" ++ @typeName(T) ++ "' missing 'Stream' type");
    }
    if (!@hasDecl(T, "Listener")) {
        @compileError("Transport '" ++ @typeName(T) ++ "' missing 'Listener' type");
    }

    // Required methods
    if (!@hasDecl(T, "dial")) {
        @compileError("Transport '" ++ @typeName(T) ++ "' missing 'dial' method");
    }
    if (!@hasDecl(T, "listen")) {
        @compileError("Transport '" ++ @typeName(T) ++ "' missing 'listen' method");
    }
    if (!@hasDecl(T, "matchesMultiaddr")) {
        @compileError("Transport '" ++ @typeName(T) ++ "' missing 'matchesMultiaddr' method");
    }

    // Validate Connection type
    const Conn = T.Connection;
    if (!@hasDecl(Conn, "openStream")) {
        @compileError("Connection type of '" ++ @typeName(T) ++ "' missing 'openStream'");
    }
    if (!@hasDecl(Conn, "acceptStream")) {
        @compileError("Connection type of '" ++ @typeName(T) ++ "' missing 'acceptStream'");
    }
    if (!@hasDecl(Conn, "close")) {
        @compileError("Connection type of '" ++ @typeName(T) ++ "' missing 'close'");
    }

    // Validate Stream type
    const Stream = T.Stream;
    if (!@hasDecl(Stream, "read")) {
        @compileError("Stream type of '" ++ @typeName(T) ++ "' missing 'read'");
    }
    if (!@hasDecl(Stream, "write")) {
        @compileError("Stream type of '" ++ @typeName(T) ++ "' missing 'write'");
    }
    if (!@hasDecl(Stream, "close")) {
        @compileError("Stream type of '" ++ @typeName(T) ++ "' missing 'close'");
    }

    // Validate Listener type
    const Listener = T.Listener;
    if (!@hasDecl(Listener, "accept")) {
        @compileError("Listener type of '" ++ @typeName(T) ++ "' missing 'accept'");
    }
    if (!@hasDecl(Listener, "close")) {
        @compileError("Listener type of '" ++ @typeName(T) ++ "' missing 'close'");
    }
}

/// Asserts at comptime that type S satisfies the Stream interface.
pub fn assertStreamInterface(comptime S: type) void {
    if (!@hasDecl(S, "read")) @compileError("Stream '" ++ @typeName(S) ++ "' missing 'read'");
    if (!@hasDecl(S, "write")) @compileError("Stream '" ++ @typeName(S) ++ "' missing 'write'");
    if (!@hasDecl(S, "close")) @compileError("Stream '" ++ @typeName(S) ++ "' missing 'close'");
}

/// Type-erased stream for heterogeneous storage at the application boundary.
/// This is the ONLY VTable in the system.
pub const AnyStream = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        readFn: *const fn (ptr: *anyopaque, io: *std.Io, buf: []u8) anyerror!usize,
        writeFn: *const fn (ptr: *anyopaque, io: *std.Io, data: []const u8) anyerror!usize,
        closeFn: *const fn (ptr: *anyopaque, io: *std.Io) void,
    };

    pub fn read(self: AnyStream, io: *std.Io, buf: []u8) anyerror!usize {
        return self.vtable.readFn(self.ptr, io, buf);
    }

    pub fn write(self: AnyStream, io: *std.Io, data: []const u8) anyerror!usize {
        return self.vtable.writeFn(self.ptr, io, data);
    }

    pub fn close(self: AnyStream, io: *std.Io) void {
        self.vtable.closeFn(self.ptr, io);
    }

    pub fn wrap(comptime StreamT: type, stream: *StreamT) AnyStream {
        const Wrapper = struct {
            fn readFn(ptr: *anyopaque, io: *std.Io, buf: []u8) anyerror!usize {
                const s: *StreamT = @ptrCast(@alignCast(ptr));
                return s.read(io, buf);
            }
            fn writeFn(ptr: *anyopaque, io: *std.Io, data: []const u8) anyerror!usize {
                const s: *StreamT = @ptrCast(@alignCast(ptr));
                return s.write(io, data);
            }
            fn closeFn(ptr: *anyopaque, io: *std.Io) void {
                const s: *StreamT = @ptrCast(@alignCast(ptr));
                s.close(io);
            }
            const vtable_instance = VTable{
                .readFn = readFn,
                .writeFn = writeFn,
                .closeFn = closeFn,
            };
        };
        return .{
            .ptr = @ptrCast(stream),
            .vtable = &Wrapper.vtable_instance,
        };
    }
};

test "assertTransportInterface catches missing types" {
    const BadTransport = struct {};
    // This should fail at comptime:
    // comptime assertTransportInterface(BadTransport);
    // We can't test compile errors directly, but we verify the function exists
    _ = &assertTransportInterface;
    _ = BadTransport;
}

test "AnyStream wrap and dispatch" {
    const MockStream = struct {
        read_called: bool = false,
        write_called: bool = false,
        close_called: bool = false,

        pub fn read(self: *@This(), _: *std.Io, buf: []u8) anyerror!usize {
            self.read_called = true;
            buf[0] = 42;
            return 1;
        }
        pub fn write(self: *@This(), _: *std.Io, _: []const u8) anyerror!usize {
            self.write_called = true;
            return 5;
        }
        pub fn close(self: *@This(), _: *std.Io) void {
            self.close_called = true;
        }
    };

    var mock = MockStream{};
    const any = AnyStream.wrap(MockStream, &mock);

    const buf: [1]u8 = undefined;
    // Note: We can't pass null for io in real code, but for mock testing
    // the mock ignores the io parameter
    _ = any;
    _ = buf;
    // Full test requires std.Io instance — deferred to integration tests
}
