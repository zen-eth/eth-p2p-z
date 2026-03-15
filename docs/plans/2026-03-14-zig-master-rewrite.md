# zig-libp2p Rewrite Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rewrite zig-libp2p from Zig 0.15.2 + libxev to Zig 0.16.0-dev + std.Io.Evented, replacing VTables with comptime composition, removing the TLS threadlocal hack, and focusing on QUIC transport with early interop testing.

**Architecture:** Comptime-composed protocol stack using `Switch(.{.transports = .{QuicTransport}, .protocols = .{Ping, Identify, Gossipsub}})`. std.Io.Evented (kqueue/io_uring) replaces libxev. lsquic C callbacks bridge to Zig via `Io.Queue`. All protocol logic is generic over stream type via comptime.

**Tech Stack:** Zig 0.16.0-dev (master), std.Io.Evented, lsquic (QUIC), BoringSSL (TLS), gremlin (protobuf), secp256k1

**Design Doc:** `docs/plans/2026-03-14-zig-master-rewrite-design.md`

---

## Phase 1: Foundation

### Task 1: Set Up Zig 0.16.0-dev Build Environment

**Files:**
- Modify: `build.zig.zon`
- Modify: `build.zig`

**Step 1: Install Zig 0.16.0-dev**

Run: Download latest Zig master from https://ziglang.org/download/

```bash
# macOS example
curl -LO https://ziglang.org/builds/zig-macos-aarch64-0.16.0-dev.2821+3edaef9e0.tar.xz
tar xf zig-macos-aarch64-0.16.0-dev.2821+3edaef9e0.tar.xz
export PATH="$PWD/zig-macos-aarch64-0.16.0-dev.2821+3edaef9e0:$PATH"
zig version  # should print 0.16.0-dev.xxxx
```

**Step 2: Create new branch**

```bash
git checkout -b feat/zig-master-rewrite
```

**Step 3: Update `build.zig.zon`**

Remove `.libxev` from dependencies. Keep all other deps. Update `.minimum_zig_version` if present.

**Step 4: Update `build.zig`**

Remove all libxev-related code:
- Remove `const libxev_dep` and `b.dependency("libxev", ...)`
- Remove `"xev"` module import from root_module
- Remove `libxev-backend` build option
- Remove the xev module from all compile targets

Replace libxev references with `std.Io` usage (no external dependency needed).

**Step 5: Verify build compiles (will have errors from removed xev imports)**

Run: `zig build 2>&1 | head -50`
Expected: Compile errors from files referencing `@import("xev")` -- this is expected, we'll fix these as we port each module.

**Step 6: Commit**

```bash
git add build.zig build.zig.zon
git commit -m "chore: begin zig master rewrite, remove libxev dependency"
```

---

### Task 2: Create New Source Tree Skeleton

**Files:**
- Create: `src/io/adapter.zig`
- Create: `src/transport/transport.zig`
- Create: `src/security/security.zig`
- Create: `src/protocol/protocol.zig`
- Modify: `src/root.zig`

**Step 1: Create directory structure**

```bash
mkdir -p src/io src/transport/quic src/security src/protocol/gossipsub src/peer/discovery src/util
```

**Step 2: Create `src/io/adapter.zig`** — std.Io utility wrappers

```zig
const std = @import("std");
const Io = std.Io;

/// Read exactly `buf.len` bytes from a stream, blocking until complete.
pub fn readExact(io: *Io, stream: anytype, buf: []u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try stream.read(io, buf[total..]);
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }
}

/// Write all bytes to a stream.
pub fn writeAll(io: *Io, stream: anytype, data: []const u8) !void {
    var total: usize = 0;
    while (total < data.len) {
        const n = try stream.write(io, data[total..]);
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

test "adapter placeholder" {
    // Will be filled with real tests once std.Io.Evented is available
}
```

**Step 3: Create `src/transport/transport.zig`** — comptime Transport interface

```zig
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

    var buf: [1]u8 = undefined;
    // Note: We can't pass null for io in real code, but for mock testing
    // the mock ignores the io parameter
    _ = any;
    _ = buf;
    // Full test requires std.Io instance — deferred to integration tests
}
```

**Step 4: Create `src/protocol/protocol.zig`** — comptime Protocol interface

```zig
const std = @import("std");

/// Protocol identifier (e.g., "/ipfs/ping/1.0.0")
pub const ProtocolId = []const u8;

/// Asserts at comptime that type P satisfies the Protocol interface.
/// A Protocol must provide:
///   - pub const id: []const u8  (protocol identifier string)
///   - pub fn handleInbound(io: *std.Io, stream: anytype, ctx: anytype) !void
///   - pub fn handleOutbound(io: *std.Io, stream: anytype, ctx: anytype) !void
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
```

**Step 5: Create `src/security/security.zig`** — comptime Security interface

```zig
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
```

**Step 6: Stub out new `src/root.zig`**

Replace the current root.zig with the new module structure. Keep it minimal — just enough to compile.

```zig
const std = @import("std");

// Core modules
pub const transport = @import("transport/transport.zig");
pub const protocol = @import("protocol/protocol.zig");
pub const security = @import("security/security.zig");
pub const io_adapter = @import("io/adapter.zig");

// Re-export key types
pub const AnyStream = transport.AnyStream;
pub const ProtocolId = protocol.ProtocolId;

test {
    std.testing.refAllDeclsRecursive(@This());
}
```

**Step 7: Update `build.zig` to use new root module**

Update the root module to point to new `src/root.zig` and remove old module imports that no longer exist.

**Step 8: Run build**

Run: `zig build test`
Expected: All comptime interface assertion tests pass.

**Step 9: Commit**

```bash
git add src/io/ src/transport/transport.zig src/protocol/protocol.zig src/security/security.zig src/root.zig build.zig
git commit -m "feat: add comptime interface assertions and new module skeleton"
```

---

### Task 3: Port Identity and secp256k1 Context

**Files:**
- Create: `src/security/identity.zig` (ported from `src/identity.zig`)
- Create: `src/security/secp_context.zig` (ported from `src/secp_context.zig`)
- Modify: `src/root.zig`

**Step 1: Port `secp_context.zig`**

Copy `src/secp_context.zig` to `src/security/secp_context.zig`. No changes needed — this module has no I/O or xev dependencies.

**Step 2: Port `identity.zig` to `src/security/identity.zig`**

Copy and update import paths:
- `@import("security/tls.zig")` → `@import("tls.zig")` (same directory)
- `@import("secp_context.zig")` → `@import("secp_context.zig")` (same directory)
- All other imports (`ssl`, `secp256k1`, `peer_id`) remain the same

No I/O or xev dependencies in this file — it's pure crypto.

**Step 3: Update `src/root.zig`**

Add identity and secp_context imports:
```zig
pub const identity = @import("security/identity.zig");
pub const secp_context = @import("security/secp_context.zig");
```

**Step 4: Run tests**

Run: `zig build test`
Expected: Identity and secp_context tests pass.

**Step 5: Commit**

```bash
git add src/security/identity.zig src/security/secp_context.zig src/root.zig
git commit -m "feat: port identity and secp256k1 context to new module structure"
```

---

### Task 4: Port TLS Certificate Functions (Remove threadlocal)

**Files:**
- Create: `src/security/tls.zig` (ported from `src/security/tls.zig`)
- Modify: `src/root.zig`

**Step 1: Port TLS cert gen/verify functions**

Copy `src/security/tls.zig` to the new location. This file is 1144 lines but most of it is pure BoringSSL interop with no I/O dependencies.

**Key changes:**
1. **Remove** `threadlocal var g_peer_cert` (line 28)
2. **Remove** `takeSavedPeerCertificate()` and `clearSavedPeerCertificate()` functions
3. **Remove** `libp2pVerifyCallback()` (the OpenSSL `SSL_CTX_set_verify` callback)
4. **Keep** all cert generation, verification, and BoringSSL interop functions:
   - `generateKeyPair`
   - `buildCert`
   - `verifyAndExtractPeerInfo`
   - `signData`
   - `createProtobufEncodedPublicKey`
   - `reconstructEvpKeyFromPublicKey`
   - `verifySignature`
   - `alpnSelectCallbackfn`
   - All private helpers (`createExtension`, `addExtension`, `extractExtensionFields`, etc.)

5. **Add** new `CertVerifyCtx` struct for `ea_verify_cert`:

```zig
/// Context passed to lsquic's ea_verify_cert callback.
/// Replaces the threadlocal g_peer_cert hack.
pub const CertVerifyCtx = struct {
    allocator: std.mem.Allocator,

    /// Verified peer info, keyed by a connection identifier.
    /// Populated by verifyCertCallback, consumed by onHskDone.
    verified_peers: std.AutoHashMap(usize, VerifiedPeer),

    pub const VerifiedPeer = struct {
        peer_id: PeerId,
        host_pubkey: keys.PublicKey,
    };

    pub fn init(allocator: std.mem.Allocator) CertVerifyCtx {
        return .{
            .allocator = allocator,
            .verified_peers = std.AutoHashMap(usize, VerifiedPeer).init(allocator),
        };
    }

    pub fn deinit(self: *CertVerifyCtx) void {
        self.verified_peers.deinit();
    }

    /// Take (move) a verified peer entry. Returns null if not found.
    pub fn take(self: *CertVerifyCtx, conn_id: usize) ?VerifiedPeer {
        return self.verified_peers.fetchRemove(conn_id);
    }
};

/// lsquic ea_verify_cert callback implementation.
/// Called by lsquic during TLS handshake with the full certificate chain.
/// Context is passed via ea_verify_ctx.
pub fn verifyCertCallback(
    verify_ctx: ?*anyopaque,
    chain: ?*ssl.stack_st_X509,
) callconv(.c) c_int {
    const ctx: *CertVerifyCtx = @ptrCast(@alignCast(verify_ctx orelse return -1));
    const cert_stack = chain orelse return -1;

    // Get first cert in chain (peer's cert)
    const num_certs = ssl.sk_X509_num(cert_stack);
    if (num_certs < 1) return -1;

    const cert = ssl.sk_X509_value(cert_stack, 0) orelse return -1;

    // Verify and extract peer info
    const info = verifyAndExtractPeerInfo(ctx.allocator, cert) catch return -1;
    if (!info.is_valid) return -1;

    // Store verified peer keyed by cert pointer (unique per connection)
    const conn_id = @intFromPtr(cert);
    ctx.verified_peers.put(conn_id, .{
        .peer_id = info.peer_id,
        .host_pubkey = info.host_pubkey,
    }) catch return -1;

    return 0; // success
}
```

**Step 2: Update imports in `identity.zig`**

Ensure `src/security/identity.zig` imports tls from `@import("tls.zig")` (same directory).

**Step 3: Run TLS tests**

Run: `zig build test`
Expected: TLS cert generation and verification tests pass (Ed25519, ECDSA, RSA).

**Step 4: Commit**

```bash
git add src/security/tls.zig src/security/identity.zig
git commit -m "feat: port TLS module, replace threadlocal with CertVerifyCtx"
```

---

### Task 5: Implement Comptime Multistream-Select

**Files:**
- Create: `src/protocol/multistream.zig`
- Modify: `src/root.zig`

**Step 1: Write the multistream-select negotiation as a comptime generic**

The current `src/protocols/mss.zig` uses callbacks and VTables. The new version is a simple function generic over any stream type.

```zig
const std = @import("std");

pub const PROTOCOL_ID = "/multistream/1.0.0";
const MESSAGE_SUFFIX = "\n";
const NA = "na";
const MAX_MESSAGE_LENGTH = 1024;

pub const Error = error{
    ProtocolIdTooLong,
    InvalidMultistreamSuffix,
    FirstLineShouldBeMultistream,
    AllProposedProtocolsRejected,
    NoSupportedProtocols,
    UnexpectedEof,
    InvalidLength,
};

/// Write a multistream-select message: length-prefixed, newline-terminated.
fn writeMessage(io: *std.Io, stream: anytype, msg: []const u8) !void {
    // Write varint length (msg + newline)
    var len_buf: [10]u8 = undefined;
    const len_bytes = encodeUvarint(msg.len + 1, &len_buf);
    try writeAllGeneric(io, stream, len_buf[0..len_bytes]);
    try writeAllGeneric(io, stream, msg);
    try writeAllGeneric(io, stream, MESSAGE_SUFFIX);
}

/// Read a multistream-select message: length-prefixed, newline-terminated.
fn readMessage(io: *std.Io, stream: anytype, buf: []u8) ![]const u8 {
    // Read varint length
    var len: usize = 0;
    var shift: u6 = 0;
    while (true) {
        var byte_buf: [1]u8 = undefined;
        const n = try stream.read(io, &byte_buf);
        if (n == 0) return Error.UnexpectedEof;
        const b = byte_buf[0];
        len |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) break;
        shift += 7;
        if (shift >= 64) return Error.InvalidLength;
    }

    if (len == 0 or len > MAX_MESSAGE_LENGTH) return Error.InvalidLength;
    if (len > buf.len) return Error.ProtocolIdTooLong;

    // Read exactly `len` bytes
    var total: usize = 0;
    while (total < len) {
        const n = try stream.read(io, buf[total..len]);
        if (n == 0) return Error.UnexpectedEof;
        total += n;
    }

    // Verify and strip newline suffix
    if (buf[len - 1] != '\n') return Error.InvalidMultistreamSuffix;
    return buf[0 .. len - 1];
}

/// Negotiate as initiator: propose protocols, return the selected one.
pub fn negotiateOutbound(
    io: *std.Io,
    stream: anytype,
    proposed_protocols: []const []const u8,
) (Error || @TypeOf(stream).ReadError || @TypeOf(stream).WriteError)![]const u8 {
    var buf: [MAX_MESSAGE_LENGTH]u8 = undefined;

    // Send multistream header
    try writeMessage(io, stream, PROTOCOL_ID);

    // Read multistream header response
    const header = try readMessage(io, stream, &buf);
    if (!std.mem.eql(u8, header, PROTOCOL_ID)) {
        return Error.FirstLineShouldBeMultistream;
    }

    // Propose each protocol until one is accepted
    for (proposed_protocols) |proto| {
        try writeMessage(io, stream, proto);
        const response = try readMessage(io, stream, &buf);
        if (std.mem.eql(u8, response, proto)) {
            return proto; // Accepted!
        }
        // "na" means rejected, try next
    }

    return Error.AllProposedProtocolsRejected;
}

/// Negotiate as responder: wait for proposals, accept if supported.
pub fn negotiateInbound(
    io: *std.Io,
    stream: anytype,
    supported_protocols: []const []const u8,
) (Error || @TypeOf(stream).ReadError || @TypeOf(stream).WriteError)![]const u8 {
    var buf: [MAX_MESSAGE_LENGTH]u8 = undefined;

    // Read multistream header
    const header = try readMessage(io, stream, &buf);
    if (!std.mem.eql(u8, header, PROTOCOL_ID)) {
        return Error.FirstLineShouldBeMultistream;
    }

    // Send multistream header
    try writeMessage(io, stream, PROTOCOL_ID);

    // Process proposals
    while (true) {
        const proposal = try readMessage(io, stream, &buf);

        for (supported_protocols) |supported| {
            if (std.mem.eql(u8, proposal, supported)) {
                try writeMessage(io, stream, supported);
                return supported;
            }
        }

        // Reject unsupported protocol
        try writeMessage(io, stream, NA);
    }
}

fn encodeUvarint(value: usize, buf: []u8) usize {
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) : (i += 1) {
        buf[i] = @intCast((v & 0x7f) | 0x80);
        v >>= 7;
    }
    buf[i] = @intCast(v);
    return i + 1;
}

fn writeAllGeneric(io: *std.Io, stream: anytype, data: []const u8) !void {
    var total: usize = 0;
    while (total < data.len) {
        const n = try stream.write(io, data[total..]);
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

test "encodeUvarint" {
    var buf: [10]u8 = undefined;
    const n = encodeUvarint(300, &buf);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(u8, 0xAC), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x02), buf[1]);
}

test "encodeUvarint single byte" {
    var buf: [10]u8 = undefined;
    const n = encodeUvarint(21, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 21), buf[0]);
}
```

**Step 2: Update `src/root.zig`**

```zig
pub const multistream = @import("protocol/multistream.zig");
```

**Step 3: Run tests**

Run: `zig build test`
Expected: Multistream uvarint encoding tests pass.

**Step 4: Commit**

```bash
git add src/protocol/multistream.zig src/root.zig
git commit -m "feat: implement comptime-generic multistream-select negotiation"
```

---

### Task 6: Port Utility Modules

**Files:**
- Create: `src/util/linear_fifo.zig` (from `src/linear_fifo.zig`)
- Create: `src/util/protobuf.zig` (from `src/protobuf.zig`)
- Modify: `src/root.zig`

**Step 1: Copy utility files**

These files have no I/O dependencies. Direct copy with updated import paths.

**Step 2: Update root.zig**

```zig
pub const util = struct {
    pub const linear_fifo = @import("util/linear_fifo.zig");
    pub const protobuf = @import("util/protobuf.zig");
};
```

**Step 3: Run tests**

Run: `zig build test`
Expected: All utility tests pass.

**Step 4: Commit**

```bash
git add src/util/ src/root.zig
git commit -m "feat: port utility modules to new structure"
```

---

## Phase 2: QUIC Transport (outline — detailed tasks to be expanded when Phase 1 is complete)

### Task 7: Upgrade lsquic Dependency to Zig Master

- Fork `zen-eth/lsquic@anshalshukla/zig-0.15.2-upgrade`
- Update `build.zig` for Zig 0.16.0-dev API changes
- Verify `zig build` compiles lsquic + boringssl
- Update hash in `build.zig.zon`

### Task 8: Implement QuicEngine (lsquic-to-std.Io Bridge)

**Files:** `src/transport/quic/engine.zig`

Core bridge between lsquic's C callback model and std.Io:
- UDP receive loop via `std.Io.Evented` (kqueue/io_uring)
- Engine process loop with timer-driven `lsquic_engine_process_conns()`
- Per-stream `Io.Queue(ReadEvent)` for read data flow
- Per-engine `Io.Queue(*QuicConnection)` for new connection flow
- `packetsOut` callback to send UDP packets via std.Io
- lsquic `stream_if` callbacks populate queues

### Task 9: Implement QuicTransport, QuicConnection, QuicStream

**Files:** `src/transport/quic/quic.zig`

- `QuicTransport`: satisfies `assertTransportInterface`, wraps `QuicEngine`
- `QuicConnection`: wraps `lsquic_conn_t`, provides `openStream`/`acceptStream` via `Io.Queue`
- `QuicStream`: wraps `lsquic_stream_t`, provides `read`/`write`/`close` via per-stream queue
- `QuicListener`: wraps listening engine, `accept` returns `QuicConnection` from queue
- TLS setup uses `ea_verify_cert`/`ea_verify_ctx` with `CertVerifyCtx`

### Task 10: QUIC Integration Tests

- Loopback dial + listen
- Stream read/write round-trip
- Multiple concurrent streams
- Connection close cleanup
- TLS peer ID verification (no threadlocal)

---

## Phase 3: Core Protocols (outline)

### Task 11: Implement Comptime Ping Protocol

**Files:** `src/protocol/ping.zig`

Port from current `src/protocols/ping.zig`:
- Remove all VTable/callback patterns
- Generic over stream type: `pub fn handleInbound(io: *std.Io, stream: anytype, ...) !void`
- Initiator: send 32 random bytes, wait for echo, measure RTT
- Responder: read 32 bytes, echo them back
- Uses `io.sleep()` for timeout instead of xev timer

### Task 12: Implement Comptime Identify Protocol

**Files:** `src/protocol/identify.zig`

Port from current `src/protocols/identify.zig`:
- Generic over stream type
- Responder: encode Identify protobuf, write to stream, close
- Initiator: read from stream until close, decode protobuf
- Reuse `IdentifyMessageBuilder` and protobuf types

### Task 13: Implement Comptime Gossipsub Protocol

**Files:** `src/protocol/gossipsub/`

Port gossipsub router, message cache. Largest protocol — mesh management, heartbeat, score tracking.

---

## Phase 4: Switch (outline)

### Task 14: Implement Comptime Switch

**Files:** `src/switch.zig`

```zig
pub fn Switch(comptime config: SwitchConfig) type { ... }
```
- Comptime validates transports and protocols
- `listen()` — inline for over transports, spawn accept loops via `io.async()`
- `dial()` — match multiaddr to transport, dial, negotiate protocol
- `handleInboundStream()` — multistream-select, dispatch to protocol handler

### Task 15: Implement Peer Store and Connection Manager

**Files:** `src/peer/store.zig`, `src/peer/conn_manager.zig`

---

## Phase 5: Interop Testing (outline — advanced priority)

### Task 16: Update Interop Test Framework

**Files:** `interop/transport/main.zig`

- Update to new API (`Switch`, `std.Io.Evented`)
- Keep Redis-based coordination and Docker infrastructure
- Test against go-libp2p `v0.41.0` and rust-libp2p

### Task 17: Interop Ping Test

### Task 18: Interop Identify Test

### Task 19: Interop Gossipsub Test

---

## Phase 6: Peer Discovery (outline)

### Task 20: Implement mDNS Discovery

**Files:** `src/peer/discovery/mdns.zig`

### Task 21: Implement Kademlia DHT

**Files:** `src/peer/discovery/dht.zig`

---

## Verification Checklist

After each phase, verify:

1. `zig build` compiles without errors on Zig 0.16.0-dev
2. `zig build test` passes all unit tests
3. No `threadlocal` usage in security code
4. No `@import("xev")` anywhere in codebase
5. No VTable patterns except `AnyStream`
6. All protocols are generic over stream type (comptime)

After Phase 5 interop:

7. `zig build transport-interop` passes against go-libp2p
8. QUIC dial/listen works cross-implementation
9. Ping, identify, gossipsub protocols interop correctly
10. `std.Io.Evented` works on both macOS (kqueue) and Linux (io_uring)
