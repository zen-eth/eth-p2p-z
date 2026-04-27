/// TLS over TCP for libp2p — ConnHandler-based approach using BoringSSL memory BIOs.
///
/// Pattern mirrors insecure.zig but the handler stays in the pipeline permanently
/// to encrypt/decrypt all post-handshake traffic.
///
/// Wire role determination:
///   conn.direction() == .OUTBOUND  →  SSL_set_connect_state (TLS client)
///   conn.direction() == .INBOUND   →  SSL_set_accept_state  (TLS server)
///
/// Handshake completion calls the ConnUpgrader callback with a heap-allocated
/// *SecuritySession whose remote_id carries the raw multihash bytes of the
/// remote peer's libp2p identity key.

const std = @import("std");
const ssl = @import("ssl");
const Allocator = std.mem.Allocator;
const p2p_conn = @import("../conn.zig");
const libp2p = @import("../root.zig");
const security = libp2p.security;
const SecuritySession = security.Session;
const keys = @import("peer_id").keys;
const tls = security.tls;
const proto_binding = @import("../multistream/protocol_binding.zig");
const ProtocolDescriptor = @import("../multistream/protocol_descriptor.zig").ProtocolDescriptor;
const ProtocolMatcher = @import("../multistream/protocol_matcher.zig").ProtocolMatcher;
const ProtocolId = libp2p.protocols.ProtocolId;

// ---------------------------------------------------------------------------
// Write-context helpers
// ---------------------------------------------------------------------------

/// Fire-and-forget write context used when draining the write BIO during the
/// handshake — no user callback to call, just free the buffer when done.
const DrainWriteCtx = struct {
    allocator: Allocator,
    buf: []u8,

    fn callback(instance: ?*anyopaque, _: anyerror!usize) void {
        const self: *DrainWriteCtx = @ptrCast(@alignCast(instance.?));
        self.allocator.free(self.buf);
        self.allocator.destroy(self);
    }
};

/// Write context used for application-data writes after the handshake.
/// Reports back to the caller with the *plaintext* byte count on success.
const TlsWriteCtx = struct {
    allocator: Allocator,
    encrypted_buf: []u8,
    plaintext_len: usize,
    user_ctx: ?*anyopaque,
    user_cb: *const fn (?*anyopaque, anyerror!usize) void,

    fn callback(instance: ?*anyopaque, res: anyerror!usize) void {
        const self: *TlsWriteCtx = @ptrCast(@alignCast(instance.?));
        defer {
            self.allocator.free(self.encrypted_buf);
            self.allocator.destroy(self);
        }
        if (res) |_| {
            self.user_cb(self.user_ctx, self.plaintext_len);
        } else |err| {
            self.user_cb(self.user_ctx, err);
        }
    }
};

// ---------------------------------------------------------------------------
// TlsTcpHandler — the per-connection ConnHandler
// ---------------------------------------------------------------------------

pub const TlsTcpHandler = struct {
    allocator: Allocator,
    /// SSL context — freed in onInactive.
    ssl_ctx: *ssl.SSL_CTX,
    /// SSL object — freed in onInactive (also frees read_bio/write_bio).
    ssl_handle: *ssl.SSL,
    /// Memory BIO we feed incoming ciphertext into (owned by ssl_handle).
    read_bio: *ssl.BIO,
    /// Memory BIO we drain encrypted output from (owned by ssl_handle).
    write_bio: *ssl.BIO,
    /// True once SSL_do_handshake has returned 1.
    handshake_done: bool = false,
    /// Passed through to the ConnUpgrader callback on handshake completion.
    user_data: ?*anyopaque,
    callback: *const fn (?*anyopaque, anyerror!?*anyopaque) void,

    const Self = @This();

    // --- Helpers -----------------------------------------------------------

    /// Drain all pending bytes from the write BIO and send them down-pipeline.
    /// Each chunk is heap-allocated so the async write can outlive this stack frame.
    fn drainWriteBio(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        while (ssl.BIO_pending(self.write_bio) > 0) {
            var tmp: [16384]u8 = undefined;
            const n = ssl.BIO_read(self.write_bio, &tmp, @intCast(tmp.len));
            if (n <= 0) break;
            const buf = self.allocator.dupe(u8, tmp[0..@intCast(n)]) catch break;
            const wc = self.allocator.create(DrainWriteCtx) catch {
                self.allocator.free(buf);
                break;
            };
            wc.* = .{ .allocator = self.allocator, .buf = buf };
            ctx.write(buf, wc, DrainWriteCtx.callback);
        }
    }

    /// Read all available decrypted application data and fire it up the pipeline.
    fn drainDecrypted(self: *Self, ctx: *p2p_conn.ConnHandlerContext) !void {
        var tmp: [16384]u8 = undefined;
        while (true) {
            const n = ssl.SSL_read(self.ssl_handle, &tmp, @intCast(tmp.len));
            if (n <= 0) {
                const err = ssl.SSL_get_error(self.ssl_handle, n);
                // WANT_READ = no more data right now; ZERO_RETURN = clean shutdown
                if (err == ssl.SSL_ERROR_WANT_READ or err == ssl.SSL_ERROR_ZERO_RETURN) break;
                // Other SSL errors after handshake — log and stop reading
                std.log.warn("tls_tcp: SSL_read error {}", .{err});
                break;
            }
            try ctx.fireRead(tmp[0..@intCast(n)]);
        }
    }

    /// Called exactly once when SSL_do_handshake returns 1.
    /// Extracts the remote peer identity from the certificate and calls back.
    fn completeHandshake(self: *Self, ctx: *p2p_conn.ConnHandlerContext) !void {
        self.handshake_done = true;

        // Flush any final handshake bytes (e.g. Finished message)
        self.drainWriteBio(ctx);

        // Get the peer's certificate directly from the SSL object (not thread-local)
        const peer_cert = ssl.SSL_get_peer_certificate(self.ssl_handle);
        if (peer_cert == null) {
            self.callback(self.user_data, error.NoPeerCertificate);
            return;
        }
        defer ssl.X509_free(peer_cert);

        // Clean up the thread-local side-effect from libp2pVerifyCallback
        tls.clearSavedPeerCertificate();

        // Verify the libp2p extension and extract the remote peer ID
        const peer_info = tls.verifyAndExtractPeerInfo(self.allocator, peer_cert.?) catch |err| {
            self.callback(self.user_data, err);
            return;
        };
        // host_pubkey.data is heap-allocated by verifyAndExtractPeerInfo
        defer if (peer_info.host_pubkey.data) |d| self.allocator.free(d);

        if (!peer_info.is_valid) {
            self.callback(self.user_data, error.InvalidPeerCertificate);
            return;
        }

        // Serialize the PeerId to raw multihash bytes for SecuritySession.remote_id.
        // These bytes are allocated with the pipeline allocator; they outlive the
        // connection (acceptable — same as the mock which uses string literals).
        const pipeline_alloc = ctx.pipeline.allocator;
        const peer_id_len = peer_info.peer_id.toBytesLen();
        const peer_id_bytes = pipeline_alloc.alloc(u8, peer_id_len) catch {
            self.callback(self.user_data, error.OutOfMemory);
            return;
        };
        _ = peer_info.peer_id.toBytes(peer_id_bytes) catch {
            pipeline_alloc.free(peer_id_bytes);
            self.callback(self.user_data, error.PeerIdEncodeFailed);
            return;
        };

        // Allocate the SecuritySession — ConnUpgrader will copy and destroy it
        const session = pipeline_alloc.create(SecuritySession) catch {
            pipeline_alloc.free(peer_id_bytes);
            self.callback(self.user_data, error.OutOfMemory);
            return;
        };
        session.* = .{
            .local_id = "",
            .remote_id = peer_id_bytes,
            .remote_public_key = "",
        };
        self.callback(self.user_data, @as(?*anyopaque, session));
    }

    // --- ConnHandler interface ----------------------------------------------

    pub fn onActive(self: *Self, ctx: *p2p_conn.ConnHandlerContext) !void {
        // Propagate up the pipeline first so higher layers are ready to receive
        try ctx.fireActive();

        // Kick off the TLS handshake
        const ret = ssl.SSL_do_handshake(self.ssl_handle);
        if (ret == 1) {
            // Handshake completed in one shot (unusual for TCP but handle it)
            try self.completeHandshake(ctx);
        } else {
            const err = ssl.SSL_get_error(self.ssl_handle, ret);
            if (err == ssl.SSL_ERROR_WANT_READ) {
                // Normal: waiting for the peer's next flight; drain our output (e.g. ClientHello)
                self.drainWriteBio(ctx);
            } else {
                std.log.warn("tls_tcp: SSL_do_handshake initial error {}", .{err});
                return error.TlsHandshakeFailed;
            }
        }
    }

    pub fn onInactive(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        ctx.fireInactive();
        // SSL_free also frees read_bio and write_bio (SSL owns them after SSL_set_bio)
        ssl.SSL_free(self.ssl_handle);
        ssl.SSL_CTX_free(self.ssl_ctx);
        self.allocator.destroy(self);
    }

    pub fn onRead(self: *Self, ctx: *p2p_conn.ConnHandlerContext, data: []const u8) !void {
        // Feed incoming bytes into the read BIO
        const written = ssl.BIO_write(self.read_bio, data.ptr, @intCast(data.len));
        if (written <= 0) return error.TlsBioWriteFailed;

        if (!self.handshake_done) {
            // Drive the handshake forward
            const ret = ssl.SSL_do_handshake(self.ssl_handle);
            if (ret == 1) {
                try self.completeHandshake(ctx);
                // Any application data that arrived in the same TCP segment as the
                // final handshake message is now readable via SSL_read
                try self.drainDecrypted(ctx);
            } else {
                const err = ssl.SSL_get_error(self.ssl_handle, ret);
                if (err == ssl.SSL_ERROR_WANT_READ) {
                    // Send our next handshake flight and wait for more peer data
                    self.drainWriteBio(ctx);
                } else {
                    std.log.warn("tls_tcp: SSL_do_handshake error {} during onRead", .{err});
                    return error.TlsHandshakeFailed;
                }
            }
        } else {
            // Handshake already done — just decrypt and forward
            try self.drainDecrypted(ctx);
        }
    }

    pub fn onReadComplete(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireReadComplete();
    }

    pub fn onErrorCaught(self: *Self, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        _ = self;
        ctx.fireErrorCaught(err);
    }

    /// Outbound write: encrypt via SSL_write, then drain the write BIO.
    /// Before the handshake this is a no-op passthrough (shouldn't be called
    /// in normal usage, but be safe).
    pub fn write(
        self: *Self,
        ctx: *p2p_conn.ConnHandlerContext,
        plaintext: []const u8,
        ud: ?*anyopaque,
        cb: *const fn (?*anyopaque, anyerror!usize) void,
    ) void {
        if (!self.handshake_done) {
            // Pass through during handshake (shouldn't happen in normal usage)
            ctx.write(plaintext, ud, cb);
            return;
        }
        const ret = ssl.SSL_write(self.ssl_handle, plaintext.ptr, @intCast(plaintext.len));
        if (ret <= 0) {
            cb(ud, error.TlsWriteFailed);
            return;
        }
        const plaintext_len: usize = @intCast(ret);

        // Drain the encrypted output from the write BIO into a single allocation
        const pending = ssl.BIO_pending(self.write_bio);
        if (pending <= 0) {
            // No encrypted output (shouldn't happen after SSL_write) — report success anyway
            cb(ud, plaintext_len);
            return;
        }
        const encrypted = self.allocator.alloc(u8, @intCast(pending)) catch {
            cb(ud, error.OutOfMemory);
            return;
        };
        const n = ssl.BIO_read(self.write_bio, encrypted.ptr, @intCast(pending));
        if (n <= 0) {
            self.allocator.free(encrypted);
            cb(ud, plaintext_len);
            return;
        }
        const wc = self.allocator.create(TlsWriteCtx) catch {
            self.allocator.free(encrypted);
            cb(ud, error.OutOfMemory);
            return;
        };
        wc.* = .{
            .allocator = self.allocator,
            .encrypted_buf = encrypted,
            .plaintext_len = plaintext_len,
            .user_ctx = ud,
            .user_cb = cb,
        };
        ctx.write(encrypted[0..@intCast(n)], wc, TlsWriteCtx.callback);
    }

    pub fn close(
        self: *Self,
        ctx: *p2p_conn.ConnHandlerContext,
        ud: ?*anyopaque,
        cb: *const fn (?*anyopaque, anyerror!void) void,
    ) void {
        _ = self;
        ctx.close(ud, cb);
    }

    // --- Static vtable wrappers --------------------------------------------

    fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) !void {
        return @as(*Self, @ptrCast(@alignCast(instance))).onActive(ctx);
    }
    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        @as(*Self, @ptrCast(@alignCast(instance))).onInactive(ctx);
    }
    fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        return @as(*Self, @ptrCast(@alignCast(instance))).onRead(ctx, msg);
    }
    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        @as(*Self, @ptrCast(@alignCast(instance))).onReadComplete(ctx);
    }
    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        @as(*Self, @ptrCast(@alignCast(instance))).onErrorCaught(ctx, err);
    }
    fn vtableWriteFn(
        instance: *anyopaque,
        ctx: *p2p_conn.ConnHandlerContext,
        buffer: []const u8,
        ud: ?*anyopaque,
        cb: *const fn (?*anyopaque, anyerror!usize) void,
    ) void {
        @as(*Self, @ptrCast(@alignCast(instance))).write(ctx, buffer, ud, cb);
    }
    fn vtableCloseFn(
        instance: *anyopaque,
        ctx: *p2p_conn.ConnHandlerContext,
        ud: ?*anyopaque,
        cb: *const fn (?*anyopaque, anyerror!void) void,
    ) void {
        @as(*Self, @ptrCast(@alignCast(instance))).close(ctx, ud, cb);
    }

    const vtable_instance = p2p_conn.ConnHandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

// ---------------------------------------------------------------------------
// TlsTcpChannel — the AnyProtocolBinding for /tls/1.0.0
// ---------------------------------------------------------------------------

pub const TlsTcpChannel = struct {
    protocol_descriptor: ProtocolDescriptor,
    allocator: Allocator,
    /// Borrowed reference — must outlive this channel.
    host_public_key: *const keys.PublicKey,
    host_sign_ctx: ?*anyopaque,
    host_sign_fn: tls.SignCallback,

    const Self = @This();
    const tls_protocol_id: ProtocolId = "/tls/1.0.0";
    const announcements: []const ProtocolId = &[_]ProtocolId{tls_protocol_id};

    pub fn init(
        self: *Self,
        allocator: Allocator,
        host_public_key: *const keys.PublicKey,
        host_sign_ctx: ?*anyopaque,
        host_sign_fn: tls.SignCallback,
    ) !void {
        var proto_matcher: ProtocolMatcher = undefined;
        try proto_matcher.initAsStrict(allocator, tls_protocol_id);
        errdefer proto_matcher.deinit();

        var proto_desc: ProtocolDescriptor = undefined;
        try proto_desc.init(allocator, announcements, proto_matcher);
        errdefer proto_desc.deinit();

        self.* = .{
            .protocol_descriptor = proto_desc,
            .allocator = allocator,
            .host_public_key = host_public_key,
            .host_sign_ctx = host_sign_ctx,
            .host_sign_fn = host_sign_fn,
        };
    }

    pub fn deinit(self: *Self) void {
        self.protocol_descriptor.deinit();
    }

    pub fn getProtoDesc(self: *Self) *ProtocolDescriptor {
        return &self.protocol_descriptor;
    }

    /// Called by ConnUpgrader after multistream negotiates /tls/1.0.0.
    /// Builds the per-connection SSL context + handler and inserts it into
    /// the pipeline. Async: the callback is fired when the TLS handshake
    /// completes (or fails).
    pub fn initConn(
        self: *Self,
        conn: p2p_conn.AnyConn,
        _: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!?*anyopaque) void,
    ) void {
        self.initConnImpl(conn, user_data, callback) catch |err| {
            callback(user_data, err);
        };
    }

    fn initConnImpl(
        self: *Self,
        conn: p2p_conn.AnyConn,
        user_data: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!?*anyopaque) void,
    ) !void {
        // 1. Generate an ephemeral key pair for this connection's TLS certificate.
        const subject_key = try tls.generateKeyPair(.ECDSA);
        // Always freed at end of scope — SSL_CTX_use_PrivateKey makes an internal copy.
        defer ssl.EVP_PKEY_free(subject_key);

        // 2. Build a libp2p self-signed certificate embedding our identity key.
        const cert = try tls.buildCert(
            self.allocator,
            self.host_public_key,
            self.host_sign_ctx,
            self.host_sign_fn,
            subject_key,
        );
        // Always freed at end of scope — SSL_CTX_use_certificate makes an internal copy.
        defer ssl.X509_free(cert);

        // 3. Create per-connection SSL_CTX
        const ssl_ctx = ssl.SSL_CTX_new(ssl.TLS_method()) orelse return error.OpenSSLFailed;
        errdefer ssl.SSL_CTX_free(ssl_ctx);

        // Load our certificate and private key into the context
        if (ssl.SSL_CTX_use_certificate(ssl_ctx, cert) <= 0) return error.OpenSSLFailed;
        if (ssl.SSL_CTX_use_PrivateKey(ssl_ctx, subject_key) <= 0) return error.OpenSSLFailed;

        // Require peer certificate; accept self-signed via libp2pVerifyCallback
        ssl.SSL_CTX_set_verify(ssl_ctx, ssl.SSL_VERIFY_PEER | ssl.SSL_VERIFY_FAIL_IF_NO_PEER_CERT, tls.libp2pVerifyCallback);

        // ALPN: advertise "libp2p" on client; select it on server
        if (conn.direction() == .OUTBOUND) {
            _ = ssl.SSL_CTX_set_alpn_protos(ssl_ctx, tls.ALPN_PROTOS.ptr, @intCast(tls.ALPN_PROTOS.len));
        } else {
            ssl.SSL_CTX_set_alpn_select_cb(ssl_ctx, tls.alpnSelectCallbackfn, null);
        }

        // 4. Create SSL object
        const ssl_handle = ssl.SSL_new(ssl_ctx) orelse return error.OpenSSLFailed;
        errdefer ssl.SSL_free(ssl_handle);

        // 5. Create memory BIOs; save pointers BEFORE handing ownership to SSL
        const rbio = ssl.BIO_new(ssl.BIO_s_mem()) orelse return error.OpenSSLFailed;
        const wbio = ssl.BIO_new(ssl.BIO_s_mem()) orelse {
            _ = ssl.BIO_free(rbio);
            return error.OpenSSLFailed;
        };
        // SSL_set_bio takes ownership of both BIOs
        ssl.SSL_set_bio(ssl_handle, rbio, wbio);

        // 6. Set client or server mode
        if (conn.direction() == .OUTBOUND) {
            ssl.SSL_set_connect_state(ssl_handle);
        } else {
            ssl.SSL_set_accept_state(ssl_handle);
        }

        // 7. Allocate the handler on the pipeline allocator (same lifetime as pipeline)
        const handler = try conn.getPipeline().allocator.create(TlsTcpHandler);
        handler.* = .{
            .allocator = conn.getPipeline().allocator,
            .ssl_ctx = ssl_ctx,
            .ssl_handle = ssl_handle,
            .read_bio = rbio,
            .write_bio = wbio,
            .user_data = user_data,
            .callback = callback,
        };

        // 8. Insert into the pipeline; it will receive onActive next
        try conn.getPipeline().addLast("tls_tcp", handler.any());
    }

    // --- Static vtable wrappers --------------------------------------------

    fn vtableProtoDescFn(instance: *anyopaque) *ProtocolDescriptor {
        return @as(*Self, @ptrCast(@alignCast(instance))).getProtoDesc();
    }

    fn vtableInitConnFn(
        instance: *anyopaque,
        conn: p2p_conn.AnyConn,
        protocol_id: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (?*anyopaque, anyerror!?*anyopaque) void,
    ) void {
        @as(*Self, @ptrCast(@alignCast(instance))).initConn(conn, protocol_id, user_data, callback);
    }

    const vtable_instance = proto_binding.ProtocolBindingVTable{
        .initConnFn = vtableInitConnFn,
        .protoDescFn = vtableProtoDescFn,
    };

    pub fn any(self: *Self) proto_binding.AnyProtocolBinding {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};
