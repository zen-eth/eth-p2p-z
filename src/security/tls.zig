//! libp2p-flavored TLS for QUIC. Not generic TLS: certificates carry a libp2p
//! `signed-key-extension` (OID `1.3.6.1.4.1.53594.1.1`) embedding the host's
//! libp2p identity public key plus a signature binding the TLS keypair to that
//! identity. The peer-side verifier parses that extension and recovers the
//! authenticated `keys.PublicKey`; peers without it are rejected.

const std = @import("std");
const ssl = @import("ssl").c;
const quiche = @import("quiche").c;
const secp = @import("secp256k1");
const secp_context = @import("../secp_context.zig");
const Allocator = std.mem.Allocator;
const keys = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;

pub const ALPN = "libp2p";

pub const ALPN_PROTOS = @as([1]u8, .{@intCast(ALPN.len)}) ++ ALPN;

/// This is the prefix libp2p uses for signing the certificate extension.
const CertificatePrefix = "libp2p-tls-handshake:";
/// This is the OID for the libp2p self-signed certificate extension.
const Libp2pExtensionOid = "1.3.6.1.4.1.53594.1.1";
/// The offset to apply to the certificate's notBefore field.
const CertNotBeforeOffsetSeconds = -3600; // 1 hour before current time
/// The offset to apply to the certificate's notAfter field.
const CertNotAfterOffsetSeconds = 365 * 24 * 3600; // 1 year after current time

const SignatureAlgs: []const u16 = &.{
    ssl.SSL_SIGN_ED25519,
    ssl.SSL_SIGN_ECDSA_SECP256R1_SHA256,
    ssl.SSL_SIGN_RSA_PSS_RSAE_SHA256,
    ssl.SSL_SIGN_RSA_PKCS1_SHA256,
};

pub const Error = error{
    CertCreationFailed,
    CertNameCreationFailed,
    CertVersionSetFailed,
    CertSerialCreationFailed,
    CertSerialSetFailed,
    CertPKeySetFailed,
    CertIssuerSetFailed,
    CertValidPeriodSetFailed,
    CertSubjectSetFailed,
    CertExtCreationFailed,
    CertExtSetFailed,
    CertSignCreationFailed,
    PubKeyTODerFailed,
    RawPubKeyGetFailed,
    OpenSSLFailed,
    InvalidOID,
    SignDataFailed,
    SignCertFailed,
    UnsupportedKeyType,
    IncompatibleCertificateExtension,
    InvalidKeyLength,
    InitializationFailed,
};

pub const SignCallbackError = Error || error{
    UnsupportedBackend,
    KeyMaterialReleased,
    OutOfMemory,
    InvalidVarInt,
    InvalidTag,
    InvalidData,
    InvalidSize,
};

pub const ContextCreateError = SignCallbackError || Allocator.Error;

pub const ExtensionData = struct {
    host_pubkey: []u8,
    signature: []u8,
};

pub const SignCallback = *const fn (ctx: ?*anyopaque, allocator: Allocator, data: []const u8) SignCallbackError![]u8;

pub const Context = struct {
    ssl_ctx: *ssl.SSL_CTX,
    subject_keypair: *ssl.EVP_PKEY,
    subject_cert: *ssl.X509,
    local_peer_id: PeerId,

    pub fn create(
        allocator: Allocator,
        host_keypair: anytype,
        cert_key_type: keys.KeyType,
        sign_ctx: ?*anyopaque,
        sign_fn: SignCallback,
    ) ContextCreateError!Context {
        const subject_keypair = try generateKeyPair(cert_key_type);
        errdefer ssl.EVP_PKEY_free(subject_keypair);

        var host_pubkey = try host_keypair.publicKey(allocator);
        defer if (host_pubkey.data) |data| allocator.free(data);

        const subject_cert = try buildCert(allocator, &host_pubkey, sign_ctx, sign_fn, subject_keypair);
        errdefer ssl.X509_free(subject_cert);

        const ssl_ctx = try createSslContext(subject_keypair, subject_cert);
        errdefer ssl.SSL_CTX_free(ssl_ctx);

        return .{
            .ssl_ctx = ssl_ctx,
            .subject_keypair = subject_keypair,
            .subject_cert = subject_cert,
            .local_peer_id = try PeerId.fromPublicKey(allocator, &host_pubkey),
        };
    }

    pub fn deinit(ctx: *Context) void {
        ssl.SSL_CTX_free(ctx.ssl_ctx);
        ssl.X509_free(ctx.subject_cert);
        ssl.EVP_PKEY_free(ctx.subject_keypair);
        ctx.* = undefined;
    }

    pub fn newSsl(ctx: *const Context) !*ssl.SSL {
        return ssl.SSL_new(ctx.ssl_ctx) orelse error.OpenSSLFailed;
    }
};

pub fn createSslContext(subject_key: *ssl.EVP_PKEY, cert: *ssl.X509) !*ssl.SSL_CTX {
    const ssl_ctx = ssl.SSL_CTX_new(ssl.TLS_method()) orelse return error.InitializationFailed;
    errdefer ssl.SSL_CTX_free(ssl_ctx);

    if (ssl.SSL_CTX_set_min_proto_version(ssl_ctx, ssl.TLS1_3_VERSION) == 0) return error.InitializationFailed;
    if (ssl.SSL_CTX_set_max_proto_version(ssl_ctx, ssl.TLS1_3_VERSION) == 0) return error.InitializationFailed;

    _ = ssl.SSL_CTX_set_options(
        ssl_ctx,
        ssl.SSL_OP_NO_TLSv1 |
            ssl.SSL_OP_NO_TLSv1_1 |
            ssl.SSL_OP_NO_TLSv1_2 |
            ssl.SSL_OP_NO_COMPRESSION |
            ssl.SSL_OP_NO_SSLv2 |
            ssl.SSL_OP_NO_SSLv3,
    );

    ssl.SSL_CTX_set_verify(
        ssl_ctx,
        ssl.SSL_VERIFY_PEER | ssl.SSL_VERIFY_FAIL_IF_NO_PEER_CERT | ssl.SSL_VERIFY_CLIENT_ONCE,
        libp2pVerifyCallback,
    );

    if (ssl.SSL_CTX_set_verify_algorithm_prefs(ssl_ctx, SignatureAlgs.ptr, @intCast(SignatureAlgs.len)) == 0)
        return error.InitializationFailed;
    if (ssl.SSL_CTX_set_signing_algorithm_prefs(ssl_ctx, SignatureAlgs.ptr, @intCast(SignatureAlgs.len)) == 0)
        return error.InitializationFailed;
    if (ssl.SSL_CTX_use_PrivateKey(ssl_ctx, subject_key) == 0) return error.InitializationFailed;
    if (ssl.SSL_CTX_use_certificate(ssl_ctx, cert) == 0) return error.InitializationFailed;
    if (ssl.SSL_CTX_set_alpn_protos(ssl_ctx, ALPN_PROTOS.ptr, @intCast(ALPN_PROTOS.len)) != 0)
        return error.InitializationFailed;
    ssl.SSL_CTX_set_alpn_select_cb(ssl_ctx, alpnSelectCallbackfn, null);

    // Wired only when SSLKEYLOGFILE is set, so it is inert in normal operation.
    if (std.c.getenv("SSLKEYLOGFILE") != null)
        ssl.SSL_CTX_set_keylog_callback(ssl_ctx, keylogCallbackFn);

    return ssl_ctx;
}

/// Appends one NSS-format secret line to SSLKEYLOGFILE so external tools can
/// decrypt captured QUIC/TLS 1.3 traffic. Best-effort: I/O errors are ignored so
/// it never disturbs the handshake; O_APPEND + single write keeps entries atomic.
fn keylogCallbackFn(_: ?*const ssl.SSL, line: [*c]const u8) callconv(.c) void {
    if (line == null) return;
    const path_z = std.c.getenv("SSLKEYLOGFILE") orelse return;
    const line_slice = std.mem.span(@as([*:0]const u8, @ptrCast(line)));
    var buf: [512]u8 = undefined;
    if (line_slice.len + 1 > buf.len) return;
    @memcpy(buf[0..line_slice.len], line_slice);
    buf[line_slice.len] = '\n';
    const fd = std.c.open(
        path_z,
        .{ .ACCMODE = .WRONLY, .CREAT = true, .APPEND = true },
        @as(c_uint, 0o600),
    );
    if (fd < 0) return;
    defer _ = std.c.close(fd);
    _ = std.c.write(fd, &buf, line_slice.len + 1);
}

/// Generates a new key pair for the given key type. SECP256K1 yields
/// `Error.UnsupportedKeyType` (no BoringSSL support).
pub fn generateKeyPair(cert_key_type: keys.KeyType) !*ssl.EVP_PKEY {
    var maybe_subject_keypair: ?*ssl.EVP_PKEY = null;

    if (cert_key_type == .ECDSA or cert_key_type == .SECP256K1) {
        const curve_nid = switch (cert_key_type) {
            .ECDSA => ssl.NID_X9_62_prime256v1,
            // SECP256K1 is not supported in BoringSSL
            .SECP256K1 => return error.UnsupportedKeyType,
            else => unreachable,
        };

        var maybe_params: ?*ssl.EVP_PKEY = null;
        {
            const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_EC, null) orelse return error.OpenSSLFailed;
            defer ssl.EVP_PKEY_CTX_free(pctx);

            if (ssl.EVP_PKEY_paramgen_init(pctx) <= 0) {
                return error.OpenSSLFailed;
            }

            if (ssl.EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, curve_nid) <= 0) {
                return error.OpenSSLFailed;
            }

            if (ssl.EVP_PKEY_paramgen(pctx, &maybe_params) <= 0) {
                return error.OpenSSLFailed;
            }
        }
        const params = maybe_params orelse return error.OpenSSLFailed;
        defer ssl.EVP_PKEY_free(params);

        {
            const kctx = ssl.EVP_PKEY_CTX_new(params, null) orelse return error.OpenSSLFailed;
            defer ssl.EVP_PKEY_CTX_free(kctx);

            if (ssl.EVP_PKEY_keygen_init(kctx) <= 0) {
                return error.OpenSSLFailed;
            }

            if (ssl.EVP_PKEY_keygen(kctx, &maybe_subject_keypair) <= 0) {
                return error.OpenSSLFailed;
            }
        }
    } else {
        const key_alg_id = switch (cert_key_type) {
            .ED25519 => ssl.EVP_PKEY_ED25519,
            .RSA => ssl.EVP_PKEY_RSA,
            else => unreachable,
        };

        const pctx = ssl.EVP_PKEY_CTX_new_id(key_alg_id, null) orelse return error.OpenSSLFailed;
        defer ssl.EVP_PKEY_CTX_free(pctx);

        if (ssl.EVP_PKEY_keygen_init(pctx) <= 0) {
            return error.OpenSSLFailed;
        }

        if (ssl.EVP_PKEY_keygen(pctx, &maybe_subject_keypair) <= 0) {
            return error.OpenSSLFailed;
        }
    }

    return maybe_subject_keypair orelse return error.OpenSSLFailed;
}

/// Deterministic Ed25519 key from a raw 32-byte private seed: same seed always
/// yields the same key. Matches Go's `ed25519.NewKeyFromSeed(seed)`, so the
/// derived libp2p peer-id is byte-identical across implementations. Caller owns
/// the returned key and must free it with `ssl.EVP_PKEY_free()`.
pub fn ed25519KeyFromSeed(seed: []const u8) !*ssl.EVP_PKEY {
    if (seed.len != 32) return error.InvalidKeyLength;
    return ssl.EVP_PKEY_new_raw_private_key(
        ssl.EVP_PKEY_ED25519,
        null,
        seed.ptr,
        seed.len,
    ) orelse error.OpenSSLFailed;
}

/// Builds a self-signed X.509 certificate for libp2p's TLS handshake. Caller owns
/// the returned cert and must free it with `ssl.X509_free()`.
///
/// `host_public_key` is the identity key bound by the libp2p extension; `host_sign_fn`
/// (with its ctx) signs the extension payload. `subjectKey` is the cert's own keypair:
/// its public key is the cert subject key, its private key signs the cert.
pub fn buildCert(
    allocator: Allocator,
    host_public_key: *const keys.PublicKey,
    host_sign_ctx: ?*anyopaque,
    host_sign_fn: SignCallback,
    subjectKey: *ssl.EVP_PKEY,
) !*ssl.X509 {
    const cert = ssl.X509_new() orelse return error.CertCreationFailed;
    errdefer ssl.X509_free(cert);

    if (ssl.X509_set_version(cert, ssl.X509_VERSION_3) <= 0) return error.CertVersionSetFailed;

    const serial = ssl.ASN1_INTEGER_new() orelse return error.CertSerialCreationFailed;
    defer ssl.ASN1_INTEGER_free(serial);

    var random_serial_bytes: [8]u8 = undefined;
    if (ssl.RAND_bytes(&random_serial_bytes, random_serial_bytes.len) != 1) return error.CertSerialCreationFailed;
    var random_serial_u64: u64 = @bitCast(random_serial_bytes);
    if (random_serial_u64 == 0) random_serial_u64 = 1;

    if (ssl.ASN1_INTEGER_set_uint64(serial, random_serial_u64) <= 0) return error.CertSerialSetFailed;
    if (ssl.X509_set_serialNumber(cert, serial) <= 0) return error.CertSerialSetFailed;

    if (ssl.X509_set_pubkey(cert, subjectKey) <= 0) return error.CertPKeySetFailed;

    const name = ssl.X509_NAME_new() orelse return error.CertNameCreationFailed;
    defer ssl.X509_NAME_free(name);
    if (ssl.X509_NAME_add_entry_by_txt(name, "C", ssl.MBSTRING_ASC, "US", -1, -1, 0) <= 0) return error.CertNameCreationFailed;
    if (ssl.X509_NAME_add_entry_by_txt(name, "O", ssl.MBSTRING_ASC, "libp2p", -1, -1, 0) <= 0) return error.CertNameCreationFailed;
    if (ssl.X509_NAME_add_entry_by_txt(name, "CN", ssl.MBSTRING_ASC, "libp2p", -1, -1, 0) <= 0) return error.CertNameCreationFailed;
    if (ssl.X509_set_issuer_name(cert, name) <= 0) return error.CertIssuerSetFailed;
    if (ssl.X509_set_subject_name(cert, name) <= 0) return error.CertSubjectSetFailed;

    if (ssl.X509_gmtime_adj(ssl.X509_get_notBefore(cert), CertNotBeforeOffsetSeconds) == null) return error.CertValidPeriodSetFailed;
    if (ssl.X509_gmtime_adj(ssl.X509_get_notAfter(cert), CertNotAfterOffsetSeconds) == null) return error.CertValidPeriodSetFailed;

    var subj_pubkey_ptr: [*c]u8 = null;
    const subj_pubkey_len = ssl.i2d_PUBKEY(subjectKey, &subj_pubkey_ptr);
    if (subj_pubkey_len <= 0) return error.PubKeyTODerFailed;
    defer ssl.OPENSSL_free(subj_pubkey_ptr);
    const subject_pubkey_der = subj_pubkey_ptr[0..@intCast(subj_pubkey_len)];

    const data_to_sign = try allocator.alloc(u8, CertificatePrefix.len + subject_pubkey_der.len);
    defer allocator.free(data_to_sign);
    @memcpy(data_to_sign[0..CertificatePrefix.len], CertificatePrefix);
    @memcpy(data_to_sign[CertificatePrefix.len..], subject_pubkey_der);

    const signature = host_sign_fn(host_sign_ctx, allocator, data_to_sign) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.SignDataFailed,
    };
    defer allocator.free(signature);

    const host_pubkey_proto = host_public_key.encode(allocator) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer allocator.free(host_pubkey_proto);

    var ext_value_der: [*c]u8 = null;
    const ext_value_der_len = try createExtension(host_pubkey_proto, signature, &ext_value_der);
    defer ssl.OPENSSL_free(ext_value_der);

    try addExtension(cert, Libp2pExtensionOid, true, ext_value_der[0..@intCast(ext_value_der_len)]);

    const base_id = ssl.EVP_PKEY_base_id(subjectKey);
    switch (base_id) {
        ssl.EVP_PKEY_ED25519 => {
            if (ssl.X509_sign(cert, subjectKey, null) <= 0) {
                return error.SignCertFailed;
            }
        },

        ssl.EVP_PKEY_EC => {
            const md = ssl.EVP_sha256();
            if (ssl.X509_sign(cert, subjectKey, md) <= 0) {
                return error.SignCertFailed;
            }
        },

        ssl.EVP_PKEY_RSA => {
            const md = ssl.EVP_sha256();
            const md_ctx = ssl.EVP_MD_CTX_new() orelse return error.CertSignCreationFailed;
            defer ssl.EVP_MD_CTX_free(md_ctx);

            var pkey_ctx: ?*ssl.EVP_PKEY_CTX = null;
            if (ssl.EVP_DigestSignInit(md_ctx, &pkey_ctx, md, null, subjectKey) <= 0) {
                return error.SignCertFailed;
            }

            const ctx = pkey_ctx orelse return error.SignCertFailed;

            if (ssl.EVP_PKEY_CTX_set_rsa_padding(ctx, ssl.RSA_PKCS1_PSS_PADDING) <= 0) {
                return error.SignCertFailed;
            }

            if (ssl.EVP_PKEY_CTX_set_rsa_pss_saltlen(ctx, -1) <= 0) {
                return error.SignCertFailed;
            }

            if (ssl.EVP_PKEY_CTX_set_rsa_mgf1_md(ctx, md) <= 0) {
                return error.SignCertFailed;
            }

            if (ssl.X509_sign_ctx(cert, md_ctx) <= 0) {
                return error.SignCertFailed;
            }
        },

        else => return error.UnsupportedKeyType,
    }

    return cert;
}

/// Encodes a public key into the libp2p PublicKey protobuf wire format and
/// returns the raw bytes — the slice variant of `createProtobufEncodedPublicKey`.
/// The caller owns the returned slice.
pub fn createProtobufEncodedPublicKeyBuf(allocator: Allocator, pkey: *ssl.EVP_PKEY) ![]const u8 {
    var public_key_proto = try createProtobufEncodedPublicKey(allocator, pkey);
    defer allocator.free(public_key_proto.data.?);

    var proto_bytes = try public_key_proto.encode(allocator);

    if (public_key_proto.type == .RSA) {
        // RSA is the default key type (0) so the encoder omits it. Append the tag/value to
        // match other implementations when hashing protobuf-encoded keys.
        const type_field = [_]u8{ 0x08, 0x00 };
        const augmented = try std.mem.concat(allocator, u8, &.{ &type_field, proto_bytes });
        allocator.free(proto_bytes);
        proto_bytes = augmented;
    }
    return proto_bytes;
}

/// Encodes a public key into the libp2p PublicKey protobuf format, returning a
/// `keys.PublicKey` struct (vs a raw byte slice). The caller owns the returned
/// struct's `data` slice.
pub fn createProtobufEncodedPublicKey(allocator: Allocator, pkey: *ssl.EVP_PKEY) !keys.PublicKey {
    const raw_pubkey = try getRawPublicKeyBytes(allocator, pkey);
    errdefer allocator.free(raw_pubkey);

    const key_type_enum: u8 = blk: {
        const base_id = ssl.EVP_PKEY_base_id(pkey);

        if (base_id == ssl.EVP_PKEY_RSA) {
            break :blk 0;
        }
        if (base_id == ssl.EVP_PKEY_ED25519) {
            break :blk 1;
        }
        if (base_id == ssl.EVP_PKEY_EC) {
            const ec_key = ssl.EVP_PKEY_get0_EC_KEY(pkey);
            if (ec_key == null) return error.OpenSSLFailed;
            const group = ssl.EC_KEY_get0_group(ec_key);
            if (group == null) return error.OpenSSLFailed;

            const curve_nid = ssl.EC_GROUP_get_curve_name(group);
            switch (curve_nid) {
                // secp256k1 is out of scope for the *certificate* key by design:
                // TLS 1.3 defines no signature scheme for it (only the secp*r1
                // curves). It only appears as a host *identity*, verified in
                // software via verifyHostSignature -> verifySecp256k1Signature.
                ssl.NID_secp256k1 => return error.UnsupportedKeyType,
                ssl.NID_X9_62_prime256v1 => break :blk 3,
                else => return error.UnsupportedKeyType,
            }
        }
        return error.UnsupportedKeyType;
    };

    const public_key_proto = keys.PublicKey{
        .type = @enumFromInt(key_type_enum),
        .data = raw_pubkey,
    };

    return public_key_proto;
}

/// Gets the raw public key bytes from an EVP_PKEY.
/// The caller owns the returned slice.
fn getRawPublicKeyBytes(allocator: Allocator, evp_key: *ssl.EVP_PKEY) ![]const u8 {
    const base_id = ssl.EVP_PKEY_base_id(evp_key);

    if (base_id == ssl.EVP_PKEY_ED25519) {
        // BoringSSL two-call idiom: query length, then fill.
        var len: usize = 0;
        if (ssl.EVP_PKEY_get_raw_public_key(evp_key, null, &len) != 1) {
            return error.RawPubKeyGetFailed;
        }
        const key = try allocator.alloc(u8, len);
        errdefer allocator.free(key);

        if (ssl.EVP_PKEY_get_raw_public_key(evp_key, key.ptr, &len) != 1) {
            return error.RawPubKeyGetFailed;
        }
        return key;
    }

    // ECDSA and RSA: i2d_PUBKEY gives the PKIX DER (SubjectPublicKeyInfo).
    if (base_id == ssl.EVP_PKEY_EC or base_id == ssl.EVP_PKEY_RSA) {
        var key_ptr: [*c]u8 = null;
        const len = ssl.i2d_PUBKEY(evp_key, &key_ptr);
        if (len < 0 or key_ptr == null) {
            return error.RawPubKeyGetFailed;
        }
        defer ssl.OPENSSL_free(key_ptr);

        const key = try allocator.alloc(u8, @intCast(len));
        @memcpy(key, key_ptr[0..@intCast(len)]);
        return key;
    }

    return error.UnsupportedKeyType;
}

/// Signs arbitrary data using the private key within an EVP_PKEY.
/// The caller owns the returned slice.
pub fn signData(allocator: Allocator, pkey: *ssl.EVP_PKEY, data: []const u8) ![]u8 {
    const ctx = ssl.EVP_MD_CTX_new() orelse return error.CertSignCreationFailed;
    defer ssl.EVP_MD_CTX_free(ctx);

    const message_digest: ?*const ssl.EVP_MD = switch (ssl.EVP_PKEY_base_id(pkey)) {
        ssl.EVP_PKEY_ED25519 => null,

        ssl.EVP_PKEY_EC, ssl.EVP_PKEY_RSA => ssl.EVP_sha256(),

        else => return error.UnsupportedKeyType,
    };

    if (ssl.EVP_PKEY_base_id(pkey) == ssl.EVP_PKEY_ED25519) {
        if (ssl.EVP_DigestSignInit(ctx, null, message_digest, null, pkey) <= 0) {
            return error.CertSignCreationFailed;
        }

        var sig_len: usize = 0;
        if (ssl.EVP_DigestSign(ctx, null, &sig_len, data.ptr, data.len) <= 0) {
            return error.CertSignCreationFailed;
        }

        var sig_buf = try allocator.alloc(u8, sig_len);
        errdefer allocator.free(sig_buf);

        if (ssl.EVP_DigestSign(ctx, sig_buf.ptr, &sig_len, data.ptr, data.len) <= 0) {
            return error.CertSignCreationFailed;
        }

        if (sig_len != sig_buf.len) sig_buf = try allocator.realloc(sig_buf, sig_len);
        return sig_buf;
    }

    if (ssl.EVP_DigestSignInit(ctx, null, message_digest, null, pkey) <= 0) {
        return error.CertSignCreationFailed;
    }

    if (ssl.EVP_DigestSignUpdate(ctx, data.ptr, data.len) <= 0) {
        return error.CertSignCreationFailed;
    }

    var sig_len: usize = 0;
    if (ssl.EVP_DigestSignFinal(ctx, null, &sig_len) <= 0) {
        return error.CertSignCreationFailed;
    }

    var sig_buf = try allocator.alloc(u8, sig_len);
    errdefer allocator.free(sig_buf);

    if (ssl.EVP_DigestSignFinal(ctx, sig_buf.ptr, &sig_len) <= 0) {
        return error.CertSignCreationFailed;
    }

    if (sig_len != sig_buf.len) {
        sig_buf = try allocator.realloc(sig_buf, sig_len);
    }

    return sig_buf;
}

pub fn signDataWithTlsKey(ctx: ?*anyopaque, allocator: Allocator, data: []const u8) SignCallbackError![]u8 {
    const key_ptr: *ssl.EVP_PKEY = @ptrCast(@alignCast(ctx orelse return error.SignDataFailed));
    return signData(allocator, key_ptr, data);
}

/// Extracts the verified libp2p host public key from the peer certificate of
/// a fully-handshaked quiche connection. The returned `keys.PublicKey` owns
/// its `data` slice; the caller is responsible for freeing it via
/// `allocator.free(pub_key.data.?)`.
pub fn extractPublicKey(allocator: Allocator, conn: *quiche.quiche_conn) !keys.PublicKey {
    var cert_ptr: [*c]const u8 = null;
    var cert_len: usize = 0;
    quiche.quiche_conn_peer_cert(conn, &cert_ptr, &cert_len);
    // Missing cert, parse failure, and a bad signature all mean the same to the
    // caller: peer identity unverified. Surface a distinct PeerVerifyFailed (not
    // opaque HandshakeFailed) so the actor records `peer_verify_failed`; OOM
    // stays itself, as it is not a verification verdict.
    if (cert_ptr == null or cert_len == 0) return error.PeerVerifyFailed;

    var der_ptr = cert_ptr;
    const cert = ssl.d2i_X509(null, &der_ptr, @intCast(cert_len)) orelse return error.PeerVerifyFailed;
    defer ssl.X509_free(cert);

    const info = verifyAndExtractPeerInfo(allocator, cert) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.PeerVerifyFailed,
    };
    if (!info.is_valid) {
        if (info.host_pubkey.data) |data| allocator.free(data);
        return error.PeerVerifyFailed;
    }
    return info.host_pubkey;
}

/// Result of verifying a peer certificate. `host_pubkey.data` is heap-allocated
/// and populated regardless of `is_valid`, so the caller must free it on BOTH
/// the valid and invalid paths. `peer_id` is a value type owning no heap.
pub const PeerVerification = struct {
    is_valid: bool,
    host_pubkey: keys.PublicKey,
    peer_id: PeerId,
};

pub fn verifyAndExtractPeerInfo(allocator: Allocator, cert: *const ssl.X509) !PeerVerification {
    const ext_data = try extractExtensionFields(allocator, cert);
    defer {
        allocator.free(ext_data.host_pubkey);
        allocator.free(ext_data.signature);
    }

    const host_pubkey_reader = try keys.PublicKeyReader.init(ext_data.host_pubkey);

    var host_pubkey = keys.PublicKey{
        .type = host_pubkey_reader.getType(),
        .data = try allocator.dupe(u8, host_pubkey_reader.getData()),
    };
    errdefer allocator.free(host_pubkey.data.?);

    const peer_id = try PeerId.fromPublicKey(allocator, &host_pubkey);

    // X509_get_pubkey wants a mutable *X509 but only bumps the pubkey's refcount
    // (no semantic mutation of the cert), so the const-cast is sound here.
    const cert_pkey = ssl.X509_get_pubkey(@constCast(cert));
    if (cert_pkey == null) return error.InvalidCertificate;
    defer ssl.EVP_PKEY_free(cert_pkey);

    var cert_pubkey_ptr: [*c]u8 = null;
    const cert_pubkey_len = ssl.i2d_PUBKEY(cert_pkey, &cert_pubkey_ptr);
    if (cert_pubkey_len <= 0) return error.InvalidCertificate;
    defer ssl.OPENSSL_free(cert_pubkey_ptr);
    const cert_pubkey_der = cert_pubkey_ptr[0..@intCast(cert_pubkey_len)];

    const data_to_verify = try allocator.alloc(u8, CertificatePrefix.len + cert_pubkey_der.len);
    defer allocator.free(data_to_verify);
    @memcpy(data_to_verify[0..CertificatePrefix.len], CertificatePrefix);
    @memcpy(data_to_verify[CertificatePrefix.len..], cert_pubkey_der);

    const is_valid = try verifyHostSignature(&host_pubkey, data_to_verify, ext_data.signature);
    return .{ .is_valid = is_valid, .host_pubkey = host_pubkey, .peer_id = peer_id };
}

pub fn reconstructEvpKeyFromPublicKey(public_key: *const keys.PublicKey) !*ssl.EVP_PKEY {
    const key_data = public_key.data orelse return error.RawPubKeyGetFailed;

    switch (public_key.type) {
        .ED25519 => {
            if (key_data.len != 32) {
                return error.InvalidKeyLength;
            }
            return ssl.EVP_PKEY_new_raw_public_key(ssl.EVP_PKEY_ED25519, null, key_data.ptr, key_data.len) orelse error.OpenSSLFailed;
        },

        .RSA => {
            var key_ptr: [*c]const u8 = key_data.ptr;
            return ssl.d2i_PUBKEY(null, &key_ptr, @intCast(key_data.len)) orelse error.OpenSSLFailed;
        },

        .ECDSA => {
            var key_ptr: [*c]const u8 = key_data.ptr;
            if (ssl.d2i_PUBKEY(null, &key_ptr, @intCast(key_data.len))) |pkey| {
                return pkey;
            }
            return try createEcdsaPkeyFromSec1(key_data);
        },

        .SECP256K1 => {
            return error.UnsupportedKeyType; // BoringSSL not supported
        },
    }
}

fn createEcdsaPkeyFromSec1(sec1_bytes: []const u8) !*ssl.EVP_PKEY {
    if (sec1_bytes.len == 0 or sec1_bytes[0] != 0x04) {
        return error.InvalidKeyLength;
    }

    const curve_nid: c_int = switch (sec1_bytes.len) {
        65 => ssl.NID_X9_62_prime256v1,
        97 => ssl.NID_secp384r1,
        133 => ssl.NID_secp521r1,
        else => return error.InvalidKeyLength,
    };

    var ec_key_managed: ?*ssl.EC_KEY = ssl.EC_KEY_new_by_curve_name(curve_nid);
    const ec_key = ec_key_managed orelse return error.OpenSSLFailed;
    defer if (ec_key_managed) |ptr| ssl.EC_KEY_free(ptr);

    const group = ssl.EC_KEY_get0_group(ec_key) orelse return error.OpenSSLFailed;
    const point = ssl.EC_POINT_new(group) orelse return error.OpenSSLFailed;
    defer ssl.EC_POINT_free(point);

    if (ssl.EC_POINT_oct2point(group, point, sec1_bytes.ptr, sec1_bytes.len, null) != 1) {
        return error.OpenSSLFailed;
    }

    if (ssl.EC_KEY_set_public_key(ec_key, point) != 1) {
        return error.OpenSSLFailed;
    }

    const pkey = ssl.EVP_PKEY_new() orelse return error.OpenSSLFailed;
    errdefer ssl.EVP_PKEY_free(pkey);

    if (ssl.EVP_PKEY_assign_EC_KEY(pkey, ec_key) != 1) {
        return error.OpenSSLFailed;
    }

    // EVP_PKEY_assign_EC_KEY took ownership of ec_key on success; null the managed
    // handle so the `defer EC_KEY_free` above doesn't double-free it.
    ec_key_managed = null;
    return pkey;
}

fn extractExtensionFields(allocator: Allocator, cert: *const ssl.X509) !ExtensionData {
    const obj = ssl.OBJ_txt2obj(Libp2pExtensionOid, 1) orelse return error.InvalidOID;
    defer ssl.ASN1_OBJECT_free(obj);
    const index = ssl.X509_get_ext_by_OBJ(cert, obj, -1);
    if (index < 0) {
        std.log.warn("Certificate does not contain the required extension", .{});
        return error.IncompatibleCertificateExtension;
    }

    const ext = ssl.X509_get_ext(cert, index);

    const os = ssl.X509_EXTENSION_get_data(ext);

    const raw_len = ssl.ASN1_STRING_length(@ptrCast(os));
    const raw_data = ssl.ASN1_STRING_get0_data(@ptrCast(os));

    if (raw_data == null or raw_len <= 0) {
        return error.IncompatibleCertificateExtension;
    }

    return parseExtensionSequence(allocator, raw_data[0..@intCast(raw_len)]);
}

/// Parses the ASN.1 SEQUENCE from the extension data.
/// The sequence contains two OCTET STRINGs: host public key and signature.
fn parseExtensionSequence(allocator: Allocator, der_data: []const u8) !ExtensionData {
    var data_ptr: [*c]const u8 = der_data.ptr;
    const seq_stack = ssl.d2i_ASN1_SEQUENCE_ANY(null, &data_ptr, @intCast(der_data.len));
    if (seq_stack == null) {
        return error.IncompatibleCertificateExtension;
    }
    defer ssl.sk_ASN1_TYPE_free(seq_stack);

    const num_items = ssl.sk_ASN1_TYPE_num(seq_stack);
    if (num_items != 2) {
        std.log.warn("Extension sequence should contain exactly 2 items, found {}", .{num_items});
        return error.IncompatibleCertificateExtension;
    }

    // Extract first OCTET STRING (host public key)
    const host_key_type = ssl.sk_ASN1_TYPE_value(seq_stack, 0);
    if (host_key_type == null or ssl.ASN1_TYPE_get(host_key_type) != ssl.V_ASN1_OCTET_STRING) {
        return error.IncompatibleCertificateExtension;
    }

    const host_key_os = host_key_type.*.value.octet_string;
    if (host_key_os == null) {
        return error.IncompatibleCertificateExtension;
    }

    const host_key_len = ssl.ASN1_STRING_length(@ptrCast(host_key_os));
    const host_key_data = ssl.ASN1_STRING_get0_data(@ptrCast(host_key_os));
    const host_pubkey = try allocator.dupe(u8, host_key_data[0..@intCast(host_key_len)]);
    errdefer allocator.free(host_pubkey);

    const sig_type = ssl.sk_ASN1_TYPE_value(seq_stack, 1);
    if (sig_type == null or ssl.ASN1_TYPE_get(sig_type) != ssl.V_ASN1_OCTET_STRING) {
        return error.IncompatibleCertificateExtension;
    }

    const sig_os = sig_type.*.value.octet_string;
    if (sig_os == null) {
        return error.IncompatibleCertificateExtension;
    }

    const sig_len = ssl.ASN1_STRING_length(@ptrCast(sig_os));
    const sig_data = ssl.ASN1_STRING_get0_data(@ptrCast(sig_os));
    const signature = try allocator.dupe(u8, sig_data[0..@intCast(sig_len)]);

    return .{ .host_pubkey = host_pubkey, .signature = signature };
}

/// Verifies a signature using the provided public key.
pub fn verifySignature(pkey: *ssl.EVP_PKEY, data: []const u8, signature: []const u8) !bool {
    const ctx = ssl.EVP_MD_CTX_new() orelse return error.OpenSSLFailed;
    defer ssl.EVP_MD_CTX_free(ctx);

    const message_digest: ?*const ssl.EVP_MD = switch (ssl.EVP_PKEY_base_id(pkey)) {
        ssl.EVP_PKEY_ED25519 => null,
        ssl.EVP_PKEY_EC, ssl.EVP_PKEY_RSA => ssl.EVP_sha256(),
        else => return error.UnsupportedKeyType,
    };

    if (ssl.EVP_DigestVerifyInit(ctx, null, message_digest, null, pkey) <= 0) {
        return error.OpenSSLFailed;
    }

    if (ssl.EVP_PKEY_base_id(pkey) == ssl.EVP_PKEY_ED25519) {
        const result = ssl.EVP_DigestVerify(ctx, signature.ptr, signature.len, data.ptr, data.len);
        if (result == 1) {
            return true;
        } else if (result == 0) {
            return false;
        } else {
            return error.OpenSSLFailed;
        }
    }

    if (ssl.EVP_DigestVerifyUpdate(ctx, data.ptr, data.len) <= 0) {
        return error.OpenSSLFailed;
    }

    const result = ssl.EVP_DigestVerifyFinal(ctx, signature.ptr, signature.len);
    if (result == 1) {
        return true;
    } else if (result == 0) {
        return false;
    } else {
        return error.OpenSSLFailed;
    }
}

/// Verify a libp2p host signature over `data` using `host_pubkey`: software
/// secp256k1 path for secp256k1 keys, BoringSSL EVP verify (Ed25519/ECDSA/RSA)
/// otherwise. Also used by gossipsub to verify a publisher's message signature.
pub fn verifyHostSignature(host_pubkey: *const keys.PublicKey, data: []const u8, signature: []const u8) !bool {
    switch (host_pubkey.type) {
        .SECP256K1 => return verifySecp256k1Signature(host_pubkey.data orelse return error.InvalidData, data, signature),
        else => {
            const evp_key = try reconstructEvpKeyFromPublicKey(host_pubkey);
            defer ssl.EVP_PKEY_free(evp_key);
            return verifySignature(evp_key, data, signature);
        },
    }
}

fn verifySecp256k1Signature(pubkey_bytes: []const u8, data: []const u8, signature: []const u8) !bool {
    if (signature.len == 0) return false;

    const context = secp_context.get();
    const public_key = secp.PublicKey.fromSlice(pubkey_bytes) catch return error.InvalidData;

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(data);
    const digest = hasher.finalResult();

    const message = secp.Message{ .inner = digest };

    var sig = secp.ecdsa.Signature.fromDer(signature) catch return error.InvalidData;
    // go-libp2p / rust-libp2p accept high-S signatures on verify, but
    // secp256k1_ecdsa_verify rejects them — normalize to low-S first so we are no
    // stricter than the reference impls (idempotent for already-low-S input).
    sig.normalizeS();

    context.verifyEcdsa(message, sig, public_key) catch {
        return false;
    };
    return true;
}

/// Creates the DER-encoded value for the libp2p extension.
/// The value is a SEQUENCE of two OCTET STRINGs. The caller is responsible for
/// freeing the memory pointed to by `out` using `ssl.OPENSSL_free()`.
fn createExtension(host_pubkey_proto: []const u8, signature: []const u8, out: [*c][*c]u8) !c_int {
    const seq_stack = ssl.sk_ASN1_TYPE_new_null() orelse return error.CertExtCreationFailed;
    defer ssl.sk_ASN1_TYPE_free(seq_stack);

    const host_key_str = ssl.ASN1_OCTET_STRING_new() orelse return error.CertExtCreationFailed;
    if (ssl.ASN1_OCTET_STRING_set(host_key_str, host_pubkey_proto.ptr, @intCast(host_pubkey_proto.len)) == 0) {
        ssl.ASN1_OCTET_STRING_free(host_key_str);
        return error.CertExtSetFailed;
    }

    const host_key_type = ssl.ASN1_TYPE_new() orelse {
        ssl.ASN1_OCTET_STRING_free(host_key_str);
        return error.CertExtCreationFailed;
    };
    ssl.ASN1_TYPE_set(host_key_type, ssl.V_ASN1_OCTET_STRING, host_key_str);

    if (ssl.sk_ASN1_TYPE_push(seq_stack, host_key_type) <= 0) {
        ssl.ASN1_TYPE_free(host_key_type);
        return error.CertExtSetFailed;
    }

    const sig_str = ssl.ASN1_OCTET_STRING_new() orelse return error.CertExtCreationFailed;
    if (ssl.ASN1_OCTET_STRING_set(sig_str, signature.ptr, @intCast(signature.len)) == 0) {
        ssl.ASN1_OCTET_STRING_free(sig_str);
        return error.CertExtSetFailed;
    }

    const sig_type = ssl.ASN1_TYPE_new() orelse {
        ssl.ASN1_OCTET_STRING_free(sig_str);
        return error.CertExtCreationFailed;
    };
    ssl.ASN1_TYPE_set(sig_type, ssl.V_ASN1_OCTET_STRING, sig_str);

    if (ssl.sk_ASN1_TYPE_push(seq_stack, sig_type) <= 0) {
        ssl.ASN1_TYPE_free(sig_type);
        return error.CertExtSetFailed;
    }

    const len = ssl.i2d_ASN1_SEQUENCE_ANY(seq_stack, out);
    if (len <= 0) return error.CertExtCreationFailed;

    return len;
}

/// Adds a DER-encoded extension to a certificate.
fn addExtension(cert: *ssl.X509, oid_str: [*:0]const u8, is_critical: bool, der_data: []const u8) !void {
    const obj = ssl.OBJ_txt2obj(oid_str, 1) orelse return error.InvalidOID;
    defer ssl.ASN1_OBJECT_free(obj);

    const octet_string = ssl.ASN1_OCTET_STRING_new() orelse return error.CertExtCreationFailed;
    defer ssl.ASN1_OCTET_STRING_free(octet_string);
    if (ssl.ASN1_OCTET_STRING_set(octet_string, der_data.ptr, @intCast(der_data.len)) == 0) return error.CertExtSetFailed;

    const ext = ssl.X509_EXTENSION_create_by_OBJ(null, obj, if (is_critical) 1 else 0, octet_string) orelse return error.CertExtCreationFailed;
    defer ssl.X509_EXTENSION_free(ext);

    if (ssl.X509_add_ext(cert, ext, -1) <= 0) return error.CertExtSetFailed;
}

/// Converts an X509 certificate to PEM format using the provided allocator.
/// The caller owns the returned slice and must free it.
/// Returns error.OpenSSLFailed on failure.
fn x509ToPem(allocator: Allocator, cert: *ssl.X509) ![]u8 {
    const bio = ssl.BIO_new(ssl.BIO_s_mem()) orelse return error.OpenSSLFailed;
    defer _ = ssl.BIO_free(bio);

    if (ssl.PEM_write_bio_X509(bio, cert) <= 0) {
        return error.OpenSSLFailed;
    }

    var data_ptr: [*c]u8 = undefined;
    const len = ssl.BIO_get_mem_data(bio, &data_ptr);
    if (len <= 0) {
        return error.OpenSSLFailed;
    }

    return allocator.dupe(u8, data_ptr[0..@intCast(len)]);
}

pub fn alpnSelectCallbackfn(ssl_handle: ?*ssl.SSL, out: [*c][*c]const u8, out_len: [*c]u8, in_protos: [*c]const u8, in_len: c_uint, _: ?*anyopaque) callconv(.c) c_int {
    _ = ssl_handle;

    const mutable_out: [*c][*c]u8 = @ptrCast(out);

    const result: c_int = ssl.SSL_select_next_proto(
        mutable_out,
        out_len,
        ALPN_PROTOS.ptr,
        @intCast(ALPN_PROTOS.len),
        in_protos,
        in_len,
    );

    if (result == ssl.OPENSSL_NPN_NEGOTIATED) {
        return ssl.SSL_TLSEXT_ERR_OK;
    } else {
        return ssl.SSL_TLSEXT_ERR_ALERT_FATAL;
    }
}

pub fn libp2pVerifyCallback(_: c_int, cert_ctx: ?*ssl.X509_STORE_CTX) callconv(.c) c_int {
    std.debug.assert(cert_ctx != null);

    const err = ssl.X509_STORE_CTX_get_error(cert_ctx);
    const err_depth = ssl.X509_STORE_CTX_get_error_depth(cert_ctx);

    const cert = ssl.X509_STORE_CTX_get_current_cert(cert_ctx);
    if (cert == null) {
        std.log.warn("No certificate found in verification context", .{});
        return 0;
    }

    var subject_name: [256]u8 = std.mem.zeroes([256]u8);
    const subject_name_ptr = ssl.X509_get_subject_name(cert);
    if (subject_name_ptr != null) {
        _ = ssl.X509_NAME_oneline(subject_name_ptr, &subject_name, subject_name.len);
    }

    var res: c_int = 0;
    if (err == ssl.X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN or
        err == ssl.X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT)
    {
        res = 1;
        std.log.debug("Certificate verify callback: subject={s}, error={s} ({}), depth={}, status=ACCEPTED (self-signed)", .{
            std.mem.sliceTo(&subject_name, 0),
            x509ErrorToStr(err),
            err,
            err_depth,
        });
    } else if (err == ssl.X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION) {
        const is_valid = checkCriticalExtensions(cert.?) catch false;
        res = if (is_valid) 1 else 0;

        const status_str = if (is_valid) "ACCEPTED (libp2p extension)" else "REJECTED (unknown critical extension)";

        if (is_valid) {
            std.log.debug("Certificate verify callback: subject={s}, error={s} ({}), depth={}, status={s}", .{
                std.mem.sliceTo(&subject_name, 0),
                x509ErrorToStr(err),
                err,
                err_depth,
                status_str,
            });
        } else {
            std.log.warn("Certificate verify callback: subject={s}, error={s} ({}), depth={}, status={s}", .{
                std.mem.sliceTo(&subject_name, 0),
                x509ErrorToStr(err),
                err,
                err_depth,
                status_str,
            });
        }
    } else {
        res = 0;
        std.log.warn("Certificate verify callback: subject={s}, error={s} ({}), depth={}, status=REJECTED", .{
            std.mem.sliceTo(&subject_name, 0),
            x509ErrorToStr(err),
            err,
            err_depth,
        });
    }

    return res;
}

fn x509ErrorToStr(error_code: c_int) []const u8 {
    return switch (error_code) {
        ssl.X509_V_OK => "ok",
        ssl.X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT => "unable to get issuer certificate",
        ssl.X509_V_ERR_UNABLE_TO_GET_CRL => "unable to get certificate CRL",
        ssl.X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE => "unable to decrypt certificate's signature",
        ssl.X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE => "unable to decrypt CRL's signature",
        ssl.X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY => "unable to decode issuer public key",
        ssl.X509_V_ERR_CERT_SIGNATURE_FAILURE => "certificate signature failure",
        ssl.X509_V_ERR_CRL_SIGNATURE_FAILURE => "CRL signature failure",
        ssl.X509_V_ERR_CERT_NOT_YET_VALID => "certificate is not yet valid",
        ssl.X509_V_ERR_CERT_HAS_EXPIRED => "certificate has expired",
        ssl.X509_V_ERR_CRL_NOT_YET_VALID => "CRL is not yet valid",
        ssl.X509_V_ERR_CRL_HAS_EXPIRED => "CRL has expired",
        ssl.X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD => "format error in certificate's notBefore field",
        ssl.X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD => "format error in certificate's notAfter field",
        ssl.X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD => "format error in CRL's lastUpdate field",
        ssl.X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD => "format error in CRL's nextUpdate field",
        ssl.X509_V_ERR_OUT_OF_MEM => "out of memory",
        ssl.X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT => "self signed certificate",
        ssl.X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN => "self signed certificate in certificate chain",
        ssl.X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY => "unable to get local issuer certificate",
        ssl.X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE => "unable to verify the first certificate",
        ssl.X509_V_ERR_CERT_CHAIN_TOO_LONG => "certificate chain too long",
        ssl.X509_V_ERR_CERT_REVOKED => "certificate revoked",
        ssl.X509_V_ERR_INVALID_CA => "invalid CA certificate",
        ssl.X509_V_ERR_PATH_LENGTH_EXCEEDED => "path length constraint exceeded",
        ssl.X509_V_ERR_INVALID_PURPOSE => "unsupported certificate purpose",
        ssl.X509_V_ERR_CERT_UNTRUSTED => "certificate not trusted",
        ssl.X509_V_ERR_CERT_REJECTED => "certificate rejected",
        ssl.X509_V_ERR_SUBJECT_ISSUER_MISMATCH => "subject issuer mismatch",
        ssl.X509_V_ERR_AKID_SKID_MISMATCH => "authority and subject key identifier mismatch",
        ssl.X509_V_ERR_AKID_ISSUER_SERIAL_MISMATCH => "authority and issuer serial number mismatch",
        ssl.X509_V_ERR_KEYUSAGE_NO_CERTSIGN => "key usage does not include certificate signing",
        ssl.X509_V_ERR_UNABLE_TO_GET_CRL_ISSUER => "unable to get CRL issuer certificate",
        ssl.X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION => "unhandled critical extension",
        ssl.X509_V_ERR_KEYUSAGE_NO_CRL_SIGN => "key usage does not include CRL signing",
        ssl.X509_V_ERR_UNHANDLED_CRITICAL_CRL_EXTENSION => "unhandled critical CRL extension",
        ssl.X509_V_ERR_INVALID_NON_CA => "invalid non-CA certificate (has CA markings)",
        ssl.X509_V_ERR_PROXY_PATH_LENGTH_EXCEEDED => "proxy path length constraint exceeded",
        ssl.X509_V_ERR_KEYUSAGE_NO_DIGITAL_SIGNATURE => "key usage does not include digital signature",
        ssl.X509_V_ERR_PROXY_CERTIFICATES_NOT_ALLOWED => "proxy certificates not allowed, please set the appropriate flag",
        ssl.X509_V_ERR_INVALID_EXTENSION => "invalid or inconsistent certificate extension",
        ssl.X509_V_ERR_INVALID_POLICY_EXTENSION => "invalid or inconsistent certificate policy extension",
        ssl.X509_V_ERR_NO_EXPLICIT_POLICY => "no explicit policy",
        ssl.X509_V_ERR_DIFFERENT_CRL_SCOPE => "different CRL scope",
        ssl.X509_V_ERR_UNSUPPORTED_EXTENSION_FEATURE => "unsupported extension feature",
        ssl.X509_V_ERR_UNNESTED_RESOURCE => "RFC 3779 resource not subset of parent's resources",
        ssl.X509_V_ERR_PERMITTED_VIOLATION => "permitted subtree violation",
        ssl.X509_V_ERR_EXCLUDED_VIOLATION => "excluded subtree violation",
        ssl.X509_V_ERR_SUBTREE_MINMAX => "name constraints minimum and maximum not supported",
        ssl.X509_V_ERR_APPLICATION_VERIFICATION => "application verification failure",
        ssl.X509_V_ERR_UNSUPPORTED_CONSTRAINT_TYPE => "unsupported name constraint type",
        ssl.X509_V_ERR_UNSUPPORTED_CONSTRAINT_SYNTAX => "unsupported or invalid name constraint syntax",
        ssl.X509_V_ERR_UNSUPPORTED_NAME_SYNTAX => "unsupported or invalid name syntax",
        ssl.X509_V_ERR_CRL_PATH_VALIDATION_ERROR => "CRL path validation error",
        else => "unknown error",
    };
}

fn checkCriticalExtensions(cert: *ssl.X509) !bool {
    var seen_libp2p_ext = false;
    const ext_count = ssl.X509_get_ext_count(cert);

    const libp2p_oid = ssl.OBJ_txt2obj(Libp2pExtensionOid, 1) orelse {
        return error.InvalidOID;
    };
    defer ssl.ASN1_OBJECT_free(libp2p_oid);

    var i: c_int = 0;
    while (i < ext_count) : (i += 1) {
        const ext = ssl.X509_get_ext(cert, i) orelse continue;

        if (ssl.X509_EXTENSION_get_critical(ext) == 0) {
            continue;
        }

        if (ssl.X509_supported_extension(ext) != 0) {
            continue;
        }

        if (seen_libp2p_ext) {
            std.log.warn("Found unknown critical extension after libp2p extension", .{});
            return false;
        }

        const ext_obj = ssl.X509_EXTENSION_get_object(ext) orelse {
            std.log.warn("Failed to get extension object", .{});
            return false;
        };

        if (ssl.OBJ_cmp(ext_obj, libp2p_oid) == 0) {
            seen_libp2p_ext = true;
            continue;
        }

        std.log.warn("Found unsupported critical extension", .{});
        return false;
    }

    return true;
}

test "Build certificate using Ed25519 keys" {
    const host_key = try generateKeyPair(.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    const subject_key = try generateKeyPair(.ED25519);
    defer ssl.EVP_PKEY_free(subject_key);

    var host_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(host_pubkey.data.?);

    const cert = try buildCert(
        std.testing.allocator,
        &host_pubkey,
        @as(?*anyopaque, @ptrCast(host_key)),
        signDataWithTlsKey,
        subject_key,
    );
    defer ssl.X509_free(cert);

    const pem_buf = try x509ToPem(std.testing.allocator, cert);
    defer std.testing.allocator.free(pem_buf);
    try std.testing.expect(pem_buf.len > 0);
}

test "Verify certificate with Ed25519 keys" {
    const host_key = try generateKeyPair(.ED25519);
    defer ssl.EVP_PKEY_free(host_key);

    const subject_key = try generateKeyPair(.ED25519);
    defer ssl.EVP_PKEY_free(subject_key);

    var host_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(host_pubkey.data.?);

    const cert = try buildCert(
        std.testing.allocator,
        &host_pubkey,
        @as(?*anyopaque, @ptrCast(host_key)),
        signDataWithTlsKey,
        subject_key,
    );
    defer ssl.X509_free(cert);

    const peer_info = try verifyAndExtractPeerInfo(std.testing.allocator, cert);
    std.testing.allocator.free(peer_info.host_pubkey.data.?);

    try std.testing.expect(peer_info.is_valid);
    try std.testing.expect(peer_info.host_pubkey.type == .ED25519);

    var expected_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(expected_pubkey.data.?);
    const expected_peer_id = try PeerId.fromPublicKey(std.testing.allocator, &expected_pubkey);
    try std.testing.expect(peer_info.peer_id.eql(&expected_peer_id));
}

test "Build certificate using ECDSA keys" {
    const host_key = try generateKeyPair(.ECDSA);
    defer ssl.EVP_PKEY_free(host_key);

    const subject_key = try generateKeyPair(.ECDSA);
    defer ssl.EVP_PKEY_free(subject_key);

    var host_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(host_pubkey.data.?);

    const cert = try buildCert(
        std.testing.allocator,
        &host_pubkey,
        @as(?*anyopaque, @ptrCast(host_key)),
        signDataWithTlsKey,
        subject_key,
    );
    defer ssl.X509_free(cert);

    const pem_buf = try x509ToPem(std.testing.allocator, cert);
    defer std.testing.allocator.free(pem_buf);

    try std.testing.expect(pem_buf.len > 0);
}

test "Verify certificate with ECDSA keys" {
    const host_key = try generateKeyPair(.ECDSA);
    defer ssl.EVP_PKEY_free(host_key);

    const subject_key = try generateKeyPair(.ECDSA);
    defer ssl.EVP_PKEY_free(subject_key);

    var host_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(host_pubkey.data.?);

    const cert = try buildCert(
        std.testing.allocator,
        &host_pubkey,
        @as(?*anyopaque, @ptrCast(host_key)),
        signDataWithTlsKey,
        subject_key,
    );
    defer ssl.X509_free(cert);

    const peer_info = try verifyAndExtractPeerInfo(std.testing.allocator, cert);
    std.testing.allocator.free(peer_info.host_pubkey.data.?);

    try std.testing.expect(peer_info.is_valid);
    try std.testing.expect(peer_info.host_pubkey.type == .ECDSA);

    var expected_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(expected_pubkey.data.?);
    const expected_peer_id = try PeerId.fromPublicKey(std.testing.allocator, &expected_pubkey);
    try std.testing.expect(peer_info.peer_id.eql(&expected_peer_id));
}

test "Build certificate using RSA keys" {
    const host_key = try generateKeyPair(.RSA);
    defer ssl.EVP_PKEY_free(host_key);

    const subject_key = try generateKeyPair(.RSA);
    defer ssl.EVP_PKEY_free(subject_key);

    var host_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(host_pubkey.data.?);

    const cert = try buildCert(
        std.testing.allocator,
        &host_pubkey,
        @as(?*anyopaque, @ptrCast(host_key)),
        signDataWithTlsKey,
        subject_key,
    );
    defer ssl.X509_free(cert);

    const pem_buf = try x509ToPem(std.testing.allocator, cert);
    defer std.testing.allocator.free(pem_buf);

    try std.testing.expect(pem_buf.len > 0);
}

test "Verify certificate with RSA keys" {
    const host_key = try generateKeyPair(.RSA);
    defer ssl.EVP_PKEY_free(host_key);

    const subject_key = try generateKeyPair(.RSA);
    defer ssl.EVP_PKEY_free(subject_key);

    var host_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(host_pubkey.data.?);

    const cert = try buildCert(
        std.testing.allocator,
        &host_pubkey,
        @as(?*anyopaque, @ptrCast(host_key)),
        signDataWithTlsKey,
        subject_key,
    );
    defer ssl.X509_free(cert);

    const peer_info = try verifyAndExtractPeerInfo(std.testing.allocator, cert);
    std.testing.allocator.free(peer_info.host_pubkey.data.?);

    try std.testing.expect(peer_info.is_valid);
    try std.testing.expect(peer_info.host_pubkey.type == .RSA);

    var expected_pubkey = try createProtobufEncodedPublicKey(std.testing.allocator, host_key);
    defer std.testing.allocator.free(expected_pubkey.data.?);
    const expected_peer_id = try PeerId.fromPublicKey(std.testing.allocator, &expected_pubkey);
    try std.testing.expect(peer_info.peer_id.eql(&expected_peer_id));
}

test "verifySecp256k1Signature round-trips and rejects a tampered payload" {
    // Exercises verifySecp256k1Signature on its own: the cert plumbing
    // (buildCert/verifyAndExtractPeerInfo) only handles BoringSSL key types.
    const ctx = secp_context.get();
    const sk = secp.SecretKey.generate();
    const pk = secp.PublicKey.fromSecretKey(ctx.*, sk);
    const pubkey_bytes = pk.serialize(); // 33-byte compressed point

    const data = "libp2p-tls-handshake:dummy-spki-der";
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(data);
    const digest = hasher.finalResult();
    const message = secp.Message{ .inner = digest };
    const sig = ctx.signEcdsa(&message, &sk);
    const der = sig.serializeDer();

    // Correct signature over the correct payload verifies.
    try std.testing.expect(try verifySecp256k1Signature(&pubkey_bytes, data, der.data[0..der.len]));
    // Same signature over a different payload must be rejected (not errored).
    try std.testing.expect(!try verifySecp256k1Signature(&pubkey_bytes, "libp2p-tls-handshake:other", der.data[0..der.len]));
}
