const std = @import("std");
const ssl = @import("ssl");
const Allocator = std.mem.Allocator;
const keys_proto = @import("../proto/keys.proto.zig");
const keys = @import("peer_id").keys;

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
};

pub const ExtensionData = struct {
    host_pubkey: []u8,
    signature: []u8,
};

/// Generates a new key pair based on the specified key type.
/// This is a helper function to encapsulate the complexity of key generation using OpenSSL.
/// Note: SECP256K1 is not supported and will result in an `Error.UnsupportedKeyType`.
pub fn generateKeyPair(cert_key_type: keys_proto.KeyType) !*ssl.EVP_PKEY {
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

/// Builds a self-signed X.509 certificate suitable for libp2p's TLS handshake,
/// The caller owns the returned certificate and must free it with ssl.X509.free().
///
/// `hostKey` param represents host's key pair. Its private key signs the extension,
/// and its public key is embedded in the extension.
/// `subjectKey` param represents the subject's key pair. Its public key is the certificate's
/// main public key, and its private key signs the certificate.
///
/// The returned certificate is owned by the caller and must be freed with `ssl.X509_free()`.
pub fn buildCert(
    allocator: Allocator,
    hostKey: *ssl.EVP_PKEY,
    subjectKey: *ssl.EVP_PKEY,
) !*ssl.X509 {
    const cert = ssl.X509_new() orelse return error.CertCreationFailed;
    errdefer ssl.X509_free(cert);

    if (ssl.X509_set_version(cert, ssl.X509_VERSION_3) <= 0) return error.CertVersionSetFailed;

    const serial = ssl.ASN1_INTEGER_new() orelse return error.CertSerialCreationFailed;
    defer ssl.ASN1_INTEGER_free(serial);

    var random_bytes_buf: [8]u8 = undefined;
    std.crypto.random.bytes(&random_bytes_buf);
    const random_serial: i64 = @bitCast(random_bytes_buf);

    if (ssl.ASN1_INTEGER_set_int64(serial, random_serial) <= 0) return error.CertSerialSetFailed;
    if (ssl.X509_set_serialNumber(cert, serial) <= 0) return error.CertSerialSetFailed;

    if (ssl.X509_set_pubkey(cert, subjectKey) <= 0) return error.CertPKeySetFailed;

    const name = ssl.X509_NAME_new() orelse return error.CertNameCreationFailed;
    defer ssl.X509_NAME_free(name);
    if (ssl.X509_NAME_add_entry_by_txt(name, "C", ssl.MBSTRING_ASC, "CN", -1, -1, 0) <= 0) return error.CertNameCreationFailed;
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

    const signature = try signData(allocator, hostKey, data_to_sign);
    defer allocator.free(signature);

    const host_pubkey_proto = try createProtobufEncodedPublicKeyBuf(allocator, hostKey);
    defer allocator.free(host_pubkey_proto);

    var ext_value_der: [*c]u8 = null;
    const ext_value_der_len = try createExtension(host_pubkey_proto, signature, &ext_value_der);
    defer ssl.OPENSSL_free(ext_value_der);

    try addExtension(cert, Libp2pExtensionOid, true, ext_value_der[0..@intCast(ext_value_der_len)]);

    const message_digest: ?*const ssl.EVP_MD = switch (ssl.EVP_PKEY_base_id(subjectKey)) {
        ssl.EVP_PKEY_ED25519 => null,

        ssl.EVP_PKEY_EC, ssl.EVP_PKEY_RSA => ssl.EVP_sha256(),

        else => return error.UnsupportedKeyType,
    };

    if (ssl.X509_sign(cert, subjectKey, message_digest) <= 0) {
        return error.SignCertFailed;
    }

    return cert;
}

/// Encodes a public key into the libp2p PublicKey protobuf format.
/// The caller owns the returned slice.
pub fn createProtobufEncodedPublicKeyBuf(allocator: Allocator, pkey: *ssl.EVP_PKEY) ![]const u8 {
    var public_key_proto = try createProtobufEncodedPublicKey(allocator, pkey);
    defer allocator.free(public_key_proto.data.?);

    const proto_bytes = try public_key_proto.encode(allocator);
    return proto_bytes;
}

/// Encodes a public key into the libp2p PublicKey protobuf format.
/// The caller owns the returned PublicKey struct.
/// This function is a convenience wrapper around `createProtobufEncodedPublicKeyBuf`.
/// It returns a `keys_proto.PublicKey` struct instead of a raw byte slice.
pub fn createProtobufEncodedPublicKey(allocator: Allocator, pkey: *ssl.EVP_PKEY) !keys_proto.PublicKey {
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
                // TODO: BoringSSL does not support SECP256K1
                ssl.NID_secp256k1 => return error.UnsupportedKeyType,
                ssl.NID_X9_62_prime256v1 => break :blk 3,
                else => return error.UnsupportedKeyType,
            }
        }
        return error.UnsupportedKeyType;
    };

    const public_key_proto = keys_proto.PublicKey{
        .type = @enumFromInt(key_type_enum),
        .data = raw_pubkey,
    };

    return public_key_proto;
}

/// Encodes a public key into the libp2p PublicKey protobuf format.
/// The caller owns the returned PublicKey struct.
/// This function is a convenience wrapper around `createProtobufEncodedPublicKeyBuf`.
/// It returns a `keys.PublicKey` struct instead of a raw byte slice.
/// This is useful for compatibility with the `keys` module.
// TODO: peer-id migrated to a separate module, will need to update this function
pub fn createProtobufEncodedPublicKey1(allocator: Allocator, pkey: *ssl.EVP_PKEY) !keys.PublicKey {
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
                // TODO: BoringSSL does not support SECP256K1
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
fn getRawPublicKeyBytes(allocator: Allocator, pkey: *ssl.EVP_PKEY) ![]u8 {
    var len: usize = 0;
    if (ssl.EVP_PKEY_get_raw_public_key(pkey, null, &len) == 0) return error.RawPubKeyGetFailed;
    const buf = try allocator.alloc(u8, len);
    if (ssl.EVP_PKEY_get_raw_public_key(pkey, buf.ptr, &len) == 0) {
        allocator.free(buf);
        return error.RawPubKeyGetFailed;
    }
    return buf;
}

/// Signs arbitrary data using the private key within an EVP_PKEY.
/// The caller owns the returned slice.
fn signData(allocator: Allocator, pkey: *ssl.EVP_PKEY, data: []const u8) ![]u8 {
    const ctx = ssl.EVP_MD_CTX_new() orelse return error.CertSignCreationFailed;
    defer ssl.EVP_MD_CTX_free(ctx);

    const message_digest: ?*const ssl.EVP_MD = switch (ssl.EVP_PKEY_base_id(pkey)) {
        ssl.EVP_PKEY_ED25519 => null,

        ssl.EVP_PKEY_EC, ssl.EVP_PKEY_RSA => ssl.EVP_sha256(),

        else => return error.UnsupportedKeyType,
    };

    if (ssl.EVP_DigestSignInit(ctx, null, message_digest, null, pkey) <= 0) {
        return error.CertSignCreationFailed;
    }

    var sig_len: usize = 0;
    if (ssl.EVP_DigestSign(ctx, null, &sig_len, data.ptr, data.len) <= 0) {
        return error.CertSignCreationFailed;
    }

    const sig_buf = try allocator.alloc(u8, sig_len);
    errdefer allocator.free(sig_buf);

    if (ssl.EVP_DigestSign(ctx, sig_buf.ptr, &sig_len, data.ptr, data.len) <= 0) {
        return error.CertSignCreationFailed;
    }
    return sig_buf;
}

// fn extractPeerInfo(allocator: Allocator, cert: *const ssl.X509) !struct {host_pubkey: keys.PublicKey,peer_id:PeerId,signature:[]u8} {
//     const subject = ssl.X509_get_subject_name(cert);
//     if (subject == null) return error.OpenSSLFailed;

//     const common_name = ssl.X509_NAME_get_text_by_NID(subject, ssl.NID_commonName);
//     if (common_name == null) return error.OpenSSLFailed;

//     return PeerInfo{
//         .common_name = try allocator.dupe(u8, common_name),
//     };
// }

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

/// Verifies the libp2p extension signature against the certificate's public key.
/// This validates that the signature in the extension was created by the host key.
pub fn verifyExtensionSignature(allocator: Allocator, cert: *const ssl.X509, host_pkey: *ssl.EVP_PKEY) !bool {
    // Extract the extension data
    const ext_data = extractExtensionFields(allocator, cert) catch |err| {
        std.log.warn("Failed to extract extension data: {}", .{err});
        return false;
    };
    defer allocator.free(ext_data.host_pubkey);
    defer allocator.free(ext_data.signature);

    const cert_pkey = ssl.X509_get_pubkey(cert);
    if (cert_pkey == null) return false;
    defer ssl.EVP_PKEY_free(cert_pkey);

    var cert_pubkey_ptr: [*c]u8 = null;
    const cert_pubkey_len = ssl.i2d_PUBKEY(cert_pkey, &cert_pubkey_ptr);
    if (cert_pubkey_len <= 0) return false;
    defer ssl.OPENSSL_free(cert_pubkey_ptr);
    const cert_pubkey_der = cert_pubkey_ptr[0..@intCast(cert_pubkey_len)];

    const data_to_verify = try allocator.alloc(u8, CertificatePrefix.len + cert_pubkey_der.len);
    defer allocator.free(data_to_verify);
    @memcpy(data_to_verify[0..CertificatePrefix.len], CertificatePrefix);
    @memcpy(data_to_verify[CertificatePrefix.len..], cert_pubkey_der);

    // Verify the signature
    return verifySignature(host_pkey, data_to_verify, ext_data.signature);
}

/// Verifies a signature using the provided public key.
fn verifySignature(pkey: *ssl.EVP_PKEY, data: []const u8, signature: []const u8) bool {
    const ctx = ssl.EVP_MD_CTX_new() orelse return false;
    defer ssl.EVP_MD_CTX_free(ctx);

    const message_digest: ?*const ssl.EVP_MD = switch (ssl.EVP_PKEY_base_id(pkey)) {
        ssl.EVP_PKEY_ED25519 => null,
        ssl.EVP_PKEY_EC, ssl.EVP_PKEY_RSA => ssl.EVP_sha256(),
        else => return false,
    };

    if (ssl.EVP_DigestVerifyInit(ctx, null, message_digest, null, pkey) <= 0) {
        return false;
    }

    const result = ssl.EVP_DigestVerify(ctx, signature.ptr, signature.len, data.ptr, data.len);
    return result == 1;
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

pub fn libp2pVerifyCallback(cert_ctx: ?*ssl.X509_STORE_CTX, ctx: ?*anyopaque) callconv(.c) c_int {
    _ = cert_ctx;
    _ = ctx;
    // TODO: Implement certificate verification logic if needed.
    return 1;
}

test "Build certificate using Ed25519 keys" {
    const fs = std.fs.cwd();
    const file_path = "test_cert.pem";

    fs.deleteFile(file_path) catch |err| {
        if (err != error.FileNotFound) {
            return err;
        }
    };

    const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
    if (ssl.EVP_PKEY_keygen_init(pctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_host_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(pctx, &maybe_host_key) == 0) {
        return error.OpenSSLFailed;
    }
    const host_key = maybe_host_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(host_key);

    const sctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;

    if (ssl.EVP_PKEY_keygen_init(sctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_subject_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(sctx, &maybe_subject_key) == 0) {
        return error.OpenSSLFailed;
    }
    const subject_key = maybe_subject_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(subject_key);

    const cert = try buildCert(std.testing.allocator, host_key, subject_key);
    defer ssl.X509_free(cert);

    // TODO: Write the certificate to a file for checking the cert file outside, will use assert once verify side is implemented.
    const file = try std.fs.cwd().createFile("test_cert.pem", .{ .truncate = true });
    defer file.close();
    const pem_buf = try x509ToPem(std.testing.allocator, cert);
    defer std.testing.allocator.free(pem_buf);
    try file.writeAll(pem_buf);
}
