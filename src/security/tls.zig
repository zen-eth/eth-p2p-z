const std = @import("std");
const ssl = @import("ssl");
const Allocator = std.mem.Allocator;
const keys_proto = @import("../proto/keys.proto.zig");

/// This is the prefix libp2p uses for signing the certificate extension.
const certificate_prefix = "libp2p-tls-handshake:";
/// This is the OID for the libp2p self-signed certificate extension.
const libp2p_extension_oid = "1.3.6.1.4.1.53594.1.1";

pub const Error = error{
    CertCreationFailed,
    CertNameCreationFailed,
    CertVersionSetFailed,
    CertSerialSetFailed,
    CertPKeySetFailed,
    CertIssuerSetFailed,
    CertSubjectSetFailed,
    CertExtSetFailed,
    PubKeyTODerFailed,
    OpenSSLFailed,
    InvalidOID,
    SignDataFailed,
    SignCertFailed,
    UnsupportedKeyType,
};

/// Builds a self-signed X.509 certificate suitable for libp2p's TLS handshake,
/// The caller owns the returned certificate and must free it with ssl.X509.free().
///
/// `hostKey` param represents host's key pair. Its private key signs the extension,
/// and its public key is embedded in the extension.
/// `subjectKey` param represents the subject's key pair. Its public key is the certificate's
/// main public key, and its private key signs the certificate.
///
/// A pointer to the newly created X509 certificate, or an error returned.
pub fn buildCert(
    allocator: Allocator,
    hostKey: *ssl.EVP_PKEY,
    subjectKey: *ssl.EVP_PKEY,
) !*ssl.X509 {
    const cert = ssl.X509_new() orelse return error.CertCreationFailed;
    errdefer ssl.X509_free(cert);

    if (ssl.X509_set_version(cert, ssl.X509_VERSION_3) <= 0) return error.CertVersionSetFailed;

    const serial = ssl.ASN1_INTEGER_new() orelse return error.OpenSSLFailed;
    defer ssl.ASN1_INTEGER_free(serial);
    if (ssl.ASN1_INTEGER_set_int64(serial, std.time.milliTimestamp()) <= 0) return error.OpenSSLFailed;
    if (ssl.X509_set_serialNumber(cert, serial) <= 0) return error.CertSerialSetFailed;

    if (ssl.X509_set_pubkey(cert, subjectKey) <= 0) return error.CertPKeySetFailed;

    const name = ssl.X509_NAME_new() orelse return error.CertNameCreationFailed;
    defer ssl.X509_NAME_free(name);
    if (ssl.X509_NAME_add_entry_by_txt(name, "C", ssl.MBSTRING_ASC, "CN", -1, -1, 0) <= 0) return error.OpenSSLFailed;
    if (ssl.X509_NAME_add_entry_by_txt(name, "O", ssl.MBSTRING_ASC, "libp2p", -1, -1, 0) <= 0) return error.OpenSSLFailed;
    if (ssl.X509_NAME_add_entry_by_txt(name, "CN", ssl.MBSTRING_ASC, "libp2p", -1, -1, 0) <= 0) return error.OpenSSLFailed;
    if (ssl.X509_set_issuer_name(cert, name) <= 0) return error.CertIssuerSetFailed;
    if (ssl.X509_set_subject_name(cert, name) <= 0) return error.CertSubjectSetFailed;

    // const now = std.time.timestamp();
    // if (ssl.X509_set_notBefore(cert, now - 3600) <= 0) return error.OpenSSLFailed;
    // if (ssl.X509_set_notAfter(cert, now + 365 * 24 * 3600) <= 0) return error.OpenSSLFailed;

    if (ssl.X509_gmtime_adj(ssl.X509_get_notBefore(cert), -3600) == null) return error.OpenSSLFailed;
    if (ssl.X509_gmtime_adj(ssl.X509_get_notAfter(cert), 365 * 24 * 3600) == null) return error.OpenSSLFailed;

    var subj_pubkey_ptr: [*c]u8 = null;
    const subj_pubkey_len = ssl.i2d_PUBKEY(subjectKey, &subj_pubkey_ptr);
    if (subj_pubkey_len <= 0) return error.PubKeyTODerFailed;
    defer ssl.OPENSSL_free(subj_pubkey_ptr);
    const subject_pubkey_der = subj_pubkey_ptr[0..@intCast(subj_pubkey_len)];

    const data_to_sign = try allocator.alloc(u8, certificate_prefix.len + subject_pubkey_der.len);
    defer allocator.free(data_to_sign);
    @memcpy(data_to_sign[0..certificate_prefix.len], certificate_prefix);
    @memcpy(data_to_sign[certificate_prefix.len..], subject_pubkey_der);

    const signature = try signData(allocator, hostKey, data_to_sign);
    defer allocator.free(signature);

    const host_pubkey_proto = try createProtobufEncodedPublicKey(allocator, hostKey);
    defer allocator.free(host_pubkey_proto);

    var ext_value_der: [*c]u8 = null;
    const ext_value_der_len = try createExtension(host_pubkey_proto, signature, &ext_value_der);
    defer ssl.OPENSSL_free(ext_value_der);

    try addExtension(cert, libp2p_extension_oid, true, ext_value_der[0..@intCast(ext_value_der_len)]);

    if (ssl.X509_sign(cert, subjectKey, null) <= 0) {
        return error.SignCertFailed;
    }

    return cert;
}

/// Encodes a public key into the libp2p PublicKey protobuf format.
fn createProtobufEncodedPublicKey(allocator: std.mem.Allocator, pkey: *ssl.EVP_PKEY) ![]const u8 {
    const raw_pubkey = try getRawPublicKeyBytes(allocator, pkey);
    defer allocator.free(raw_pubkey);

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
                ssl.NID_secp256k1 => break :blk 2,
                ssl.NID_X9_62_prime256v1 => break :blk 3,
                else => return error.UnsupportedKeyType,
            }
        }
        return error.UnsupportedKeyType;
    };

    var public_key_proto = keys_proto.PublicKey{
        .type = @enumFromInt(key_type_enum),
        .data = raw_pubkey,
    };

    const proto_bytes = try public_key_proto.encode(allocator);
    return proto_bytes;
}

/// Gets the raw public key bytes from an EVP_PKEY.
fn getRawPublicKeyBytes(allocator: std.mem.Allocator, pkey: *ssl.EVP_PKEY) ![]u8 {
    var len: usize = 0;
    if (ssl.EVP_PKEY_get_raw_public_key(pkey, null, &len) == 0) return error.OpenSSLFailed;
    const buf = try allocator.alloc(u8, len);
    if (ssl.EVP_PKEY_get_raw_public_key(pkey, buf.ptr, &len) == 0) {
        allocator.free(buf);
        return error.OpenSSLFailed;
    }
    return buf;
}

/// Signs arbitrary data using the private key within an EVP_PKEY.
fn signData(allocator: std.mem.Allocator, pkey: *ssl.EVP_PKEY, data: []const u8) ![]u8 {
    const ctx = ssl.EVP_MD_CTX_new() orelse return error.OpenSSLFailed;
    defer ssl.EVP_MD_CTX_free(ctx);

    const message_digest: ?*const ssl.EVP_MD = switch (ssl.EVP_PKEY_base_id(pkey)) {
        // TODO: Check if secp256k1 is supported by OpenSSL.
        ssl.EVP_PKEY_ED25519 => null,

        ssl.EVP_PKEY_EC, ssl.EVP_PKEY_RSA => ssl.EVP_sha256(),

        else => return error.UnsupportedKeyType,
    };

    if (ssl.EVP_DigestSignInit(ctx, null, message_digest, null, pkey) <= 0) {
        return error.OpenSSLFailed;
    }

    var sig_len: usize = 0;
    if (ssl.EVP_DigestSign(ctx, null, &sig_len, data.ptr, data.len) <= 0) {
        return error.SignFailed;
    }

    const sig_buf = try allocator.alloc(u8, sig_len);
    errdefer allocator.free(sig_buf);

    if (ssl.EVP_DigestSign(ctx, sig_buf.ptr, &sig_len, data.ptr, data.len) <= 0) {
        return error.SignFailed;
    }
    return sig_buf;
}

/// Creates the DER-encoded value for the libp2p extension.
/// The value is a SEQUENCE of two OCTET STRINGs.
fn createExtension(host_pubkey_proto: []const u8, signature: []const u8, out: [*c][*c]u8) !c_int {
    const seq_stack = ssl.sk_ASN1_TYPE_new_null() orelse return error.OpenSSLFailed;
    defer ssl.sk_ASN1_TYPE_free(seq_stack);

    const host_key_str = ssl.ASN1_OCTET_STRING_new() orelse return error.OpenSSLFailed;
    if (ssl.ASN1_OCTET_STRING_set(host_key_str, host_pubkey_proto.ptr, @intCast(host_pubkey_proto.len)) == 0) {
        ssl.ASN1_OCTET_STRING_free(host_key_str);
        return error.OpenSSLFailed;
    }

    const host_key_type = ssl.ASN1_TYPE_new() orelse {
        ssl.ASN1_OCTET_STRING_free(host_key_str);
        return error.OpenSSLFailed;
    };
    ssl.ASN1_TYPE_set(host_key_type, ssl.V_ASN1_OCTET_STRING, host_key_str);

    if (ssl.sk_ASN1_TYPE_push(seq_stack, host_key_type) <= 0) {
        ssl.ASN1_TYPE_free(host_key_type);
        return error.OpenSSLFailed;
    }

    const sig_str = ssl.ASN1_OCTET_STRING_new() orelse return error.OpenSSLFailed;
    if (ssl.ASN1_OCTET_STRING_set(sig_str, signature.ptr, @intCast(signature.len)) == 0) {
        ssl.ASN1_OCTET_STRING_free(sig_str);
        return error.OpenSSLFailed;
    }

    const sig_type = ssl.ASN1_TYPE_new() orelse {
        ssl.ASN1_OCTET_STRING_free(sig_str);
        return error.OpenSSLFailed;
    };
    ssl.ASN1_TYPE_set(sig_type, ssl.V_ASN1_OCTET_STRING, sig_str);

    if (ssl.sk_ASN1_TYPE_push(seq_stack, sig_type) <= 0) {
        ssl.ASN1_TYPE_free(sig_type);
        return error.OpenSSLFailed;
    }

    const len = ssl.i2d_ASN1_SEQUENCE_ANY(seq_stack, out);
    if (len <= 0) return error.OpenSSLFailed;

    return len;
}

/// A generic helper to add a DER-encoded extension to a certificate.
fn addExtension(cert: *ssl.X509, oid_str: [*:0]const u8, is_critical: bool, der_data: []const u8) !void {
    const obj = ssl.OBJ_txt2obj(oid_str, 1) orelse return error.InvalidOID;
    defer ssl.ASN1_OBJECT_free(obj);

    const octet_string = ssl.ASN1_OCTET_STRING_new() orelse return error.OpenSSLFailed;
    defer ssl.ASN1_OCTET_STRING_free(octet_string);
    if (ssl.ASN1_OCTET_STRING_set(octet_string, der_data.ptr, @intCast(der_data.len)) == 0) return error.OpenSSLFailed;

    const ext = ssl.X509_EXTENSION_create_by_OBJ(null, obj, if (is_critical) 1 else 0, octet_string) orelse return error.OpenSSLFailed;
    defer ssl.X509_EXTENSION_free(ext);

    if (ssl.X509_add_ext(cert, ext, -1) <= 0) return error.CertExtSetFailed;
}

/// Converts an X509 certificate to PEM format for testing.
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

test "Build certificate" {
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

    const file = try std.fs.cwd().createFile("test_cert.pem", .{ .truncate = true });
    defer file.close();
    const pem_buf = try x509ToPem(std.testing.allocator, cert);
    defer std.testing.allocator.free(pem_buf);
    try file.writeAll(pem_buf);
}
