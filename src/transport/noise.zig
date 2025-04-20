const std = @import("std");
const p2p_conn = @import("../conn.zig");
const noiz = @import("noiz");
const noiz_cipher = noiz.cipher;

pub const NoiseConn = struct {
    /// Underlying plaintext connection
    conn: p2p_conn.AnyConn,
    /// Cipher for encrypting outgoing messages
    sendCipher: noiz_cipher.CipherState,
    /// Cipher for decrypting incoming messages
    recvCipher: noiz_cipher.CipherState,

    /// Perform the Noise handshake over the given plaintext AnyConn
    pub fn init(
        allocator: std.mem.Allocator,
        plain: p2p_conn.AnyConn,
        isInitiator: bool,
    ) !NoiseConn {
        // Initialize handshake state
        var hs = try noiz.handshake_state.HandshakeState.initName("Noise_Xpsk1_25519_ChaChaPoly_BLAKE2s", allocator, if (isInitiator) .Initiator else .Responder, &[_]u8{}, null, .{
            .s = try noiz.DH.KeyPair.generate(null),
            .rs = null,
        });
        defer hs.deinit();

        // Buffer for handshake messages
        var buf = try std.ArrayList(u8).initCapacity(allocator, noiz.MAX_MESSAGE_LEN);
        defer buf.deinit();

        // For storing the cipher states once handshake is complete
        var sendCipher: noiz_cipher.CipherState = undefined;
        var recvCipher: noiz_cipher.CipherState = undefined;

        if (isInitiator) {
            if (try hs.writeMessage(&[_]u8{}, &buf)) |ciphers| {
                if (isInitiator) {
                    sendCipher = ciphers[0];
                    recvCipher = ciphers[1];
                } else {
                    sendCipher = ciphers[1];
                    recvCipher = ciphers[0];
                }
            }
            _ = try plain.writer().writeAll(buf.items);
            buf.clearRetainingCapacity();
        }

        // <- Read/respond
        var inbuf = try allocator.alloc(u8, noiz.MAX_MESSAGE_LEN);
        defer allocator.free(inbuf);

        const plain_reader = plain.reader();
        const n = try plain_reader.read(inbuf);

        // Process received message
        if (try hs.readMessage(inbuf[0..n], &buf)) |ciphers| {
            if (isInitiator) {
                sendCipher = ciphers[0];
                recvCipher = ciphers[1];
            } else {
                sendCipher = ciphers[1];
                recvCipher = ciphers[0];
            }
        }
        buf.clearRetainingCapacity();

        if (!isInitiator) {
            // -> Responder sends second message
            if (try hs.writeMessage(&[_]u8{}, &buf)) |ciphers| {
                if (isInitiator) {
                    sendCipher = ciphers[0];
                    recvCipher = ciphers[1];
                } else {
                    sendCipher = ciphers[1];
                    recvCipher = ciphers[0];
                }
            }
            _ = try plain.writer().writeAll(buf.items);
        }

        return NoiseConn{
            .conn = plain,
            .sendCipher = sendCipher,
            .recvCipher = recvCipher,
        };
    }

    /// Read a ciphertext frame, decrypt it, and return plaintext length
    pub fn read(self: NoiseConn, buf: []u8) anyerror!usize {
        // Read ciphertext into buf
        const n = try self.conn.reader().read(buf);
        // Decrypt
        const clear = try self.recvCipher.decryptWithAd(buf[0..n], &[_]u8{}, buf[0..n]);
        // Copy decrypted data back
        std.mem.copy(u8, buf[0..clear.len], clear);
        return clear.len;
    }

    /// Encrypt plaintext and write as ciphertext
    pub fn write(self: NoiseConn, buf: []const u8) anyerror!usize {
        // Allocate temp buffer for ciphertext
        var ct: [noiz.MAX_MESSAGE_LEN]u8 = undefined;
        const slice = try self.sendCipher.encryptWithAd(ct[0..], &[_]u8{}, buf);
        return self.conn.writer().write(slice);
    }

    /// Close the underlying connection
    pub fn close(self: NoiseConn) void {
        self.conn.close();
    }

    /// Support GenericReader
    pub inline fn reader(self: *NoiseConn) std.io.GenericReader(
        *NoiseConn,
        anyerror,
        NoiseConn.read,
    ) {
        return .{ .context = self };
    }

    /// Support GenericWriter
    pub inline fn writer(self: *NoiseConn) std.io.GenericWriter(
        *NoiseConn,
        anyerror,
        NoiseConn.write,
    ) {
        return .{ .context = self };
    }
};

/// Expose as a GenericConn implementation
pub const Connection = p2p_conn.GenericConn(
    NoiseConn,
    anyerror,
    anyerror,
    NoiseConn.read,
    NoiseConn.write,
    NoiseConn.close,
);

/// Factory to upgrade a plaintext AnyConn into a Noise-secured Connection
pub fn toConn(
    allocator: std.mem.Allocator,
    plain: p2p_conn.AnyConn,
    isInitiator: bool,
) !Connection {
    const noise = try NoiseConn.init(allocator, plain, isInitiator);
    return Connection{ .context = noise };
}
