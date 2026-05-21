const ssl = @import("ssl").c;
const cid_mod = @import("cid.zig");

pub const local_cid_len = cid_mod.local_cid_len;
pub const LocalCid = cid_mod.LocalCid;
pub const ResetToken = [16]u8;

pub const GenerateError = error{RandomFailed};

pub fn randomLocalCid() GenerateError!LocalCid {
    var cid: LocalCid = undefined;
    try randomBytes(&cid);
    return cid;
}

pub fn randomResetToken() GenerateError!ResetToken {
    var token: ResetToken = undefined;
    try randomBytes(&token);
    return token;
}

fn randomBytes(out: []u8) GenerateError!void {
    if (ssl.RAND_bytes(out.ptr, out.len) != 1) return error.RandomFailed;
}
