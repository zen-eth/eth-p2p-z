const std = @import("std");
const secp = @import("secp256k1");

var global_ctx: ?secp.Secp256k1 = null;
var global_ctx_lock: std.atomic.Mutex = .unlocked;

pub fn get() *secp.Secp256k1 {
    while (!global_ctx_lock.tryLock()) std.Thread.yield() catch {};
    defer global_ctx_lock.unlock();
    if (global_ctx == null) global_ctx = secp.Secp256k1.genNew();
    return &global_ctx.?;
}

pub fn deinit() void {
    if (global_ctx) |*ctx| {
        ctx.deinit();
        global_ctx = null;
    }
}
