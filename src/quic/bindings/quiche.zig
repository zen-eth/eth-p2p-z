//! Stable local import name for the upstream quiche C bindings: `quiche.c.<sym>`.
//! No wrapping logic lives here. quiche statically links and re-exports BoringSSL,
//! so the same `quiche_raw` module also carries the SSL_* surface — that is exposed
//! under a separate namespace (`ssl.c.*`) by ssl.zig for call-site readability.
pub const c = @import("quiche_raw");
