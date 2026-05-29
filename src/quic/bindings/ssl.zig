//! BoringSSL symbols (`ssl.c.SSL_*`), re-exported from the quiche C bindings.
//! Deliberately a separate module from quiche.zig (both resolve to the same
//! `quiche_raw` — quiche bundles BoringSSL) purely so call sites read as
//! `ssl.c.SSL_*` vs `quiche.c.quiche_*`. Do NOT collapse: it would churn every
//! `@import("ssl")` site and make `quiche.c.SSL_*` read wrong.
pub const c = @import("quiche_raw");
