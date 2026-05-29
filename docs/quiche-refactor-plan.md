# quiche QUIC — Bottom-Up Refactor & Cleanup Plan

Goal: refactor and clean the quiche QUIC stack from the lowest layer up, applying
Zig 0.16 best practices, while **preserving the high-performance multi-executor
threading/IO model verbatim**. Grounded in a per-layer code audit (2026-05-28).

This is polish + harden + clean, NOT a redesign. The model is confirmed correct;
the audit's `model_critical` findings are the contract every refactor must keep.

---

## 0. The model (must-preserve) — Invariant Ledger

The whole stack is a message-passing actor system on the zio std.Io fiber runtime,
**multi-executor by construction**. Cross-fiber comms are ONLY: lock-free
`std.Io.Queue`, atomics, and a futex wake (`Signal` = epoch + `io.futexWait/Wake`;
`WaitSet` = atomic u32 bitset + `Signal`). These invariants are load-bearing — do
not weaken orderings, make atomics non-atomic, or remove the futex/refcounts:

- **Signal (io/channel.zig):** `notify` = `epoch.fetchAdd(1,.release)` THEN `futexWake`;
  waiters `observe()` (acquire) BEFORE checking state, then `wait(observed)`. This
  observe-before-wait + epoch-before-wake is the lost-wakeup-free contract.
- **WaitSet (io/waitset.zig):** `notify` = `bits.fetchOr(.release)` THEN signal;
  `take` = `swap(0,.acq_rel)`; `wait` peeks `any()` before blocking. Keep orderings.
- **Refcounts** (SharedState=2, stream SharedState=1→2 via Record, SharedUdpSocket,
  EndpointCore, IncomingPacketChannel): `fetchAdd/Sub(.acq_rel)`, `assert(previous>0)`,
  last releaser (`previous==1`) frees. The `.acq_rel` is the happens-before edge.
- **Connection actor owns quiche_conn alone** (not thread-safe). All other fibers
  reach it via `inbox: Queue(Command)` + `notifyControlCommandsReady` (post-then-notify
  order) and stack-allocated reply slots. Never touch actor-private memory from a handle.
- **outbound_signaled coalescing** (stream/shared_state + actor): false→true CAS winner
  pushes to `outbound_pending_ids`; actor clears `store(false,.release)` BEFORE
  `markStreamOutboundReady`. Clear-before-mark is the lost-wakeup guard. Overflow →
  `fallback_full_scan` (non-fatal full sweep) — must stay non-fatal.
- **Router single-owner cid_map** (router/loop.zig): mutated only on the router fiber;
  cross-fiber callers go through `RouteRegistrar.register` → `route_commands` queue →
  synchronous `ack` handshake. cid_map holds one retain per IncomingPacketChannel.
- **Accept admission** (router/accept_queue.zig): atomic `available` counter; every
  reserve balanced by exactly one publish-or-cancel; `take()` move prevents double-release.
- **Teardown ordering** (endpoint/router): `router_future.cancel` (blocks until fiber
  gone) → `handshake_waiters.cancel` → `route_commands.close/discardQueued` (fail-acks
  parked dialers) → accept queue close → `accept_available=0` → socket release. Future
  (not Group) for the router is deliberate. `dropCommand` MUST fail-ack parked register reqs.
- **byte_queue single-producer**: `used_bytes` under `mutex`; `available()`-then-`reserve()`
  double-lock is safe ONLY because one producer per queue. Don't share a ByteQueue.
- **retry_token (router/retry_token.zig):** `std.crypto.timing_safe.eql` for the HMAC —
  must NOT become `std.mem.eql`. HMAC binds domain tag + canonical peer IP.
- **TLS callbacks (security/tls.zig):** `libp2pVerifyCallback`/`alpnSelectCallback` are
  `callconv(.c)`, invoked on any executor — must stay free of shared mutable / threadlocal
  state (threadlocal peer-cert was explicitly removed for multi-executor safety). `Context`
  (SSL_CTX) is immutable after `create()` so `newSsl` needs no lock.
- **config invariants:** `disable_active_migration` must stay true (single-owner socket
  can't rebind); all queue-length knobs validated non-zero (the cross-fiber backpressure budget).

---

## 1. Prerequisite gate — E1 (do FIRST, before touching any primitive)

All 81 current tests run on `std.Io.Threaded`. None exercise the multi-executor
scheduling that the invariants above protect. This is exactly the gap that let zio
v0.11's stubbed `batchAwaitConcurrent` reach interop. **Bottom-up refactoring of the
IO primitives without a zio-runtime, multi-executor test layer is unverifiable.**

E1 deliverable: a test harness that spins a real `zio.Runtime` with `.exact(N>=2)`
executors and exercises: Signal/WaitSet wake under cross-executor notify; channel
refcount free-on-last under concurrent retain/release; byte_queue backpressure;
actor inbox command round-trip; full loopback handshake + stream ping/pong + teardown.
Until this is green, do not start Layer 0.

### E1 STATUS — DELIVERED (2026-05-29). Two tiers:

- **Tier A — `zig build zio-io-test`** (`src/quic/zio_io_tests.zig`): Layer-0 IO
  primitives (Signal, WaitSet, Bounded channel slot + byte-backpressure, refcount)
  on a real multi-executor zio runtime with producers/consumers on different
  executors. BoringSSL-free (imports only `zio` + io primitives) → compiles in
  seconds, runs **anywhere**. **12/12 pass on macOS kqueue, ~87ms.** This is the
  net for L0 — it is green, so **L0 may begin.**
- **Tier B — `zig build zio-integ-test`** (`src/zio_integration_tests.zig`): two
  QUIC endpoints over loopback (exact(2) + exact(4)), server accept on a fiber,
  full handshake + bidirectional stream echo. Net for L2-L6.

**macOS kqueue limitation (found via E1, important):** Tier B's QUIC *data path*
(handshake + echo + per-connection teardown) works on kqueue, but
`endpoint.deinit → router.closeListener → router_future.cancel` **livelocks** on
the macOS kqueue multi-executor backend — zio does not interrupt a fiber blocked
in a socket `receive` when its Future is cancelled (staged-logging diagnosis: both
tests reach the final connection-deinit, then spin in fixture.deinit). The same
teardown runs cleanly on **Linux epoll/io_uring** (the QUIC interop suite already
exercises it). So Tier B **skips on macOS** (`requireLinux()`) and runs on
Linux/Docker/CI. Verification split: **L0/L1 locally (Tier A on kqueue); L2-L6 on
Linux (Tier B via `-Dzio-backend=epoll`).** This is a second zio kqueue-backend
upstream finding alongside E7.

NOTE: the native macOS build works (`zig build`, kqueue), so L0/L1 refactor +
Tier-A verification is a fully local, fast loop.

## 2. Settle the contract — E2 + C4 (cheap, do after E1)

"Keep the high-performance model" = keep multi-executor. So E2 resolves to making the
≥2-executor requirement explicit (assert/document at endpoint init), and C4 = name the
interop `.exact(4)` magic number with a rationale comment. No behavior change.

## 3. Bottom-up refactor sweep (layer by layer; each gated by E1 tests + interop)

Order = dependency order (leaves first). After each layer: run E1 multi-executor tests
+ `zig build` + the zig↔zig interop smoke. Preserve every Invariant-Ledger item.

### L0 — io primitives (io/*.zig)  [lowest]
Bugs/best-practice:
- **time.zig:23 (med):** `us()/ms()/s()` multiply in u64 BEFORE `@intCast` to i96 →
  overflow wraps. Widen first: `@as(i96, value) * std.time.ns_per_ms`.
- time.zig:7 monotonicNs unguarded upper `@intCast` → use `std.math.cast … orelse`.
- time.zig:13 rename misleading `receiveTimeout` → `durationNs` (alias kept).
- channel.zig:131 drop identity `catch switch` (Canceled→Canceled noise) → `try`.
- socket_control.zig: comment cmsgAlign vs kernel CMSG_ALIGN; `std.math.cast(i32,..)` for ifindex.
- transport.zig:81 doc NetworkTransport ownership (socket/core retained; route_updates borrowed).
Cleanup (C5): time.zig dead `ns/us/s` + stale comment; channel.zig Bounded/Unbounded
refcount+Signal duplication (extract shared mixin); packet_route `IncomingPacket` alias +
unused `bytes()`; transport `subStat` dead-check; waitset `Ready` reserved-bits note;
channel.zig doc the dual `ready` vs `writable` Signal split.

### L1 — bindings (bindings/*.zig)
- ssl.zig and quiche.zig are byte-identical shims. Collapse to one, or add `//!` docs
  explaining the deliberate `_compat` indirection. (Trivial.)

### L2 — stream (stream/*.zig)
- **shared_state.zig:12 (med):** module doc says refcount=2 but `init(1)`; fix doc to
  "init 1; Record.init retains the 2nd ref."
- handle.zig:232/263, record.zig:75/84 `var queue` → `const`.
- byte_queue.zig:43 document the single-producer requirement at the double-lock site.
- handle.zig:152 document `close()` is fire-and-forget vs `closeWrite()` waits-for-FIN.
- handle.zig:354 shrink `iovecs_buffer` to `[1][]u8` (only dest[0] used).
Cleanup (C5): delete dead `SharedState.outboundIdle` (+ its duplicate-with-Record);
align stale reset doc comments; note the `Impl`-suffix alias convention.

### L3 — connection (connection/*.zig)
- **actor.zig:1413 (med, crash):** guard `quiche_conn_peer_streams_left_bidi` sign
  before `@intCast` (negative → safe-build panic in the actor fiber).
- **actor.zig:1536 (scaling bug, ties E3):** `drainOutboundStreams` full-scan buffers
  finished stream IDs in `[256]u64`; >256 open streams → finished records not reaped →
  slow leak. Collect within the iterator instead of buffering IDs.
- **commands.zig:121 (med, perf):** `close_connection` inlines `[256]u8` reason →
  ~280-byte Command, copied per inbox push. Shrink reason cap or pass a borrowed slice.
- **stats.zig:210 (med):** `Publisher.load(*const)` + `@constCast` is misleading; take
  `*Publisher` (it mutates the mutex).
- stage.zig:115 `Handshake.deadline_ns` non-atomic cross-fiber (safe only by spawn
  ordering) → make atomic store(.release)/load(.acquire) or document set-once-before-spawn.
- actor.zig:1085 `currentSourceCidKey` ignores `self` → free function.
Cleanup (C5): stale comments (shared_state accepted_stream_pops; setup.zig mapRoute;
cid.zig local==max assert); dedup ActorParams/SpawnParams tuning fields.

### L4 — router (router/*.zig)
- loop.zig:222 `unreachable` on Timeout → make impossibility type-level (omit Timeout
  from the no-timeout path's error set).
- loop.zig: name magic numbers (VN `[1500]`→const; token `[256]`→`retry_token.max_token_len`).
- loop.zig:879 rxMonoNs `@intCast` → `std.math.cast … orelse 0`.
- retry_token.zig:133 name `[17]u8`/`[1..5]`/`[1..17]` (ip4/ip6 enc lens).
Cleanup: loop.zig stale "0.15.x" comment; document the drain/observe/redrain protocol
(lines 191-213); rename cid_map local `sender` → `channel`; doc controlDestination precedence.

### L5 — assembly (endpoint/, dialer/, datagram/)
- **endpoint/handle.zig:39 (fragility):** add `errdefer allocator.destroy(endpoint)` after create.
- raw.zig:40 drop identity error switch; raw.zig:58 remove `undefined` scid lifetime
  (compute `scid: []const u8` once), drop `generated_scid[0..].ptr` → `&generated_scid`.
- dialer/dial.zig:92 map handshake timeout → `error.Timeout` (don't collapse all to
  HandshakeFailed); dial.zig:113 `publicKeysEqual` treat null!=empty (gates identity).
- datagram/handle_pool.zig: `setLen`/`release` bounds asserts; document acquire-narrows-slice.
- core.zig:77 (perf, E5): per-packet `stats_lock` mutex on the hot path → atomic counters.
Cleanup: endpoint deinit "safe on unbound" doc; datagram mod.zig re-export Pool/Queue.

### L6 — cross-cutting (config.zig, address.zig, log.zig, quic.zig, security/tls.zig)
Absorbs C1, C2, C3:
- **C1:** tls.zig:1098/1216 delete debug cert-to-file dumps + stale "will use assert"
  comment (verify side IS implemented); then delete now-dead `x509ToPem` (tls.zig:861).
- **C2:** tls.zig:380 remove stale "peer-id migrated" TODO (peer_id is a module now).
- **C3 config audit:** add ≥1200 min validation for max_recv/send_udp_payload_size
  (config.zig:165); validate stateless_reset_token only server-side; fix doc typos
  (112 "handles use", 145 over-explained validateOptions); confirm every
  `quiche_config_set*` is reachable from Options + tested.
- tls.zig:269 **copy-paste slip:** country `"C"` set to value `"CN"` — confirm/fix.
- tls.zig:547 promote anonymous return to named `PeerVerification` + document
  `host_pubkey.data` ownership (caller frees, incl. is_valid==false path).
- tls.zig: pick one X509 const convention; comment the `X509_get_pubkey` `@constCast`
  (refcount-only) and the EC_KEY/ASN1 assign-consumes-on-success ownership dances.
- tls.zig: dead `createProtobufEncodedPublicKeyBuf`? verify + remove if unused.
- config.zig comment that cross-fiber queue knobs are the backpressure budget.
- quic.zig: decide whether the config Options surface belongs in the public barrel.
- log.zig:14 comment the `swap(.acq_rel)` one-time global-callback gate.

## 4. Broader enhancements (after the sweep)
- **E3** stream-handler fiber bounding/pooling + dispatcher backpressure (builds on the
  L3 drainOutboundStreams fix).
- **E4** datagram path tests + example (or gate behind an option).
- **E5** observability hooks + tuning docs (builds on the L5 core.zig atomics change).

## 5. Audits & hygiene (last)
- **E6** QUIC feature audit (migration/0-RTT/key-update/stateless-reset/secp256k1-over-TLS).
- **E7** upstream/document the zio epoll stdout-write gap.
- **C6** branch hygiene: rebase the 3 fix commits off the `wip` base before landing.

---

## Verification strategy (every layer)
1. E1 multi-executor zio tests must stay green (the real regression net).
2. `zig build` clean.
3. zig↔zig interop smoke (handshake + ping) on BOTH backends (io_uring w/ seccomp=unconfined,
   epoll under default seccomp).
4. Touch NO `model_critical` invariant without a test that pins the ordering it protects.
5. One concern per commit; preserve the Invariant Ledger verbatim.
