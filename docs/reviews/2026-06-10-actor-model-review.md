# Actor concurrency model review — QUIC + gossipsub

**Date:** 2026-06-10
**Scope:** `src/quic/**` (per-connection actor, endpoint router, IO plumbing) and
`src/protocols/pubsub/**` (gossipsub router actor, peer IO), plus the zio runtime
integration and the `switch.zig` binding.
**Method:** six independent review passes — three deep-readers (QUIC actor,
gossipsub router, end-to-end data path) followed by three adversarial critics
(concurrency correctness, performance at eth2 scale, alternative-architecture
comparison vs rust-libp2p / go-libp2p / quinn). Every finding below was verified
against the code at the cited `file:line`; findings the critics could not verify
were dropped. Line numbers refer to the tree at the time of review (branch
`fix/quiche-iouring-handshake`, post-`6ce8a5a`).

---

## Verdict

**The macro-architecture is correct and well-matched to a fiber runtime.**
A per-connection actor exclusively owning `quiche_conn` (quinn's
connection-driver model) plus a single gossipsub router fiber owning all
mesh/score/cache state (go-libp2p's `processLoop` model) is the right skeleton,
and the single-writer invariant is enforced *by construction* — handles
physically cannot reach quiche state. Two deliberate non-choices are also right:

- **Do not** move to rust-libp2p-style poll composition. Zig has no
  compiler-generated futures; hand-written resumable state machines are exactly
  the pain the fiber model eliminates (relearned previously with the lsquic
  bridge).
- **Do not** shard gossipsub state across fibers. go-libp2p proves the
  single-loop model at mainnet scale; sharding buys data races, not throughput.

**However**, the review found **three deployment-blocking correctness bugs**
(all small, local fixes) and a data plane that delivers roughly **5–10x less
than this same architecture can**, due to syscall-layer gaps (no
sendmmsg/GSO/recvmmsg), per-datagram fixed costs in the endpoint router, and
Ed25519 verification serialized — before deduplication — on the single
gossipsub router fiber.

---

## Part I — What stands (keep as-is)

These survived adversarial scrutiny; they are the parts a from-scratch redesign
would keep unchanged.

| Design | Evidence | Why it is right |
|---|---|---|
| Single-writer conn actor; handles can't reach quiche state | `actor.zig:118-175`, `handle.zig:46-50,91-103`, `shared_state.zig:13-16` | The no-locks-around-`quiche_conn` claim is auditable in minutes; invariant by construction, not convention |
| WaitSet coalesced readiness (atomic bitmask + epoch futex), level/edge split | `waitset.zig:26-91` (the prevented livelock is documented at `:63-87`) | Beats N-channels+Select per iteration; a burst of N writes wakes the actor once |
| Data off the command inbox: per-stream ByteQueue rings; commands carry stack reply slots completed on inbox close | `byte_queue.zig`, `commands.zig:37-143`, `shared_state.zig:204-220` | Zero heap per command; teardown cannot orphan a waiter |
| Explicit per-boundary backpressure policies | packets drop like loss (`packet_route.zig:170-176`); full stream ring ⇒ bytes stay in quiche ⇒ QUIC flow control (`actor.zig:1774-1796`); gossip lane full ⇒ drop one frame for that peer (`peer_io.zig:107-110`) | The hardest part of an actor data plane, done deliberately (go mostly just drops) |
| Encode-once refcounted `OutboundFrame` fan-out | `peer_io.zig:30-76`, `router.zig:1865-1932,3357-3421` | A 100 KB block to a D=8 mesh is encoded+framed once: saves ~800 KB of memcpy + 7 encodes per block; IWANT serving is retain+push (`router.zig:2597-2601`) |
| Three-lane priority per-peer queues (subscribe > control > data) + IDONTWANT purge | `peer_io.zig:12,142-161,241-261`, `router.zig:2677-2694` | GRAFT/PRUNE/IHAVE never queue behind a block backlog |
| Msg-id interning with assert-empty-at-deinit | `intern.zig:56-75` | One allocation shared across seen/mcache/dont_send/promises; leaks trip an assert |
| Comptime-generic `Router(Transport)` + in-memory FakeTransport | `router.zig:574`, tests from `router.zig:3838` | Zero dispatch cost; buys ~5.7k lines of deterministic tests incl. refcount/leak assertions |
| Persistent-stopping-signal teardown (flag + channel close + await; never one-shot cancel) | `loop.zig:187-220`, `router.zig:1129-1164` | The hard-won lesson from the QUIC teardown deadlock, applied consistently and regression-tested |
| Bounded async validation with inbox re-entry; validation context freed exactly once across all 3 ownership paths | `router.zig:3083-3092,3270-3297,3791` | go's validator-worker model done safely without GC |
| Accept-path hygiene: Retry-token validation before any allocation, 1200 B Initial gate, VN guards | `loop.zig:621-691` | Correct ordering many stacks get wrong |
| QUIC engine maturity: pacing honored (deferred packet + timer), keep-alive clamped to peer idle and re-armed on flush, per-tick budgets with carried-over ready bits | `actor.zig:563-568,705-740,789-792,859-919` | |

One stated-design discrepancy worth recording: the stack is **not** lock-free
(every channel hop is `std.Io.Mutex`-guarded; handles use a spin-lock). It *is*
single-writer/share-nothing for quiche and router state — which is the property
that matters. Docs and commit messages should say "single-writer", not
"lock-free".

---

## Part II — Critical correctness findings

### C1 — Gossipsub writer-teardown self-deadlock (whole pubsub plane wedges) — `CRITICAL`

`onWriterDisconnect` posts from the **writer fiber** via blocking
`putOneUncancelable` into the 256-slot router inbox (`router.zig:3805`,
capacity at `:381`). `teardownPeer` runs **on the router fiber** — the inbox's
only consumer — and does `writer_future.cancel` + await (`router.zig:3688-3690`).

Interleaving: sustained inbound load fills the inbox → a peer's connection dies
→ its writer exhausts reopen retries and parks **uncancelably** in the post →
the Switch's independent `peer_disconnected` for the same peer reaches the
router first → `teardownPeer` joins the parked writer → the router never drains
its own inbox again → every producer (readers, Switch callbacks, heartbeat,
even `destroy`'s post at `router.zig:1150`) wedges permanently. The shutdown
path has the same hole: `mainLoop`'s defer runs `teardownAllPeers` **before**
the inbox is closed (`router.zig:1700-1703`), so a parked writer wedges
`destroy` too.

This is precisely the one-shot-cancel-vs-uncancelable-block failure class
already root-caused and fixed in the QUIC router (`loop.zig:59-75`) —
re-derived one layer up.

**Fix:** the writer's disconnect signal must be non-blocking (tryPut + an
atomic per-peer `writer_dead` flag the router checks, or rely on the Switch's
own disconnect event), and the `mainLoop` defer order must close the inbox
(waking parked putters with `Closed`) before tearing down peers.

**Rule to codify:** *a fiber that any other fiber joins must never block
uncancelably on a resource the joiner controls.*

### C2 — `seqno` restarts at 0: network-wide self-censorship after restart — `CRITICAL`

`seqno: u64 = 0` (`router.zig:894`), incremented per publish; under
`strict_sign`/`none` the message-id is `from ++ seqno` (`router.zig:2982-2996`).
A node that restarts within its peers' 120 s seen-TTL (`router.zig:430`)
re-publishes with already-seen ids — **its messages are silently dropped by the
entire network** until the window expires. For a validator that is missed
attestations/blocks after every restart. go-libp2p seeds the counter with
wall-clock nanoseconds for exactly this reason.

**Fix:** one line — seed `seqno` from the wall clock at router init.

### C3 — Duplicate-connection close tears down the live peer — `CRITICAL`

The Switch appends every connection with no per-peer dedup (`switch.zig:324`)
and fires `on_disconnected` on **every** connection unregister, keyed only by
`PeerId` (`switch.zig:337-362`). The router dedups connects — a second
connection to a tracked peer is ignored (`router.zig:1731-1733`) — but
`onPeerDisconnected` unconditionally `fetchRemove`s and tears down
(`router.zig:1985`). In the simultaneous-dial case (both sides dial each other —
routine during mesh formation), closing the redundant connection destroys the
surviving peer's mesh/writer/score state while its real connection is healthy.
Conversely the retained `PeerState` keeps writing to the *first* connection's
sink even when that one is the dead one.

**Fix:** carry connection identity in both events; `PeerState` records which
connection it is bound to; `onPeerDisconnected` ignores events for non-matching
connections (rust-libp2p's `ConnectionId` model). Alternatively dedup in the
Switch with a tie-break rule and fire `on_disconnected` only at 1→0.

### H1 — Inline `on_message` can self-deadlock the router — `HIGH`

`deliverLocal` invokes the application handler **on the router fiber**
(`router.zig:3299-3304`); `Gossipsub.publish` posts a blocking `putOne` into the
same inbox whose only consumer is that fiber (`gossipsub.zig:551-566`). A
handler that re-publishes — the natural eth2 pattern (receive → validate →
forward/aggregate) — deadlocks exactly when the inbox is full, i.e. under
flood. No documentation warns against it.

**Fix:** document the prohibition + provide a non-blocking `tryPublish`;
better, deliver to the app via a per-subscription bounded queue drained by app
fibers (go's Subscription model), keeping the zero-copy inline variant opt-in.

### H2 — Handle `deinit` vs concurrent use is an undocumented UAF — `HIGH`

`Impl.deinit` nulls the state pointer under `handle_lock` then
`allocator.destroy(self)` (`handle.zig:71-88`; stream identical at
`stream/handle.zig:61-77`). The spin-lock (`handle.zig:53-58`) protects only the
pointer swap, **not the Impl allocation** — a fiber concurrently inside
`read`/`write`/`openStream` dereferences freed memory, including the lock
itself. The real contract ("no concurrent use with deinit") is stated nowhere,
and the lock's presence suggests a guarantee it cannot deliver. Additionally
`Connection.deinit` blocks the caller until the actor fiber fully unwinds
(`actor.zig:273-288`) — callers won't expect a join inside deinit.

**Fix:** document the external-synchronization contract on both `deinit`s and
on `handle_lock`'s purpose, or refcount the Impl itself (method entry retains,
deinit marks dead, last exit frees).

### M1 — Fragile teardown invariant in `shutdownAndCleanup` — `MEDIUM`

Application queues are closed only when `shutdown_requested || isClosed`
(`actor.zig:448-451`). A bare `error.Canceled` exit from `mainLoop` without
either flag would strand uncancelably-parked command posters (the
belt-and-suspenders close lives in the *final* SharedState release, which a
parked caller's own retain prevents — deadlock). Unreachable today only because
the sole canceller sets the flag first (`actor.zig:274` before `:280`); one new
cancellation site breaks it silently. **Fix:** close unconditionally (it is
idempotent); the conditional buys nothing.

### M2 — Uncancelable reply waits + one blocking send point — `MEDIUM`

Every control reply wait is `waitUncancelable` (`handle.zig:135,146,259`),
justified only for the dead-actor case. But the actor can block indefinitely in
`sendMany` on the shared socket (`actor.zig:884`) — while it does, no recv, no
command processing, and parked callers cannot be cancelled. **Fix:** bound
`sendMany` with a deadline, and/or make reply waits cancelable via actor-owned
refcounted reply slots (Tokio-oneshot shape).

Note (2026-06-10): upstream Zig PR 35564 (codeberg.org/ziglang/zig/pulls/35564,
open) fixes a `std.Io.Condition` signal-vs-cancel race that swallows
`error.Canceled` and "leaves the task in an uncancelable state" —
`std.Io.Queue` (our inbox) is built on `Condition`, so today a cancel racing a
queue signal can be silently lost. This is part of why the codebase leans on
uncancelable waits + persistent signals. Once that fix lands, the cancelable
reply-wait rework here becomes safe to do (it still ALSO needs the rc'd reply
slots so an abandoned waiter cannot UAF). The teardown-protocol "last words"
posts (`shutdown`, `reap_dead_writers`) must stay uncancelable regardless —
their requirement is delivery-while-being-cancelled, which no cancel-semantics
fix changes.

### Minor (correctness-adjacent)

- Integration tests read `router.peers` off-fiber while the router runs
  (`gossipsub.zig:854-899`), violating the router's own sync-barrier contract
  (`router.zig:641-649`). Test-only data race; route through the `sync` command.
- `accepted_stream_push` waitset bit has no drainReady consumer — every
  accepted-stream push self-wakes the actor for a no-op tick
  (`shared_state.zig:243-252`).
- Stale file-head doc: `router.zig:20-21` claims IHAVE/IWANT/IDONTWANT are
  "parsed-but-ignored" — all three are fully implemented (`router.zig:2511,
  2586,2647`). Also a `GS_DEBUG` "TEMP DEBUG" block in the heartbeat
  (`router.zig:2158-2172`) and test-only commands in the production `Command`
  enum (`router.zig:636-649`).

---

## Part III — Performance findings (eth2 scale: ~100 peers, D=8, 64 subnets, 0.5–3k msgs/s, 100 KB+ blocks)

### P1 — Ed25519 verify runs inline, on the router fiber, **before dedup** — `CRITICAL` (the throughput ceiling)

`signing.verifyMessage` runs first (`router.zig:3019-3027`); `seen` is checked
only **after** (`router.zig:3043`). Gossipsub delivers each message up to D
times, and IDONTWANT suppression only fires for data ≥ 1 KiB — so eth2
attestations (<1 KiB) arrive ~2–4x duplicated with **every copy paying a full
~60–100 µs verify**, serialized on the one fiber that also runs mesh
maintenance, scoring, heartbeat sweeps, and (H1) the app handler.

Numbers: 1.5k unique msgs/s × 3 dup factor × 80 µs ≈ **360 ms/s — 36 % of the
fiber on crypto alone**; the whole-node ceiling is ~5–10k verifies/s regardless
of core count. Saturation back-pressures into Switch dial/accept (blocking
`putOne`, `gossipsub.zig:586-606`).

**Fix (three independent steps):**
1. Compute msg-id and **check** seen before verifying (mark only after verify
   passes, so a forged `from++seqno` cannot poison the cache) — ~10 lines, cuts
   crypto by the duplicate factor.
2. Move signature verification into the existing async-validation worker
   machinery (`router.zig:3083-3092` already owns snapshot/verdict-reentry) —
   verify then scales with cores, like go-libp2p's validation workers.
3. At the in-flight cap, throttle-**drop** like go instead of falling back to
   inline (`router.zig:3087-3092`) — the inline fallback means a validation
   flood still stalls the router.

### P2 — Send path: one event-loop round trip per packet; no GSO/sendmmsg — `HIGH`

zio's `netSendImpl` loops one `NetSendMsg` op + `waitForIo` per message
(vendored zio `src/io.zig:2093-2109` — verified); there is no
sendmmsg/UDP_SEGMENT anywhere (`socket_control.zig` emits only PKTINFO). The
actor's careful 32-packet `sendMany` batch (`actor.zig:846-890`) therefore
recovers **zero** syscalls. One 100 KB block to 8 mesh peers ≈ 680 event-loop
round trips. quinn/quic-go report 2–4x send throughput from GSO alone — the
single biggest known lever in production QUIC stacks.

**Fix:** sendmmsg in zio's netSend; UDP_SEGMENT (GSO) assembly in
`flushScheduled` over quiche's equal-size packet runs — target ≤ 1 syscall per
~64 KB. Both are zio/socket_control-local; the actor's batch loop already has
the right shape.

### P3 — Recv path: 2 fiber spawns + 1 cancel + 1 heap dupe per datagram; GRO inverted — `HIGH`

Each `routerSocketLoop` iteration builds a fresh `std.Io.Select` and spawns two
concurrent arms, cancelling the loser (`loop.zig:243-254`); zio does one
recvmsg per call (no recvmmsg in the vendored tree). Every datagram is
heap-duped into a `RoutedPacket` (`packet_route.zig:42`), and a GRO
super-datagram is re-duped **per segment** (`loop.zig:545-570`) — a 64 KB GRO
read becomes ~45 allocs+copies, inverting GRO's benefit. The recv fiber is also
the accept path: `startServerConnection` runs inline (`loop.zig:713-796`), so
accept bursts head-of-line-block every established connection. At ~30k pps this
is plausibly 30–50 % of a core of pure overhead, all serialized.

**Fix:** (a) the CID table is *not* quiche state — make it a sharded/mutex map
written directly by actors and the dialer (quinn's model), eliminating
`route_commands`-through-the-recv-fiber; the recv loop becomes a persistent
fiber with zero spawns (teardown stays flag + socket close). (b) recvmmsg in
zio. (c) a refcounted recv slab: `RoutedPacket = {slab ref, offset, len}`, GRO
segments become views into one pooled buffer — zero allocs/copies between the
wire and `quiche_conn_recv`. (d) hand post-Retry-validated Initials to an
accept fiber.

### P4 — Heartbeat sweeps: O(n·k) restart-after-removal scans on the router fiber — `HIGH`

`SeenCache.sweep` restarts the full map iteration after **every** removal
(`router.zig:360-375`); `seen` is count-unbounded by design. At 120 s TTL ×
1.5k msgs/s the map holds ~180k entries — multi-ms (worst-case near-second)
stalls every heartbeat, on the same fiber as P1. Related: `fulfillPromise`
probes **every peer's** promise map on **every** message (`router.zig:2565-2572`)
≈ 450k probes/s at 100 peers, for a usually-no-op feature.

**Fix:** expiry wheel for seen (bucket per heartbeat tick; sweep = drain one
bucket, O(expired)); one global msg-id-keyed promise map (go's tracer shape).

### P5 — Per-connection footprint: ~1.8 MiB and 5 fibers — `MEDIUM`

5 fibers/conn (conn actor, switch conn actor, dispatcher, reader, writer) ×
256 KiB committed zio stacks (zio `runtime.zig:63-68` — verified) ≈ 1.25 MiB,
plus ~190 KiB preallocated packet-channel slots (2048 × ~96 B), a 48-slot
datagram pool allocated even when unused, and 2×48 KiB + 16 KiB per stream —
≈ 1.8 MiB/conn, ~180 MB at 100 peers (3–5x quinn/go-libp2p). `flushScheduled`'s
~66 KiB of locals (`actor.zig:809-812`) guarantees every actor stack commits
its pages. The SwitchConnectionActor also serializes multistream negotiation
(network RTTs) on its command fiber (`switch.zig:759-781`), head-of-line
blocking close/stats for that conn.

**Fix:** drop to 3 fibers/conn (the switch conn actor owns no hot state — a
mutexed record + negotiation on the opener's fiber with a timeout); lazy
datagram pool; executor-local flush scratch; shrink the packet queue slot count.

### P6 — Control fan-out re-encodes; no per-peer RPC coalescing — `MEDIUM`

`sendIHave` does a fresh protobuf encode of up to 5000 ids **per gossip
target** (`router.zig:2227,2279-2288`); `broadcastIDontWant` frames per peer.
With ~70 topics × d_lazy=6 that is hundreds of full encodes per heartbeat on
the router fiber, and every GRAFT/PRUNE/IHAVE is its own length-prefixed frame
(go batches all control per peer per heartbeat into one RPC).

**Fix:** encode once per topic, share via the existing `fanOutFrame` machinery;
coalesce per-peer control into one RPC per flush.

### P7 — Per-message alloc/copy budget — `MEDIUM`

~10 heap allocations + 3 full-payload copies per relayed message before
fan-out; `readUvarint` does one refcounted, mutex-guarded `readAll` **per
byte** (`pubsub.zig:149-161`) while a buffered `StreamReader` exists unused
(`stream/handle.zig:319`); the frame's own `ids[0]` dupe duplicates the
already-interned id (`router.zig:3371-3375`).

### P8 — Slow-peer defense absent — `MEDIUM`

`StreamSink.writeFrame` is an unbounded `writeAll` (`gossipsub.zig:70-72`); a
stalled-but-alive peer parks its writer fiber forever, pinning up to 1024
refcounted data frames + 4096 control frames; the subscribe lane is unbounded
(`peer_io.zig:201`). go drops the queue; rust applies timeouts + penalties.
**Fix:** write deadline + queue-full eviction/penalty.

### P0 — Do this first: observability

No high-rate benchmark exists in-tree; inbox saturation is silent (producers
park with no metric). The interop numbers (3 ms handshakes) exercise none of
these paths. **Land a 50k-pps router microbenchmark and a 5k-msgs/s gossipsub
soak with per-stage counters** (verify time, sweep time, allocs/s, syscalls/s,
inbox saturation, lane drops) so every fix above is measurement-ranked rather
than estimate-ranked.

---

## Part IV — If designed from scratch (vs rust-libp2p / go-libp2p / quinn)

The honest comparison: the chosen skeleton **is** what a from-scratch design
would pick for this runtime.

- **vs rust-libp2p** (poll-based Swarm/ConnectionHandler, one event loop,
  behaviours composed via `NetworkBehaviour`): wrong fit for Zig — poll
  composition exists to avoid task-per-connection in a futures world; without
  compiler-generated state machines you would hand-write every resumable step.
  The one thing worth importing is the **ConnectionId-keyed lifecycle** (fixes
  C3) and `Bytes`-style shared buffers (already done via `OutboundFrame`).
- **vs go-libp2p** (goroutine-per-conn, single pubsub processLoop, per-peer
  outbound goroutines): this codebase consciously copies the right parts. What
  it under-copied: parallel signature validation workers (P1), seqno seeding
  (C2), timecache's bucketed expiry (P4), per-peer control-RPC coalescing (P6),
  throttle-drop at the validation cap, and anti-eclipse mesh shuffling (the
  star-topology selection here is deterministic map order — a v1.1 security
  feature, not a nicety).
- **vs quinn**: the connection-driver model matches; quinn's endpoint differs in
  exactly the two places flagged above — the CID table is a plain shared map
  (not routed through the recv task), and the UDP layer does GSO/GRO with
  batched syscalls. Those two differences are most of the 5–10x.

What a from-scratch design changes, concretely (all incremental on the current
architecture — no rewrite):

1. **Recv:** persistent fiber → recvmmsg into a slab ring → refcounted segment
   views → sharded CID map probe → channel move. Zero spawns/allocs per packet;
   SO_REUSEPORT shards for multi-core recv on Linux.
2. **Send:** sendmmsg + UDP_SEGMENT in zio; one GSO super-buffer per flush
   burst.
3. **Fiber topology:** 3 fibers/conn, not 5.
4. **Gossipsub pipeline split by state-need, not stage:** parse + seen-*check*
   + signature verify on the per-peer reader fibers (pure functions, parallel
   by peer for free); the router fiber does state mutation only; app delivery
   on per-subscription bounded queues. **Two inboxes:** a small control queue
   (peer events, writer-death — never backpressured by data) + the existing
   bounded data queue.
5. **Three iron rules** for review, derived from the bugs found:
   (i) a fiber that any other fiber joins must never block uncancelably on a
   resource the joiner controls; (ii) lifecycle events are keyed by connection
   identity, not PeerId; (iii) `cancel` is reserved for process teardown —
   hot-path coordination uses persistent signals.

---

## Part V — Landing order

| # | Item | Size | Status |
|---|---|---|---|
| 0 | gossipsub router + QUIC loopback micro-benchmarks, baselines on macOS+Linux | S | **done** (`docs/benchmarks/2026-06-10-p0-baseline.md`; per-stage router counters still open) |
| 1 | C1 writer-teardown deadlock; C2 seqno seed; C3 connection-identity lifecycle | S (C2 is one line) | **done** (`ff993a9`, `b54027b`, `962a009` + ABA follow-up `9839fec`) |
| 2 | P1 check-seen-before-verify; verify in validation workers; throttle-drop at cap | S–M | **done** (`68f159e`; promise fulfilment follows go's tracer carve-out — fulfilled on duplicate/throttle/post-signature verdicts, never on a signature reject) |
| 3 | P2 sendmmsg + GSO in zio + flushScheduled | M | open |
| 4 | P3 persistent recv fiber + sharded CID map + slab ring | M | **done** (`67cf084` + `3ee2bb1`: loopback 7-11x, pps 4-6x; bump-arena slabs deferred pending Linux profiling) |
| 5 | P4 expiry wheel + global promise map; P6 shared IHAVE encode + control coalescing | S–M | expiry wheel **done** (359×); promise map **closed by measurement** (+0.24 µs/msg at 64 peers); P6-a shared IHAVE **done** (6×: 0.85 → 0.14 ms/tick, allocs 58→6); P6-b coalescing **closed** (steady-state emits zero grafts/prunes — unmeasurable) |
| 6 | H1 delivery queue / tryPublish; H2 handle lifetime contract; M1/M2 hardenings; P5 fiber/memory diet | M | H1 **done** (`70a3a92`: queued delivery default, sync fences through the queue, inline opt-in); H2+M1 **done** (`9efc7b9`: deinit contract documented — an internal refcount cannot deliver more; unconditional queue close); M2 open (gated on Zig PR 35564); P5 diet **closed by measurement** (`362bf0d`: ~125 KiB RSS / ~437 KiB heap per conn-side — the 1.8 MiB estimate assumed committed stacks; negotiation head-of-line nit stays open) |
| — | P8 slow-peer defense: per-write timeout + consecutive-write-failure give-up + bounded subscribe lane + lane-drop counter | S | **done** (write timeout 10 s matching negotiation; 3 consecutive failures → reap via the existing on_disconnect machinery; subscribe lane capped at 256) |
| — | P7 alloc/copy diet: persistent buffered RPC reads + single-id frames | S | **done** (readUvarint per-byte locked `readAll` → one buffered refill per ~4 KiB; the frame id slice is gone — allocs/offered 13.0→12.0 strict, 9.0→8.0 anonymous; wiring the buffered reader flushed out and fixed a latent `StreamReader.readVec` fill-loop byte-loss bug) |
| — | quiche 0.28.0 C-FFI use-after-free (found by the P5 footprint bench, not in the original review): `quiche_connection_id_iter_next` / `quiche_conn_retired_scid_next` return dangling pointers | S | **done** (`64a057c` avoids both APIs' outputs; upstream cloudflare/quiche#2509 + fix PR #2510) |

| — | Two-inbox split (Part IV (d)): control/verdicts prioritized over data ingress | S | **done** (`f29492c`; bench: burst 601→1977 delivered, self-starvation eliminated) |

None of these disturb the ownership architecture — which is the part that is
already right.
