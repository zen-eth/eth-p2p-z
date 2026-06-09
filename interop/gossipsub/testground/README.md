# gossipsub-quic — Testground plan (Path B)

A [Testground](https://github.com/seetadev/testground-quic) participant for
**QUIC-only gossipsub interop** using eth-p2p-z.

**Path B:** a Go testplan owns all Testground coordination (sync barriers,
peer-address exchange, network shaping, result recording) and drives the
eth-p2p-z `libp2p-gossipsub-interop` node as a subprocess via its `key=value`
CLI. The Zig node stays a dumb node. Testground runs real Docker containers on a
real `tc`/`netem`-shaped network, so **real QUIC works** — unlike the Shadow
simulator (which can't carry eth-p2p-z's QUIC transport).

## What the test does (`publish-deliver`)
1. Every instance waits for the data network and shapes it (latency/bandwidth).
2. Role from `initCtx.GlobalSeq`: instance **1 = publisher** (`role=listen
   mode=pub`), all others = **subscribers** (`role=listen mode=sub`).
3. Each node binds + advertises its **data-network IP** via `announce=<ip>` (NOT
   `127.0.0.1`), so the published `/ip4/<ip>/udp/<port>/quic-v1/p2p/<id>`
   multiaddr is dialable across containers.
4. The publisher mints its multiaddr (polls the node's `addr_file`), publishes it
   on a sync topic; a `ready` barrier gates subscribers until then.
5. Subscribers collect the publisher multiaddr(s) and launch with `peers=<addr>`.
6. The Go plan scrapes node stdout for delivery (`RECV topic=…` + final
   `{"received":true}`) → `RecordSuccess`/`RecordFailure`. A `complete` barrier
   holds all nodes before teardown.

## Files
- `main.go` — the Go participant (sdk-go coordination + node driver).
- `manifest.toml` — plan + testcase + params (`builder=docker:generic`).
- `Dockerfile` — multi-stage: stage 1 builds the eth-p2p-z node (epoll backend),
  stage 2 builds the Go plan, stage 3 carries both (ENTRYPOINT = the Go plan).
- `go.mod` — pins sdk-go to the testground-quic platform fork's version.
- `import.sh` — imports the repo root as the plan (handles the manifest staging).
- `../../../.testgroundignore` (repo root) — excludes build artifacts from the import.

## Build & run
Prereqs: a running `testground daemon` (own terminal) + Docker.

```sh
# 0) one-time: generate go.sum (needs network) — the Docker stage `go mod download` needs it
cd interop/gossipsub/testground && go mod tidy && cd -

# 1) import (stages the manifest at the repo root, imports, verifies, cleans up)
bash interop/gossipsub/testground/import.sh

# 2) build the image (slow: Zig + Rust + Go compile from scratch)
testground build single --plan gossipsub-quic --builder docker:generic --wait

# 3) run a 2-node publish/deliver test over shaped QUIC
testground run single --plan gossipsub-quic --testcase publish-deliver \
    --builder docker:generic --runner local:docker --instances 2 --wait
```

Scale / override params (1 publisher + 5 subscribers, anonymous, 30s, 100ms link):

```sh
testground run single --plan gossipsub-quic --testcase publish-deliver \
    --builder docker:generic --runner local:docker --instances 6 --wait \
    --test-param sign=anonymous --test-param duration_ms=30000 \
    --test-param latency_ms=100 --test-param message=hello-quic
```

**Pass:** each subscriber logs `RECV topic=<topic> … data=<message>` + final
`{"role":"listen","mode":"sub","received":true}` → `RecordSuccess`; the publisher
passes on clean exit. A subscriber miss fails that instance (and the run).

## Params
`topic`, `message`, `sign` (strict|anonymous), `px` (on|off), `duration_ms`,
`d`/`d_low`/`d_high` (mesh degrees), `port`, `n_publishers`, `latency_ms`,
`bandwidth_mb`, `setup_slack_secs`. See `manifest.toml` for defaults.

## Notes / gotchas
- **Run with `--runner local:docker`** (not `local:exec`): only the docker
  runner provides the sidecar (data network + tc/netem shaping). Under
  `local:exec` the node falls back to `announce=0.0.0.0` (loopback only).
- **`sign=anonymous`** makes the publisher append `#<seq>` to each payload, so a
  subscriber's `data=` is `<message>#<n>` — the plan matches on the `RECV topic=`
  prefix + `"received":true`, so it works for both modes. Don't tighten the match
  to require an exact `data=<message>`.
- **First build is minutes-long** (compiles Zig + Rust/quiche + Go). Under high
  `WORKER_COUNT` the local:docker runner has a known startup race
  (`ConnectionRefused`) unrelated to this code — run serially or retry.
- **Data-network IP bind** is the one runtime unknown only a real run proves: the
  node binds `announce=<dataIP>` directly. The plan launches the node only after
  `MustWaitNetworkInitialized` + `MustConfigureNetwork`, by which point the
  sidecar has assigned the IP, so the bind should succeed.
- **Zig 0.16.0 tarball:** stage 1 curls `ziglang.org/download/0.16.0/...`. If your
  build host's arch tarball 404s, align `ZIG_VERSION` with what
  `interop/gossipsub/shadow/Dockerfile` uses (the proven build).
