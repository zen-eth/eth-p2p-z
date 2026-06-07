# zig QUIC gossipsub under Shadow

A turn-key, one-command Linux/Docker setup that runs two zig QUIC gossipsub
interop nodes under the [Shadow](https://shadow.github.io) network simulator:
`node0` listens + subscribes, `node1` dials + publishes, and `node0` must
receive the message. Passing this proves the **zio runtime (epoll backend) + the
QUIC stack run under Shadow** — the gating unknown before adding a zig
participant to the upstream gossipsub-interop framework.

> **Shadow runs only on Linux x86-64.** It does NOT support macOS, and it does
> NOT support arm64/aarch64 (per Shadow's supported-platforms docs). Run this on
> an **x86-64 Linux host** — a cloud VM, an x86-64 Linux box, or x86-64 CI (e.g.
> GitHub Actions `ubuntu-latest`, which is where the upstream interop runs).
>
> On an Apple-Silicon / arm64 machine, Docker's Linux VM is arm64, so Shadow
> cannot run there even in a container. Forcing `--platform linux/amd64` runs the
> whole stack under x86-64 emulation (Rosetta/qemu), which Shadow's ptrace/seccomp
> interception does not reliably support — use a real x86-64 Linux host instead.

## One command

From the **repository root** (the Docker build needs the whole source tree), on
an **x86-64 Linux host**:

```sh
docker build -f interop/gossipsub/shadow/Dockerfile -t zig-gs-shadow . \
  && docker run --rm \
       --shm-size=2g \
       --cap-add=SYS_PTRACE \
       --security-opt seccomp=unconfined \
       zig-gs-shadow
```

The `docker run` flags matter:

- `--shm-size=2g` — Shadow keeps its simulated process memory in `/dev/shm`; the
  default 64 MB is far too small and Shadow fails to start without this.
- `--cap-add=SYS_PTRACE` — Shadow intercepts managed processes via ptrace.
- `--security-opt seccomp=unconfined` — Shadow makes syscalls the default Docker
  seccomp profile blocks. (The node itself uses the epoll backend specifically so
  it does **not** need io_uring, which seccomp blocks; this flag is for Shadow.)

A successful run ends with:

```
PASS: node0 received the gossipsub message over QUIC under Shadow
      => zio (epoll) + QUIC run under Shadow.
```

and exit code 0.

## What it does

- **Dockerfile** (3 stages):
  1. Builds `libp2p-gossipsub-interop` with `-Dzio-backend=epoll` (Shadow does
     not support io_uring; io_uring is also seccomp-blocked in containers). The
     node selects the Shadow-compatible UDP path at runtime via `shadow=on`.
  2. Builds Shadow from source (pinned to a release tag).
  3. Runtime image: Shadow + the node binary + `shadow.yaml` + `run-shadow.sh`.
- **shadow.yaml**: 2 hosts on one network node (10ms self-loop link), explicit
  IPs `11.0.0.1`/`11.0.0.2`, `stop_time: 30s`,
  `model_unblocked_syscall_latency: true`, and generous socket buffers. Each host
  runs the node with `shadow=on` and `announce=<its-ip>`.
- **run-shadow.sh**: runs Shadow, prints both nodes' stdout, and greps node0's
  stdout for the `RECV` line + `"received":true` to decide PASS/FAIL.

## How the two nodes find each other

Each Shadow host is single-homed (one assigned IP), and a `0.0.0.0` bind is not
dialable, so the node has two additive args (off by default, so the non-Shadow
on-host scenarios are unchanged):

- `shadow=on` — runs the QUIC endpoint in `shadow_compatible` mode: plain
  `recvmsg`/`sendmsg` with no GRO/GSO, no `IP_PKTINFO`, no per-packet timestamps
  — the only socket form Shadow supports. (Under Shadow there is no per-packet
  local-destination address, so the node falls back to the socket's bound
  address; that is why the listener binds its specific IP, not the wildcard.)
- `announce=<ip>` — the listener BINDS and ADVERTISES this IP (its Shadow host
  IP) in its addr file, identify listen addrs, and sealed peer record.

Coordination is the **addr_file / peer_file pair over the shared host
filesystem** (Shadow runs processes against the real FS): `node0` writes its
published `/p2p/<peer-id>` multiaddr to `/tmp/node0.addr`, and `node1` reads it
via `peer_file=/tmp/node0.addr`. `node1` starts 2s after `node0`, and `peer_file`
additionally polls for the file, so the listener is always bound first.

This file-based coordination is simpler than the upstream framework's
deterministic-key-from-hostname scheme (which would need an ED25519-from-seed
key constructor plumbed through the TLS layer) and is sufficient for a fixed
2-node de-risk.

## Inspecting a run

Shadow writes per-process output under the data dir (default `/sim/shadow.data`
inside the container):

```
/sim/shadow.data/hosts/node0/libp2p-gossipsub-interop.*.stdout   # RECV lines + result JSON
/sim/shadow.data/hosts/node0/libp2p-gossipsub-interop.*.stderr   # info/debug logs
/sim/shadow.data/hosts/node1/...
```

To poke around interactively:

```sh
docker run --rm -it --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
  --entrypoint bash zig-gs-shadow
# then inside: run-shadow.sh ; ls -R /sim/shadow.data/hosts
```

## What a PASS means (and the remaining unknown)

PASS confirms a full QUIC handshake, `/meshsub` negotiation, and a gossipsub
publish→deliver, all running on the zio **epoll** runtime over Shadow's emulated
UDP. That is the de-risk for the zig participant PR: the open question was
whether zio's cooperative runtime and our QUIC packet path behave under Shadow's
deterministic, intercepted-syscall execution model. A green run answers yes.

The node logic and the Shadow-compatible UDP path are verified on macOS (the
shadow-mode 2-node interop passes there over loopback QUIC). The epoll backend
build and the Shadow run itself can only be exercised on Linux — that is exactly
what this image + command do.
