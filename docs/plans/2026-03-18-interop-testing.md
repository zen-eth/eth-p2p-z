# Interop Testing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Port the existing interop binary to the new Zig 0.16 + comptime Switch API so it works with the official `libp2p/test-plans` framework.

**Architecture:** The interop binary is a standalone executable that acts as either a listener or dialer, coordinated via Redis. It uses the comptime Switch with ping (and identify) protocols over QUIC-v1. Redis client uses `std.Io.net` TCP streams. Entry point uses `Io.Threaded`.

**Tech Stack:** Zig 0.16.0-dev, std.Io, comptime Switch, QUIC-v1, Redis RESP protocol, Docker

---

### Task 1: Add interop build step to build.zig

**Files:**
- Modify: `build.zig:62-84`

**Step 1: Add the interop executable build step**

After the test step (line 83), add:

```zig
    // --- Interop binary ---
    const interop_module = b.createModule(.{
        .root_source_file = b.path("interop/transport/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    interop_module.addImport("zig-libp2p", root_module);
    interop_module.addImport("ssl", ssl_module);
    interop_module.addImport("multiaddr", multiaddr_module);
    interop_module.addImport("peer_id", peer_id_module);

    const interop_exe = b.addExecutable(.{
        .name = "libp2p-transport-interop",
        .root_module = interop_module,
    });
    interop_exe.root_module.linkLibrary(lsquic_artifact);
    interop_exe.root_module.addIncludePath(lsquic_dep.path("include"));
    b.installArtifact(interop_exe);

    const interop_step = b.step("transport-interop", "Build the transport interop binary");
    interop_step.dependOn(&interop_exe.step);
```

**Step 2: Verify it compiles (will fail — main.zig uses old API)**

Run: `./zig-aarch64-macos-0.16.0-dev.2821+3edaef9e0/zig build transport-interop 2>&1 | head -5`
Expected: Compilation errors about old imports — confirms the build step works.

**Step 3: Commit**

```bash
git add build.zig
git commit -m "build: add transport-interop build step for interop binary"
```

---

### Task 2: Rewrite interop/transport/main.zig — env parsing and Redis client

This is the biggest task. Rewrite the entire file from scratch using the new API. The env parsing helpers and Redis client need to use Zig 0.16 APIs.

**Files:**
- Rewrite: `interop/transport/main.zig`

**Step 1: Write the new main.zig with env parsing, Redis client, and Switch setup**

The new file structure:

```zig
const std = @import("std");
const Io = std.Io;
const net = Io.net;
const libp2p = @import("zig-libp2p");
const Switch = libp2p.Switch;
const ping_mod = libp2p.ping;
const identify_mod = libp2p.identify;
const quic_mod = libp2p.quic_transport;
const engine_mod = libp2p.quic_engine;
const multiaddr = @import("multiaddr");
const Multiaddr = multiaddr.Multiaddr;
const ssl = @import("ssl");
const tls_mod = libp2p.tls;

const log = std.log.scoped(.interop);
```

Key changes from old to new:

1. **Entry point**: `pub fn main() !void` creates `Io.Threaded`, gets `Io`, calls `mainIo(io)`.
2. **Env parsing**: Use `std.c.getenv` instead of `std.process.getEnvVarOwned` (which doesn't exist in 0.16). Return slices directly (no ownership needed since getenv returns static pointers).
3. **Redis client**: Use `net.IpAddress.connect(addr, io, .{.mode = .nonblocking})` for TCP, then `stream.reader(io, &buf)` / `stream.writer(io, &buf)` for RESP protocol. All IO operations take `io: Io`.
4. **Switch**: `Switch(.{ .transports = &.{quic_mod.QuicTransport}, .protocols = &.{ping_mod.Handler, identify_mod.Handler} })`.
5. **Listener**: `sw.listen(io, addr)`, get bound port, format multiaddr + peer_id, RPUSH to Redis, sleep.
6. **Dialer**: BLPOP from Redis, `sw.dial(io, addr)`, `sw.newStream(io, peer_id, ping_mod.Handler)`, read RTT from handler, print JSON.
7. **RTT timing**: Use `std.c.mach_absolute_time()` on macOS. For Linux (Docker), use `clock_gettime(CLOCK_MONOTONIC)`. Use a portable wrapper.
8. **JSON output**: Use `std.json.Stringify.value(metrics, .{}, &writer)`.

**Step 2: Verify it compiles**

Run: `./zig-aarch64-macos-0.16.0-dev.2821+3edaef9e0/zig build transport-interop`
Expected: Clean compilation.

**Step 3: Commit**

```bash
git add interop/transport/main.zig
git commit -m "feat: port interop binary to Zig 0.16 + comptime Switch API"
```

---

### Task 3: Update Dockerfile

**Files:**
- Modify: `interop/transport/Dockerfile`

**Step 1: Update Zig version and build command**

```dockerfile
# syntax=docker/dockerfile:1

ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION=0.16.0-dev.2821+3edaef9e0

FROM debian:bookworm-slim AS build

ARG DEBIAN_FRONTEND
ARG ZIG_VERSION

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        xz-utils \
        git \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        perl \
        python3 \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://ziglang.org/builds/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && mv /opt/zig-x86_64-linux-${ZIG_VERSION} /opt/zig

ENV PATH="/opt/zig:${PATH}"

WORKDIR /app

COPY . .

RUN zig build transport-interop -Doptimize=ReleaseFast

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        zlib1g \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/zig-out/bin/libp2p-transport-interop /usr/local/bin/libp2p-transport-interop

ENTRYPOINT ["/usr/local/bin/libp2p-transport-interop"]
```

Key changes:
- Zig version: `0.16.0-dev.2821+3edaef9e0`
- Download URL: `ziglang.org/builds/` (dev builds) instead of `ziglang.org/download/`
- Build command: `zig build transport-interop` instead of `zig build`
- Removed: `-Dlibxev-backend=epoll` and `-Dlog-level` flags
- Removed: `unzip` package (not needed)

**Step 2: Commit**

```bash
git add interop/transport/Dockerfile
git commit -m "build: update Dockerfile for Zig 0.16 and new build step"
```

---

### Task 4: Local self-test (zig listener + zig dialer)

**Step 1: Start local Redis**

```bash
docker run --rm -d --name redis-test -p 6379:6379 redis:7-alpine
```

**Step 2: Build the interop binary**

```bash
./zig-aarch64-macos-0.16.0-dev.2821+3edaef9e0/zig build transport-interop
```

**Step 3: Run listener in background**

```bash
TRANSPORT=quic-v1 IS_DIALER=false REDIS_ADDR=localhost:6379 IP=127.0.0.1 LISTEN_PORT=0 \
  ./zig-out/bin/libp2p-transport-interop &
```

**Step 4: Run dialer**

```bash
TRANSPORT=quic-v1 IS_DIALER=true REDIS_ADDR=localhost:6379 TEST_TIMEOUT_SECONDS=30 \
  ./zig-out/bin/libp2p-transport-interop
```

Expected output: JSON line like `{"handshakePlusOneRTTMillis":12.345,"pingRTTMilllis":1.234}`

**Step 5: Clean up**

```bash
docker stop redis-test
kill %1  # listener background job
```

**Step 6: Commit any fixes**

---

### Task 5: Docker build test

**Step 1: Build Docker image**

```bash
docker build -t eth-p2p-z-interop -f interop/transport/Dockerfile .
```

Expected: Clean build, image created.

**Step 2: Test with docker-compose (optional)**

Create a minimal `docker-compose.yml` for local testing:

```yaml
services:
  redis:
    image: redis:7-alpine
  listener:
    image: eth-p2p-z-interop
    environment:
      TRANSPORT: quic-v1
      IS_DIALER: "false"
      REDIS_ADDR: redis:6379
      IP: 0.0.0.0
    depends_on: [redis]
  dialer:
    image: eth-p2p-z-interop
    environment:
      TRANSPORT: quic-v1
      IS_DIALER: "true"
      REDIS_ADDR: redis:6379
    depends_on: [redis, listener]
```

Run: `docker compose up --abort-on-container-exit`
Expected: Dialer prints JSON metrics and exits 0.

**Step 3: Commit**

```bash
git add interop/transport/Dockerfile
git commit -m "test: verify Docker build for interop binary"
```

---

### Task 6: Push and verify

**Step 1: Push branch**

```bash
git push origin feat/zig-master-rewrite
```

**Step 2: Verify the official test-plans can use our Docker image**

This is manual — follow the `libp2p/test-plans` instructions to run the transport interop suite with our image against go-libp2p and rust-libp2p.
