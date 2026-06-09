# syntax=docker/dockerfile:1
#
# Multi-stage image for the Testground "gossipsub-quic" Path-B plan.
#   Stage 1 (zig-build)  : build the eth-p2p-z gossipsub node (EPOLL backend).
#   Stage 2 (go-peer-build)  : build the go-libp2p (quic-go) gossipsub peer.
#   Stage 3 (rust-peer-build): build the rust-libp2p (quinn) gossipsub peer.
#   Stage 4 (plan-build) : build the Go testplan that drives a peer.
#   Stage 5 (runtime)    : carry all THREE peer binaries + the plan;
#                          ENTRYPOINT = the Go plan, which picks the peer binary
#                          per instance from the `impls` param (zig|go|rust).
#
# docker:generic (testground-quic/pkg/build/docker_generic.go) invokes THIS
# Dockerfile with:
#   * build context root = UnpackedSources.BaseDir. The imported plan tree is
#     unpacked UNDER ./plan/ (engine.go: "BaseDir is the directory containing the
#     plan under ./plan"). We import the eth-p2p-z REPO ROOT, so ./plan IS the
#     repo root and ./plan/<PLAN_PATH> is this Go-plan dir.
#   * Dockerfile path = /plan/<PLAN_PATH>/Dockerfile, PLAN_PATH auto-injected
#     (= interop/gossipsub/testground, from the manifest `path`).
#   * NetworkMode=host (so curl-ing the zig/rust tarballs + go mod download work).
ARG image=golang:1.21-bookworm
ARG ZIG_VERSION=0.16.0
ARG RUST_VERSION=1.85.1
ARG ZIG_LOG_LEVEL=info
# go-peer's go.mod requires go >= 1.24, so its build stage pins a newer toolchain
# than the plan-build stage (the plan itself only needs go 1.19).
ARG GO_PEER_VERSION=1.24
# go module proxy for the go-peer's dependency download (mirror first, then the
# canonical proxy, then direct VCS). Override with --build-arg GOPROXY=... .
ARG GOPROXY=https://goproxy.cn,https://proxy.golang.org,direct

# --------------------------------------------------------------------------
# Stage 1: build the eth-p2p-z gossipsub node with the EPOLL zio backend.
# (io_uring is blocked by the default Docker seccomp profile -> must use epoll.)
# --------------------------------------------------------------------------
FROM debian:bookworm-slim AS zig-build
ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION
ARG RUST_VERSION
ARG ZIG_LOG_LEVEL
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl xz-utils git build-essential cmake ninja-build \
        pkg-config perl python3 zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
# Arch-aware: container `uname -m` is aarch64|x86_64, matching Zig's tarball name.
RUN ARCH="$(uname -m)" \
    && curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ARCH}-linux-${ZIG_VERSION}.tar.xz" \
       | tar -xJ -C /opt \
    && mv "/opt/zig-${ARCH}-linux-${ZIG_VERSION}" /opt/zig
# Rust builds the vendored quiche crate the QUIC stack links against.
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}"
ENV PATH="/root/.cargo/bin:/opt/zig:${PATH}"
WORKDIR /src
# The imported tree is at ./plan/ in the build context; ./plan IS the eth-p2p-z
# repo root. Copy it to /src.
COPY plan/ .
# Drop any host-built cache so Zig doesn't serve a stale compile (known trap).
RUN rm -rf .zig-cache zig-out \
    && zig build install -Doptimize=ReleaseFast -Dzio-backend=epoll -Dlog-level=${ZIG_LOG_LEVEL}
# artifact: /src/zig-out/bin/libp2p-gossipsub-interop

# --------------------------------------------------------------------------
# Stage 2: build the go-libp2p (quic-go) gossipsub peer.
# Pure-Go (CGO disabled) static-ish QUIC node; needs the module download.
# --------------------------------------------------------------------------
FROM golang:${GO_PEER_VERSION}-bookworm AS go-peer-build
ARG GOPROXY
ENV CGO_ENABLED=0
ENV GOPROXY=${GOPROXY}
ENV GOFLAGS=-mod=mod
WORKDIR /src
# The imported tree is at ./plan/ in the build context; the go-peer sources live
# under plan/interop/gossipsub/go-peer.
COPY plan/interop/gossipsub/go-peer/ ./
# Prime the FULL build closure (all transitive module zips), retried, so a
# transient proxy error does not abort the image.
RUN for i in 1 2 3 4 5; do go mod download all && break || sleep 5; done
RUN go build -o /gspeer-go .
# artifact: /gspeer-go

# --------------------------------------------------------------------------
# Stage 3: build the rust-libp2p (quinn) gossipsub peer.
# quinn uses rustls (no native OpenSSL); the rustup gnu toolchain links glibc,
# matching the debian:bookworm runtime.
# --------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS rust-peer-build
WORKDIR /src
# The imported tree is at ./plan/ in the build context; the rust-peer sources
# live under plan/interop/gossipsub/rust-peer.
COPY plan/interop/gossipsub/rust-peer/ ./
RUN cargo build --release
# artifact: /src/target/release/gspeer

# --------------------------------------------------------------------------
# Stage 4: build the Go testplan.
# --------------------------------------------------------------------------
FROM ${image} AS plan-build
ARG PLAN_PATH
ENV CGO_ENABLED=0
WORKDIR /build
# Copy only the module files for a cached dependency download...
COPY plan/${PLAN_PATH}/go.mod plan/${PLAN_PATH}/go.sum ./
RUN go mod download
# ...then the plan sources and build.
COPY plan/${PLAN_PATH}/ ./
RUN go build -a -o /testplan .

# --------------------------------------------------------------------------
# Stage 5: runtime carrying ALL THREE peer binaries + the plan; ENTRYPOINT =
# the Go plan, which selects the peer per instance from the `impls` param.
# --------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
# libstdc++6/zlib1g cover the zig node; the go peer is pure-Go (CGO disabled) and
# the rust peer links only glibc + rustls (no native OpenSSL), so the base libc
# already present suffices for both.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libstdc++6 zlib1g iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=zig-build       /src/zig-out/bin/libp2p-gossipsub-interop /usr/local/bin/libp2p-gossipsub-interop
COPY --from=go-peer-build   /gspeer-go                                /usr/local/bin/gspeer-go
COPY --from=rust-peer-build /src/target/release/gspeer                /usr/local/bin/gspeer-rust
COPY --from=plan-build      /testplan                                 /testplan
ENV GOSSIPSUB_BIN=/usr/local/bin/libp2p-gossipsub-interop
# Testground health/pprof port.
EXPOSE 6060
ENTRYPOINT ["/testplan"]
