# syntax=docker/dockerfile:1

ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION=0.16.0
ARG RUST_VERSION=1.85.1
ARG ZIG_LOG_LEVEL=info

FROM debian:bookworm-slim AS build

ARG DEBIAN_FRONTEND
ARG ZIG_VERSION
ARG RUST_VERSION
ARG ZIG_LOG_LEVEL

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        xz-utils \
        unzip \
        git \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        perl \
        python3 \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Arch-aware: `uname -m` (aarch64 / x86_64) matches Zig's tarball naming, so the
# image builds natively on the host arch — x86_64 on CI, arm64 on Apple Silicon,
# no x86-under-Rosetta emulation.
RUN ARCH="$(uname -m)" \
    && curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ARCH}-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && mv "/opt/zig-${ARCH}-linux-${ZIG_VERSION}" /opt/zig

RUN curl -fsSL https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}"

ENV PATH="/root/.cargo/bin:/opt/zig:${PATH}"

WORKDIR /app

COPY . .

# Build on the epoll backend: io_uring's syscalls are blocked by the default
# Docker seccomp profile that the libp2p test-plans harness runs containers
# under, so an io_uring-backed binary dies at startup with PermissionDenied.
# epoll runs fine under default seccomp (like rust/go's interop binaries).
RUN zig build -Doptimize=ReleaseFast -Dcpu=baseline -Dzio-backend=epoll -Dlog-level=${ZIG_LOG_LEVEL}

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        zlib1g \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/zig-out/bin/libp2p-transport-interop /usr/local/bin/libp2p-transport-interop

ENTRYPOINT ["/usr/local/bin/libp2p-transport-interop"]
