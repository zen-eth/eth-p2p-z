# syntax=docker/dockerfile:1
#
# Turn-key image to run the zig QUIC gossipsub interop node under the Shadow
# network simulator. It builds the node with the EPOLL zio backend (Shadow does
# not support io_uring, and io_uring is also seccomp-blocked in containers) and
# in shadow_compatible mode (selected at runtime via shadow=on), installs Shadow
# from source, and bakes in the 2-host shadow.yaml. The default entrypoint runs
# the simulation and prints a PASS/FAIL verdict.
#
# Build context is the REPO ROOT (the build needs the whole tree):
#   docker build -f interop/gossipsub/shadow/Dockerfile -t zig-gs-shadow .
# Run:
#   docker run --rm zig-gs-shadow
#
# Shadow uses LD_PRELOAD-style interception and needs SYS_PTRACE; the run script
# (run-shadow.sh) passes the required flags. `docker run` without them may fail
# at Shadow startup — prefer run-shadow.sh.

ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION=0.16.0
ARG RUST_VERSION=1.85.1
ARG SHADOW_VERSION=v3.3.0
ARG ZIG_LOG_LEVEL=info

# ---------------------------------------------------------------------------
# Stage 1: build the gossipsub interop node with the epoll backend.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS zig-build

ARG DEBIAN_FRONTEND
ARG ZIG_VERSION
ARG RUST_VERSION
ARG ZIG_LOG_LEVEL

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

# Arch-aware: in the Linux container `uname -m` is `aarch64` or `x86_64`, which
# matches Zig's release tarball naming (zig-<arch>-linux-<version>). This lets the
# image build natively on both arm64 (e.g. Apple-Silicon Docker) and x86_64 CI.
RUN ARCH="$(uname -m)" \
    && curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ARCH}-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && mv "/opt/zig-${ARCH}-linux-${ZIG_VERSION}" /opt/zig

# Rust is needed to build the vendored quiche crate (cdylib/staticlib) that the
# QUIC stack links against — same as the transport interop Dockerfile.
RUN curl -fsSL https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}"

ENV PATH="/root/.cargo/bin:/opt/zig:${PATH}"

WORKDIR /app
COPY . .

# Build on the epoll backend. Shadow intercepts epoll/recvmsg/sendmsg syscalls;
# it does NOT support io_uring (zio's default), and io_uring is additionally
# blocked by the default container seccomp profile. shadow_compatible mode (the
# plain recvmsg/sendmsg UDP path with no GRO/GSO/IP_PKTINFO/timestamp cmsgs) is
# selected at runtime by the node's shadow=on arg — see shadow.yaml.
RUN zig build install -Doptimize=ReleaseFast -Dzio-backend=epoll -Dlog-level=${ZIG_LOG_LEVEL}

# ---------------------------------------------------------------------------
# Stage 2: build Shadow from source.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS shadow-build

ARG DEBIAN_FRONTEND
ARG SHADOW_VERSION

# Shadow's documented Debian build dependencies plus rustup (Shadow's core is
# Rust). libclang-dev is needed by bindgen; python3-networkx by its tooling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        cmake \
        findutils \
        gcc \
        g++ \
        make \
        libclang-dev \
        libc-dbg \
        libglib2.0-0 \
        libglib2.0-dev \
        netbase \
        python3 \
        python3-networkx \
        util-linux \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

# Build + install Shadow to /opt/shadow (no spaces in the path: Shadow's
# LD_PRELOAD mechanism rejects whitespace in the install prefix).
RUN git clone --depth 1 --branch "${SHADOW_VERSION}" https://github.com/shadow/shadow.git /shadow \
    && cd /shadow \
    && ./setup build --clean --prefix /opt/shadow \
    && ./setup install

# ---------------------------------------------------------------------------
# Stage 3: runtime image with Shadow + the node + the scenario.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

ARG DEBIAN_FRONTEND

# Shadow's runtime shared-library dependencies (glib) plus the node's (libstdc++,
# zlib). procps gives `ps`/`free` for debugging inside the container.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libglib2.0-0 \
        libstdc++6 \
        zlib1g \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=shadow-build /opt/shadow /opt/shadow
ENV PATH="/opt/shadow/bin:${PATH}"

COPY --from=zig-build /app/zig-out/bin/libp2p-gossipsub-interop /usr/local/bin/libp2p-gossipsub-interop
COPY interop/gossipsub/shadow/shadow.yaml /sim/shadow.yaml
COPY interop/gossipsub/shadow/run-shadow.sh /usr/local/bin/run-shadow.sh
RUN chmod +x /usr/local/bin/run-shadow.sh

WORKDIR /sim

# Default: run the simulation and print the PASS/FAIL verdict. Override with a
# shell to poke around (`docker run --rm -it ... bash`).
ENTRYPOINT ["/usr/local/bin/run-shadow.sh"]
