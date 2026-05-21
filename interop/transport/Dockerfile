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

RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && mv /opt/zig-x86_64-linux-${ZIG_VERSION} /opt/zig

RUN curl -fsSL https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}"

ENV PATH="/root/.cargo/bin:/opt/zig:${PATH}"

WORKDIR /app

COPY . .

RUN zig build -Doptimize=ReleaseFast -Dcpu=x86_64 -Dlog-level=${ZIG_LOG_LEVEL}

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        zlib1g \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/zig-out/bin/libp2p-transport-interop /usr/local/bin/libp2p-transport-interop

ENTRYPOINT ["/usr/local/bin/libp2p-transport-interop"]
