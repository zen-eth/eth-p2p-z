ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION=0.16.0-dev.2821+3edaef9e0
FROM debian:bookworm AS build

ARG DEBIAN_FRONTEND
ARG ZIG_VERSION

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

RUN ARCH=$(uname -m) \
    && ZIG_ARCH=$(case "$ARCH" in aarch64) echo "aarch64";; x86_64) echo "x86_64";; *) echo "x86_64";; esac) \
    && curl -fsSL "https://ziglang.org/builds/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && mv /opt/zig-${ZIG_ARCH}-linux-${ZIG_VERSION} /opt/zig

# Patch Uring.zig chdir bug: ev.chdir() should be plain chdir() (standalone fn, not method)
RUN sed -i 's/return ev\.chdir(\&sync, dir_path_posix);/return chdir(\&sync, dir_path_posix);/' /opt/zig/lib/std/Io/Uring.zig

# Patch glibc _Float* typedefs that Zig's translate-c cannot parse on x86_64.
# These types are GCC extensions unused by BoringSSL; safe to remove.
RUN if [ -f /usr/include/x86_64-linux-gnu/bits/floatn-common.h ]; then \
      sed -i '/^typedef.*_Float[0-9][0-9]*x\?;/d' /usr/include/x86_64-linux-gnu/bits/floatn-common.h; \
    fi

ENV PATH="/opt/zig:${PATH}"

WORKDIR /app

COPY . .

RUN zig build transport-interop -Doptimize=ReleaseFast

FROM debian:bookworm AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        zlib1g \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/zig-out/bin/libp2p-transport-interop /usr/local/bin/libp2p-transport-interop

ENTRYPOINT ["/usr/local/bin/libp2p-transport-interop"]
