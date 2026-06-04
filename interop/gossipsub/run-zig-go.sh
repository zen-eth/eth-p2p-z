#!/usr/bin/env bash
# Cross-implementation gossipsub interop: a real go-libp2p node talking to our
# zig gossipsub node over loopback QUIC. Two scenarios run back to back:
#
#   PRIMARY (go -> zig): the ZIG node listens + subscribes; the GO node dials +
#     publishes (default StrictSign signing). Asserts the zig subscriber logs
#     RECV ... data=<message> and received:true. This is the de-risk target: it
#     exercises negotiation, RPC framing, subscription propagation, GRAFT/PRUNE,
#     and message receipt with go-libp2p's signed Message on the wire.
#
#   REVERSE (zig -> go): the GO node listens + subscribes; the ZIG node dials +
#     publishes SIGNED (StrictSign). go-libp2p in its default StrictSign mode now
#     ACCEPTS the zig message because zig signs per the libp2p pubsub scheme.
#     Asserts go logs received:true. This proves the zig-side signing matches
#     libp2p byte-for-byte (go verifies the signature + the from/key binding).
#
# Everything is bounded by duration and leftover processes are killed on exit.

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GS_DIR="$ROOT_DIR/interop/gossipsub"
GO_DIR="$GS_DIR/go-peer"
ZIG_BIN="$ROOT_DIR/zig-out/bin/libp2p-gossipsub-interop"
GO_BIN="$GO_DIR/gspeer"

TOPIC="interop-test"
MESSAGE="hello-from-go"

# This environment has no network for go module downloads; the go peer's deps
# are pinned to versions present in the local module cache, so build offline.
export GOPROXY=off
export GOSUMDB=off
export GOFLAGS=-mod=mod

# --- build both peers ------------------------------------------------------
echo "==> Building zig gossipsub interop binary"
( cd "$ROOT_DIR" && zig build install )
if [ ! -x "$ZIG_BIN" ]; then
    echo "FAIL: zig binary not found at $ZIG_BIN" >&2
    exit 1
fi

echo "==> Building go-libp2p peer"
( cd "$GO_DIR" && go build -o gspeer . )
if [ ! -x "$GO_BIN" ]; then
    echo "FAIL: go peer not found at $GO_BIN" >&2
    exit 1
fi

# --- process bookkeeping ---------------------------------------------------
PIDS=()
TMPFILES=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    for f in "${TMPFILES[@]:-}"; do
        [ -n "$f" ] && rm -f "$f"
    done
}
trap cleanup EXIT

mktmp() {
    local f
    f="$(mktemp -t "gs_interop.XXXXXX")"
    TMPFILES+=("$f")
    echo "$f"
}

# wait_for_addr <addr_file> <pid> -> echoes the address, or empty on timeout
wait_for_addr() {
    local addr_file="$1" pid="$2" waited=0 addr=""
    while [ "$waited" -lt 100 ]; do
        if [ -s "$addr_file" ]; then
            addr="$(head -n1 "$addr_file")"
            [ -n "$addr" ] && { echo "$addr"; return 0; }
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.1
        waited=$((waited + 1))
    done
    return 1
}

OVERALL=0

# ===========================================================================
# PRIMARY: go publishes -> zig subscribes (the de-risk target)
# ===========================================================================
echo
echo "############################################################"
echo "# PRIMARY scenario: go-libp2p publishes -> zig receives"
echo "############################################################"

ZIG_ADDR_FILE="$(mktmp)"
ZIG_LOG="$(mktmp)"
GO_LOG="$(mktmp)"
: > "$ZIG_ADDR_FILE"

echo "==> Starting ZIG node (role=listen mode=sub) on port 4111"
GOSSIPSUB_INTEROP_DEBUG=1 "$ZIG_BIN" \
    role=listen mode=sub \
    port=4111 \
    "topic=$TOPIC" \
    "addr_file=$ZIG_ADDR_FILE" \
    duration_ms=14000 \
    > "$ZIG_LOG" 2>&1 &
ZIG_PID=$!
PIDS+=("$ZIG_PID")

echo "==> Waiting for zig listener address"
ZIG_ADDR="$(wait_for_addr "$ZIG_ADDR_FILE" "$ZIG_PID")"
if [ -z "$ZIG_ADDR" ]; then
    echo "FAIL: zig listener never published its address" >&2
    echo "--- zig log ---" >&2; cat "$ZIG_LOG" >&2
    exit 1
fi
echo "    zig address: $ZIG_ADDR"

echo "==> Starting GO node (role=dial mode=pub, StrictSign) -> zig"
"$GO_BIN" \
    role=dial mode=pub \
    "peer=$ZIG_ADDR" \
    "topic=$TOPIC" \
    "message=$MESSAGE" \
    duration_ms=12000 \
    > "$GO_LOG" 2>&1 &
GO_PID=$!
PIDS+=("$GO_PID")

echo "==> Waiting for both nodes to finish"
wait "$GO_PID" 2>/dev/null
wait "$ZIG_PID" 2>/dev/null

echo
echo "--- zig (subscriber) output ---"
cat "$ZIG_LOG"
echo "--- go (publisher) output ---"
cat "$GO_LOG"
echo

if grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "$ZIG_LOG" \
   && grep -q '"role":"listen".*"received":true' "$ZIG_LOG"; then
    echo "PASS (PRIMARY): zig node received a message published by go-libp2p over QUIC"
else
    echo "FAIL (PRIMARY): zig node did not receive the go-libp2p message" >&2
    OVERALL=1
fi

# ===========================================================================
# REVERSE: zig publishes (StrictSign) -> go subscribes. With zig-side signing
# go's default StrictSign mode now ACCEPTS the message. This is the acceptance
# test proving zig's signing matches libp2p.
# ===========================================================================
echo
echo "############################################################"
echo "# REVERSE scenario: zig publishes (signed) -> go receives"
echo "############################################################"

GO_ADDR_FILE="$(mktmp)"
GO_LOG2="$(mktmp)"
ZIG_LOG2="$(mktmp)"
: > "$GO_ADDR_FILE"

echo "==> Starting GO node (role=listen mode=sub, StrictSign) on port 4112"
GOLOG_LOG_LEVEL="${GOLOG_LOG_LEVEL:-error}" "$GO_BIN" \
    role=listen mode=sub \
    port=4112 \
    "topic=$TOPIC" \
    "addr_file=$GO_ADDR_FILE" \
    duration_ms=12000 \
    > "$GO_LOG2" 2>&1 &
GO_PID2=$!
PIDS+=("$GO_PID2")

echo "==> Waiting for go listener address"
GO_ADDR="$(wait_for_addr "$GO_ADDR_FILE" "$GO_PID2")"
if [ -z "$GO_ADDR" ]; then
    echo "FAIL (REVERSE): go listener never published its address" >&2
    cat "$GO_LOG2" >&2
    OVERALL=1
else
    echo "    go address: $GO_ADDR"
    echo "==> Starting ZIG node (role=dial mode=pub) -> go"
    "$ZIG_BIN" \
        role=dial mode=pub \
        "peer=$GO_ADDR" \
        "topic=$TOPIC" \
        "message=hello-from-zig" \
        duration_ms=10000 \
        > "$ZIG_LOG2" 2>&1 &
    ZIG_PID2=$!
    PIDS+=("$ZIG_PID2")

    wait "$ZIG_PID2" 2>/dev/null
    wait "$GO_PID2" 2>/dev/null

    echo
    echo "--- go (subscriber) output ---"
    cat "$GO_LOG2"
    echo "--- zig (publisher) output ---"
    cat "$ZIG_LOG2"
    echo

    if grep -q "RECV topic=$TOPIC.*data=hello-from-zig" "$GO_LOG2" \
       && grep -q '"role":"listen".*"received":true' "$GO_LOG2"; then
        echo "PASS (REVERSE): go-libp2p (StrictSign) accepted the zig-published signed message"
    else
        echo "FAIL (REVERSE): go did NOT receive the zig signed message" >&2
        OVERALL=1
    fi
fi

echo
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (both directions: go<->zig signed message receipt verified)"
else
    echo "OVERALL: FAIL (a direction did not succeed)"
fi
exit "$OVERALL"
