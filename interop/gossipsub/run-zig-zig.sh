#!/usr/bin/env bash
# Zig<->zig gossipsub smoke run: build the interop node binary, start a LISTENER
# (subscriber) and a DIALER (publisher) as separate processes talking over real
# loopback QUIC, and assert the listener received the published message.
#
# Proves the gossipsub node binary works end-to-end out-of-process before we add
# a cross-implementation (go-libp2p) peer.

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT_DIR/zig-out/bin/libp2p-gossipsub-interop"

TOPIC="interop-test"
MESSAGE="hello-gossipsub"
PORT=4101
ADDR_FILE="$(mktemp -t gs_addr.XXXXXX)"
LISTEN_LOG="$(mktemp -t gs_listen.XXXXXX)"
DIAL_LOG="$(mktemp -t gs_dial.XXXXXX)"

LISTEN_PID=""
DIAL_PID=""

cleanup() {
    [ -n "$LISTEN_PID" ] && kill "$LISTEN_PID" 2>/dev/null
    [ -n "$DIAL_PID" ] && kill "$DIAL_PID" 2>/dev/null
    wait 2>/dev/null
    rm -f "$ADDR_FILE" "$LISTEN_LOG" "$DIAL_LOG"
}
trap cleanup EXIT

echo "==> Building gossipsub interop binary"
( cd "$ROOT_DIR" && zig build install )
if [ ! -x "$BIN" ]; then
    echo "FAIL: binary not found at $BIN" >&2
    exit 1
fi

# The listener writes its full multiaddr to ADDR_FILE once bound; clear it so we
# can wait for a fresh write.
: > "$ADDR_FILE"

echo "==> Starting LISTENER (mode=sub, port=$PORT)"
"$BIN" \
    role=listen mode=sub \
    "port=$PORT" \
    "topic=$TOPIC" \
    "addr_file=$ADDR_FILE" \
    duration_ms=8000 \
    > "$LISTEN_LOG" 2>&1 &
LISTEN_PID=$!

echo "==> Waiting for listener to publish its address"
WAITED=0
ADDR=""
while [ "$WAITED" -lt 100 ]; do
    if [ -s "$ADDR_FILE" ]; then
        ADDR="$(head -n1 "$ADDR_FILE")"
        [ -n "$ADDR" ] && break
    fi
    if ! kill -0 "$LISTEN_PID" 2>/dev/null; then
        echo "FAIL: listener exited before publishing its address" >&2
        echo "--- listener log ---" >&2
        cat "$LISTEN_LOG" >&2
        exit 1
    fi
    sleep 0.1
    WAITED=$((WAITED + 1))
done

if [ -z "$ADDR" ]; then
    echo "FAIL: timed out waiting for listener address" >&2
    cat "$LISTEN_LOG" >&2
    exit 1
fi
echo "    listener address: $ADDR"

echo "==> Starting DIALER (mode=pub, message=$MESSAGE)"
"$BIN" \
    role=dial mode=pub \
    "peer=$ADDR" \
    "topic=$TOPIC" \
    "message=$MESSAGE" \
    duration_ms=6000 \
    > "$DIAL_LOG" 2>&1 &
DIAL_PID=$!

echo "==> Waiting for both nodes to finish"
wait "$DIAL_PID"
wait "$LISTEN_PID"
LISTEN_PID=""
DIAL_PID=""

echo
echo "--- listener output ---"
cat "$LISTEN_LOG"
echo "--- dialer output ---"
cat "$DIAL_LOG"
echo

if grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "$LISTEN_LOG" \
   && grep -q '"role":"listen".*"received":true' "$LISTEN_LOG"; then
    echo "PASS: listener received the published message over QUIC"
    exit 0
fi

echo "FAIL: listener did not receive the published message" >&2
exit 1
