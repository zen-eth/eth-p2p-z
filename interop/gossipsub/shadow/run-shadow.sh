#!/usr/bin/env bash
# Run the 2-host zig QUIC gossipsub simulation under Shadow and print a verdict.
#
# Designed to run INSIDE the image built from this directory's Dockerfile (Shadow
# on $PATH, the node at /usr/local/bin, the scenario at /sim/shadow.yaml), but it
# also works on any Linux host with `shadow` on PATH if you point SHADOW_YAML at a
# copy and have the node binary at the path the YAML names.
#
# Success = node0 (the subscriber) logged the published message AND its result
# JSON says received:true. That proves the QUIC handshake, /meshsub negotiation,
# and gossipsub propagation all ran over Shadow's emulated UDP path on the zio
# epoll runtime — the gating unknown for running this stack under Shadow.

set -u

SHADOW_YAML="${SHADOW_YAML:-/sim/shadow.yaml}"
DATA_DIR="${SHADOW_DATA_DIR:-/sim/shadow.data}"
TOPIC="${TOPIC:-interop-test}"
MESSAGE="${MESSAGE:-hello-shadow}"

# A stale addr file from a previous run would make node1 dial a dead listener.
rm -f /tmp/node0.addr
# Shadow refuses to overwrite an existing data dir.
rm -rf "$DATA_DIR"

echo "==> Shadow version"
shadow --version || { echo "FAIL: shadow not found on PATH" >&2; exit 1; }

echo "==> Running Shadow simulation ($SHADOW_YAML)"
# Shadow writes each process's stdout/stderr under <data>/hosts/<host>/.
shadow --progress true -d "$DATA_DIR" "$SHADOW_YAML"
SHADOW_RC=$?
if [ "$SHADOW_RC" -ne 0 ]; then
    echo "FAIL: shadow exited with code $SHADOW_RC" >&2
fi

# node0's stdout file is named <exe-basename>.<pid>.stdout under its host dir.
NODE0_OUT="$(find "$DATA_DIR/hosts/node0" -name '*.stdout' 2>/dev/null | head -n1)"
NODE1_OUT="$(find "$DATA_DIR/hosts/node1" -name '*.stdout' 2>/dev/null | head -n1)"

echo
echo "--- node0 (listener/subscriber) stdout ---"
[ -n "$NODE0_OUT" ] && cat "$NODE0_OUT" || echo "(no node0 stdout found)"
echo "--- node1 (dialer/publisher) stdout ---"
[ -n "$NODE1_OUT" ] && cat "$NODE1_OUT" || echo "(no node1 stdout found)"
echo

echo "==> Verdict"
if [ -n "$NODE0_OUT" ] \
   && grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "$NODE0_OUT" \
   && grep -q '"role":"listen".*"received":true' "$NODE0_OUT"; then
    echo "PASS: node0 received the gossipsub message over QUIC under Shadow"
    echo "      => zio (epoll) + QUIC run under Shadow."
    exit 0
fi

echo "FAIL: node0 did not receive the published message under Shadow" >&2
exit 1
