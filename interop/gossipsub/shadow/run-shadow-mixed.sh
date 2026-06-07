#!/usr/bin/env bash
# Run the MIXED go+zig gossipsub mesh under Shadow over QUIC and print a verdict.
#
# Topology (shadow-mixed.yaml): node0 zig bootstrap (listen+sub), node1 go (sub),
# node2 go (PUBLISH), node3 zig (sub). node2's payload must reach all three
# subscribers across the zig<->go boundary over QUIC under Shadow.
#
# PASS = every subscriber (node0, node1, node3) logged RECV of the payload AND
# its result JSON says received:true. The publisher (node2) is the source, so no
# receipt is expected from it.
#
# Designed to run INSIDE the image built from Dockerfile.arm.mixed (Shadow on
# PATH, both binaries in /usr/local/bin, the scenario at /sim/shadow-mixed.yaml).

set -u

SHADOW_YAML="${SHADOW_YAML:-/sim/shadow-mixed.yaml}"
DATA_DIR="${SHADOW_DATA_DIR:-/sim/shadow-mixed.data}"
TOPIC="${TOPIC:-interop-mixed}"
MESSAGE="${MESSAGE:-hello-mixed-shadow}"

# Subscribers that must receive the publish (node2 is the publisher/source).
SUBSCRIBERS=(node0 node1 node3)

# A stale addr file from a previous run would make dialers chase a dead listener.
rm -f /tmp/boot.addr
# Shadow refuses to overwrite an existing data dir.
rm -rf "$DATA_DIR"

echo "==> Shadow version"
shadow --version || { echo "FAIL: shadow not found on PATH" >&2; exit 1; }

echo "==> Running mixed go+zig Shadow simulation ($SHADOW_YAML)"
# Keep going past a non-zero Shadow exit so we can dump per-node logs + diagnose.
shadow --progress true -d "$DATA_DIR" "$SHADOW_YAML"
SHADOW_RC=$?
if [ "$SHADOW_RC" -ne 0 ]; then
    echo "NOTE: shadow exited with code $SHADOW_RC (a host failing its expected_final_state)" >&2
fi

# Per-host stdout/stderr live under <data>/hosts/<host>/.
node_out() { find "$DATA_DIR/hosts/$1" -name '*.stdout' 2>/dev/null | head -n1; }
node_err() { find "$DATA_DIR/hosts/$1" -name '*.stderr' 2>/dev/null | head -n1; }

for host in node0 node1 node2 node3; do
    echo
    echo "--- $host stdout ---"
    f="$(node_out "$host")"; [ -n "$f" ] && cat "$f" || echo "(no stdout)"
    echo "--- $host stderr (tail) ---"
    f="$(node_err "$host")"; [ -n "$f" ] && tail -n 20 "$f" || echo "(no stderr)"
done

# Syscall evidence: Shadow logs each unsupported syscall once at WARN. recvmmsg /
# sendmmsg or an ENOPROTOOPT setsockopt from the go (quic-go) hosts is the
# cross-impl blocker signature.
echo
echo "==> Shadow syscall diagnostics (unsupported-syscall + setsockopt warnings)"
grep -rhniE 'unsupported syscall|recvmmsg|sendmmsg|setsockopt called with unsupported|ENOPROTOOPT' \
    "$DATA_DIR" 2>/dev/null | sort -u | head -40 || echo "(none found in shadow logs)"

echo
echo "==> Verdict"
all_ok=1
for host in "${SUBSCRIBERS[@]}"; do
    out="$(node_out "$host")"
    if [ -n "$out" ] \
       && grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "$out" \
       && grep -q '"received":true' "$out"; then
        echo "  PASS: $host received the publish over QUIC under Shadow"
    else
        echo "  FAIL: $host did NOT receive the publish"
        all_ok=0
    fi
done

if [ "$all_ok" -eq 1 ]; then
    echo "PASS: cross-impl go+zig gossipsub propagated over QUIC under Shadow"
    exit 0
fi

echo "FAIL: mixed go+zig gossipsub did not fully propagate under Shadow" >&2
exit 1
