#!/usr/bin/env bash
# Scenario B — VERSION FALLBACK (multi-version negotiation).
#
# Our zig node speaks /meshsub/1.2.0, 1.1.0 and 1.0.0 (advertised newest-first
# via identify; the dialer proposes them best-first and the listener registers
# an inbound handler for each). A go (or rust) peer restricted to ONLY
# /meshsub/1.1.0 must therefore meet our node at 1.1.0 rather than the 1.2.0 we
# default to with an unrestricted peer.
#
# The peer is restricted with protocols=meshsub/1.1.0:
#   go  : pubsub.WithGossipSubProtocols([]{GossipSubID_v11}, <1.1-only feature test>)
#   rust: ConfigBuilder::protocol_id("/meshsub/1.1.0", Version::V1_1)
#
# Topology: our zig node listens (1.2-capable). A 1.1-only peer dials it and
# publishes. We assert, from the zig node's own logs, that BOTH its inbound and
# outbound /meshsub streams with that peer negotiated 1.1.0 (NOT 1.2.0), AND
# that pub/sub still works (the zig node receives the peer's published payload).
# The run is repeated for a 1.1-only go peer and a 1.1-only rust peer.

set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TOPIC="interop-version-fallback"
MESSAGE="hello-v1-1-only"
BASE_PORT=4210
# The peer's gossipsub grafts our node into its mesh on its heartbeat (the go and
# rust peers heartbeat once per second), and the rust 1.1-only peer in particular
# can take several heartbeats after connect+identify+subscribe before the 1.1
# mesh stabilizes and a publish actually propagates. The publisher republishes
# every 500ms for the whole run, so a generous duration guarantees many publish
# attempts land AFTER the mesh has formed — making receipt reliable rather than
# racing a short window. 30s gives ~60 republish cycles, ample for the slowest
# (rust 1.1) mesh-formation path while keeping the scenario quick.
DURATION_MS=30000

build_all
install_cleanup

# run_one <peer_impl> -> 0 on PASS, 1 on FAIL.
# Stands up the zig node (1.2-capable) as a subscribing listener, then a
# 1.1-only peer that dials + publishes, and checks negotiation + receipt.
run_one() {
    local peer_impl="$1"
    local zlog plog addr zport pport zpid ppid
    zlog="$(mktmp)"; plog="$(mktmp)"; addr="$(mktmp)"; : > "$addr"
    zport=$BASE_PORT; pport=$((BASE_PORT + 1))
    BASE_PORT=$((BASE_PORT + 2))

    echo
    echo "############################################################"
    echo "# Version fallback: zig (1.2-capable) listener  vs  $peer_impl (1.1.0-only) publisher"
    echo "############################################################"

    echo "==> [zig] listen sub (advertises 1.2/1.1/1.0) port=$zport"
    "$ZIG_BIN" role=listen mode=sub "port=$zport" "topic=$TOPIC" \
        "addr_file=$addr" "duration_ms=$DURATION_MS" > "$zlog" 2>&1 &
    zpid=$!; track_pid "$zpid"

    addr="$(wait_for_addr "$addr" "$zpid")"
    if [ -z "$addr" ]; then
        echo "FAIL: zig listener never published its address" >&2
        cat "$zlog" >&2
        return 1
    fi
    echo "    zig address: $addr"

    echo "==> [$peer_impl] dial pub, RESTRICTED to /meshsub/1.1.0, port=$pport"
    "$(bin_for "$peer_impl")" role=dial mode=pub "port=$pport" "peers=$addr" \
        "topic=$TOPIC" "message=$MESSAGE" "duration_ms=$DURATION_MS" \
        protocols=meshsub/1.1.0 "addr_file=/dev/null" > "$plog" 2>&1 &
    ppid=$!; track_pid "$ppid"

    wait "$zpid" 2>/dev/null
    wait "$ppid" 2>/dev/null

    echo "--- zig log ---"; cat "$zlog"
    echo "--- $peer_impl log (tail) ---"; tail -3 "$plog"

    local ok=0
    # (1) Negotiation: both the inbound and outbound /meshsub streams the zig node
    #     formed with this peer must be 1.1.0 (and explicitly NOT 1.2.0).
    if grep -q "inbound stream negotiated v1_1" "$zlog" \
       && grep -q "outbound stream negotiated /meshsub/1.1.0" "$zlog"; then
        echo "    [PASS] zig negotiated /meshsub/1.1.0 (inbound v1_1 + outbound 1.1.0)"
    else
        echo "    [FAIL] zig did not negotiate 1.1.0 with the 1.1-only $peer_impl peer"
        ok=1
    fi
    if grep -q "negotiated /meshsub/1.2.0\|negotiated v1_2" "$zlog"; then
        echo "    [FAIL] zig negotiated 1.2.0 with a 1.1-only peer (should have fallen back)"
        ok=1
    fi
    # (2) Pub/sub still works across the 1.1.0 link.
    if grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "$zlog" \
       && grep -q '"received":true' "$zlog"; then
        echo "    [PASS] zig received the 1.1-only $peer_impl peer's published message over 1.1.0"
    else
        echo "    [FAIL] zig did not receive the published message over the 1.1.0 link"
        ok=1
    fi
    return $ok
}

OVERALL=0
run_one go   || OVERALL=1
run_one rust || OVERALL=1

echo
echo "============================================================"
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (our 1.2-capable node negotiated DOWN to /meshsub/1.1.0 with 1.1-only go AND rust peers, and pub/sub worked over the negotiated version)"
else
    echo "OVERALL: FAIL (version fallback to /meshsub/1.1.0 or pub/sub over it failed)"
fi
exit "$OVERALL"
