#!/usr/bin/env bash
# Import this plan into Testground using the "repo-root as build context" layout.
#
# docker:generic builds from the IMPORTED plan tree, and our Zig stage needs the
# whole eth-p2p-z source — so we import the REPO ROOT and point the manifest
# `path` at this Go-plan subdir. `testground plan import` requires the manifest
# at the import root, so this script stages it there for the import, then removes
# it again (the import has already packed a copy into $TESTGROUND_HOME/plans).
#
# Prereqs: a running `testground daemon`, and `go mod tidy` already run here
# (so go.sum exists). Usage: bash interop/gossipsub/testground/import.sh
set -euo pipefail

PLAN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLAN_DIR/../../.." && pwd)"
NAME="gossipsub-quic"

[ -f "$PLAN_DIR/go.sum" ] || { echo "FAIL: $PLAN_DIR/go.sum missing — run 'cd $PLAN_DIR && go mod tidy' first"; exit 1; }

STAGED="$REPO_ROOT/manifest.toml"
if [ -e "$STAGED" ]; then
    echo "FAIL: $STAGED already exists; refusing to overwrite. Remove it and re-run." >&2
    exit 1
fi
cp "$PLAN_DIR/manifest.toml" "$STAGED"
cleanup() { rm -f "$STAGED"; }
trap cleanup EXIT

echo "==> importing repo root as plan '$NAME' (manifest path=interop/gossipsub/testground)"
testground plan import --from "$REPO_ROOT" --name "$NAME"

# Verify the full subtree landed (Zig source + Go plan) before the first build.
TGHOME="${TESTGROUND_HOME:-$HOME/testground}"
echo "==> verifying imported layout under $TGHOME/plans/$NAME"
ls "$TGHOME/plans/$NAME/build.zig" >/dev/null 2>&1 \
    && echo "    ok: build.zig present (Zig source imported)" \
    || echo "    WARN: build.zig not found at expected path — check $TGHOME/plans/$NAME"
ls "$TGHOME/plans/$NAME/interop/gossipsub/testground/main.go" >/dev/null 2>&1 \
    && echo "    ok: main.go present (Go plan imported)" \
    || echo "    WARN: main.go not found at expected path"

echo "==> imported. Next:"
echo "    testground build single --plan $NAME --builder docker:generic --wait"
echo "    testground run single --plan $NAME --testcase publish-deliver \\"
echo "        --builder docker:generic --runner local:docker --instances 2 --wait"
