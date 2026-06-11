#!/usr/bin/env bash
# Import this plan into Testground using the "repo-root as build context" layout.
#
# docker:generic builds from the IMPORTED plan tree, and our Zig stage needs the
# whole eth-p2p-z source — so we import the REPO ROOT and point the manifest
# `path` at this Go-plan subdir. `testground plan import` requires the manifest
# at the import root.
#
# The staged repo-root manifest.toml is PERMANENT, not cleanup-able:
# `testground plan import --from` SYMLINKS $TESTGROUND_HOME/plans/<name> to the
# --from directory (it does not copy), so every later build/run resolves the
# manifest through the live repo root — deleting it breaks the plan with
# "failed to access plan manifest". It is gitignored (/manifest.toml) so it
# never shows up as repo noise.
#
# Prereqs: a running `testground daemon`, and `go mod tidy` already run here
# (so go.sum exists). Usage: bash interop/gossipsub/testground/import.sh
set -euo pipefail

PLAN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLAN_DIR/../../.." && pwd)"
NAME="gossipsub-quic"

[ -f "$PLAN_DIR/go.sum" ] || { echo "FAIL: $PLAN_DIR/go.sum missing — run 'cd $PLAN_DIR && go mod tidy' first"; exit 1; }

cp "$PLAN_DIR/manifest.toml" "$REPO_ROOT/manifest.toml"

echo "==> importing repo root as plan '$NAME' (manifest path=interop/gossipsub/testground)"
testground plan import --from "$REPO_ROOT" --name "$NAME"

# The plan is a symlink to the repo root; verify it resolves the tree the
# docker:generic build will pack (Zig source + Go plan + the manifest).
TGHOME="${TESTGROUND_HOME:-$HOME/testground}"
[ -d "$TGHOME/plans" ] || TGHOME="$HOME/.config/testground"
echo "==> verifying imported layout under $TGHOME/plans/$NAME"
for f in manifest.toml build.zig interop/gossipsub/testground/main.go; do
    ls "$TGHOME/plans/$NAME/$f" >/dev/null 2>&1 \
        && echo "    ok: $f present" \
        || echo "    WARN: $f not found — check $TGHOME/plans/$NAME"
done

echo "==> imported. Next:"
echo "    testground build single --plan $NAME --builder docker:generic --wait"
echo "    testground run single --plan $NAME --testcase publish-deliver \\"
echo "        --builder docker:generic --runner local:docker --instances 2 --wait"
