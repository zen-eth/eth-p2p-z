# testground env patches

Running the cross-impl `gossipsub-quic` matrix (`--instances 3+`, mixed `impls=`)
surfaced a bug in the **testground sync client (sdk-go)** that crashes the
testground **daemon** at run teardown. This dir carries the fix as a standalone
patch so the matrix can run, and so it can be contributed upstream.

This is a **testground/sdk-go platform bug, not an eth-p2p-z bug.** The eth-p2p-z
nodes (zig/go/rust) deliver gossipsub correctly; the daemon just crashes while
finalizing the run.

## `sdk-go-responsesworker-panic.patch`

### Symptom
A multi-instance run (reliably with 3+ instances) ends with:

```
panic: send on closed channel
goroutine ... github.com/testground/sdk-go/sync.(*DefaultClient).responsesWorker
    .../sync/client_conn.go:43
```

in the **daemon** log, right after `all containers are complete` / `deleting
containers`. The daemon process dies, so every subsequent run fails with
`Post "http://localhost:8042/run": dial tcp [::1]:8042: connect: connection
refused`, and `testground run` reports `outcome ... canceled`.

### Root cause (a close-vs-send race in the sync client)
`sync/client_conn.go`:

- `responsesWorker()` reads the per-request handler channel **under**
  `handlersMu`, **releases the lock**, then sends `ch <- res` **unlocked**
  (line 43).
- `makeRequest()`'s ctx-cancellation goroutine, on teardown, takes `handlersMu`,
  `close(c.handlers[req.ID])`, `delete(...)`, unlocks (lines ~79–82).

If a late response arrives for a request whose handler was just closed at
teardown, `responsesWorker` sends on a now-closed channel → `panic`. The race is
probabilistic (more instances / more sync traffic → reliably triggered); a
2-instance run often dodges it, a 3-instance cross-impl run hits it every time.

### The fix
Wrap the racy send in `recover()` and drop the late response — the request
handler is already gone, so the response is unwanted:

```go
} else {
    func() {
        defer func() { _ = recover() }()
        ch <- res
    }()
}
```

(A cleaner long-term fix would send under the lock with a non-blocking select,
or signal completion instead of closing the handler channel; `recover` is the
minimal, safe change.)

### How to apply (to run the matrix locally)
sdk-go is a Go module dependency of the testground daemon, so patch a writable
copy and `replace` it, then rebuild the daemon:

```sh
V=v0.3.1-0.20220525111316-b6b10897b578   # the testground-quic platform's sdk-go pin
SDK=$(go env GOPATH)/pkg/mod/github.com/testground/sdk-go@$V
cp -r "$SDK" ~/sdk-go-patched && chmod -R u+w ~/sdk-go-patched
git apply --directory ~/sdk-go-patched sdk-go-responsesworker-panic.patch   # or patch -p1 -d ~/sdk-go-patched < ...

cd <testground-quic checkout>
go mod edit -replace github.com/testground/sdk-go=$HOME/sdk-go-patched
make goinstall            # rebuild the daemon with the patched client
# restart `testground daemon`, then run the matrix
```

After this, all three directions pass `outcome = success (single:3/3)`.

### Upstream
Candidate PR against `github.com/testground/sdk-go` (`sync/client_conn.go`,
`responsesWorker`). The same race exists on `master`. The testground-quic fork
could also carry it via a `replace`/vendor until upstream merges.
