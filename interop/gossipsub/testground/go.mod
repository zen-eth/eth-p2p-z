module github.com/eth-p2p-z/testground/plans/gossipsub-quic

go 1.19

require github.com/testground/sdk-go v0.3.1-0.20220525111316-b6b10897b578

require (
	github.com/avast/retry-go v2.6.0+incompatible // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/influxdata/influxdb1-client v0.0.0-20200515024757-02f0bf5dbca3 // indirect
	github.com/klauspost/compress v1.10.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_golang v1.7.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/raulk/clock v1.1.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/testground/sync-service v0.1.0 // indirect
	github.com/testground/testground v0.5.3 // indirect
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	go.uber.org/zap v1.16.0 // indirect
	golang.org/x/sys v0.0.0-20200625212154-ddb9806d33ae // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	nhooyr.io/websocket v1.8.6 // indirect
)

// go.sum is committed (the Dockerfile's `go mod download` needs it), so no
// `go mod tidy` is required before building — only re-run it if you change deps.
// sdk-go is pinned to the testground-quic PLATFORM fork's own go.mod version, so
// the sync/network wire protocol the daemon speaks lines up with this plan.
// Verified: `go build` + `go vet` pass against the real sdk-go (2026-06-09).
