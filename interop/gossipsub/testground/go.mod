module github.com/eth-p2p-z/testground/plans/gossipsub-quic

go 1.19

require github.com/testground/sdk-go v0.3.1-0.20220525111316-b6b10897b578

// Run `go mod tidy` in this directory BEFORE building the image: it generates
// go.sum (the Dockerfile's `go mod download` needs it) and pulls the sdk-go
// transitive deps. This pseudo-version matches the testground-quic PLATFORM
// fork's own go.mod, so the sync/network wire protocol the daemon speaks lines
// up with what this plan links against.
