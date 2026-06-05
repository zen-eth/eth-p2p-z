// A minimal go-libp2p gossipsub peer for cross-implementation interop smoke
// testing against the zig node. It mirrors the zig interop binary's argument
// convention (key=value): role=listen|dial, mode=pub|sub, port=, peer=,
// addr_file=, topic=, message=, duration_ms=. A listener writes its full
// /p2p/<peer-id> multiaddr to addr_file once bound; a dialer reads peer= and
// connects. Both subscribe to the topic; a publisher publishes the message
// every 500ms (mesh-formation tolerant, matching the zig node); a subscriber
// prints "RECV ..." on receipt and a final {"received":..} JSON line.
//
// Signing policy defaults to StrictSign (go-libp2p's default): the publisher
// signs every message and the subscriber rejects unsigned inbound. Set
// sign=nosign to switch to StrictNoSign with a content-addressed message-id
// (sha256 of the data) for the reverse-direction diagnostic, where the zig
// node publishes unsigned.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/multiformats/go-multiaddr"
)

type args struct {
	role       string // listen | dial
	mode       string // pub | sub
	port       string
	peer       string // a single peer multiaddr (kept for two-node runs)
	peers      string // comma-separated peer multiaddrs for mesh runs
	addrFile   string
	topic      string
	message    string
	durationMs int
	sign       string // strict (default) | nosign
}

// dialTargets returns every peer multiaddr to dial: each comma-separated entry
// of peers plus a lone peer if set. Blank entries are skipped (a trailing comma
// is fine).
func (a args) dialTargets() []string {
	var out []string
	for _, p := range strings.Split(a.peers, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	if t := strings.TrimSpace(a.peer); t != "" {
		out = append(out, t)
	}
	return out
}

func parseArgs() args {
	a := args{
		port:       "4101",
		topic:      "interop-test",
		message:    "hello-from-go",
		durationMs: 12000,
		sign:       "strict",
	}
	for _, arg := range os.Args[1:] {
		eq := strings.IndexByte(arg, '=')
		if eq < 0 {
			fatalf("expected key=value argument, got %q", arg)
		}
		key, val := arg[:eq], arg[eq+1:]
		switch key {
		case "role":
			a.role = val
		case "mode":
			a.mode = val
		case "port":
			a.port = val
		case "peer":
			a.peer = val
		case "peers":
			a.peers = val
		case "addr_file":
			a.addrFile = val
		case "topic":
			a.topic = val
		case "message":
			a.message = val
		case "duration_ms":
			fmt.Sscanf(val, "%d", &a.durationMs)
		case "sign":
			a.sign = val
		default:
			fatalf("unknown argument key %q", key)
		}
	}
	if a.role != "listen" && a.role != "dial" {
		fatalf("role must be listen|dial, got %q", a.role)
	}
	if a.mode != "pub" && a.mode != "sub" {
		fatalf("mode must be pub|sub, got %q", a.mode)
	}
	if a.role == "dial" && a.peer == "" && a.peers == "" {
		fatalf("dial role requires peer=<multiaddr> or peers=<ma1>,<ma2>,...")
	}
	return a
}

func main() {
	a := parseArgs()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(a.durationMs)*time.Millisecond+5*time.Second)
	defer cancel()

	h := newHost(a)
	defer h.Close()

	gs := newGossipSub(ctx, h, a)

	topic, err := gs.Join(a.topic)
	if err != nil {
		fatalf("join topic: %v", err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		fatalf("subscribe: %v", err)
	}

	switch a.role {
	case "listen":
		runListener(h, a)
	case "dial":
		runDialer(ctx, h, a)
	}

	received := false
	done := make(chan struct{})

	// Reader goroutine: print every received message (skipping our own) and
	// record that at least one arrived.
	go func() {
		defer close(done)
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return // context done / subscription cancelled
			}
			if msg.ReceivedFrom == h.ID() {
				continue // ignore our own republished messages
			}
			received = true
			fmt.Printf("RECV topic=%s from=%s data=%s\n", a.topic, msg.ReceivedFrom, string(msg.Data))
		}
	}()

	// Publisher: republish every 500ms so at least one publish lands after the
	// mesh forms (matching the zig node's strategy).
	deadline := time.After(time.Duration(a.durationMs) * time.Millisecond)
	if a.mode == "pub" {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		// Publish once immediately, then on each tick.
		_ = topic.Publish(ctx, []byte(a.message))
	loop:
		for {
			select {
			case <-deadline:
				break loop
			case <-ticker.C:
				if err := topic.Publish(ctx, []byte(a.message)); err != nil {
					fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
				}
			}
		}
	} else {
		<-deadline
	}

	cancel()
	<-done

	fmt.Printf("{\"role\":\"%s\",\"mode\":\"%s\",\"received\":%t}\n", a.role, a.mode, received)
}

func newHost(a args) host.Host {
	// Generate a fresh Ed25519 identity each run (the peer id is advertised via
	// the multiaddr, so it need not be fixed).
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		fatalf("generate key: %v", err)
	}
	listen := fmt.Sprintf("/ip4/127.0.0.1/udp/%s/quic-v1", a.port)
	if a.role == "dial" {
		// A dialer still needs a transport-bound host; bind an ephemeral port.
		listen = "/ip4/127.0.0.1/udp/0/quic-v1"
	}
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ListenAddrStrings(listen),
	)
	if err != nil {
		fatalf("new host: %v", err)
	}
	return h
}

func newGossipSub(ctx context.Context, h host.Host, a args) *pubsub.PubSub {
	opts := []pubsub.Option{}
	if a.sign == "nosign" {
		// Reverse-direction diagnostic: accept unsigned messages and use a
		// content-addressed message-id (the zig node publishes unsigned, with a
		// from++seqno id we cannot reproduce without signing).
		opts = append(opts,
			pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
			pubsub.WithMessageIdFn(func(m *pubsubpb.Message) string {
				h := sha256.Sum256(m.Data)
				return string(h[:])
			}),
		)
	}
	gs, err := pubsub.NewGossipSub(ctx, h, opts...)
	if err != nil {
		fatalf("new gossipsub: %v", err)
	}
	return gs
}

func runListener(h host.Host, a args) {
	// Build the full /ip4/.../udp/<port>/quic-v1/p2p/<peer-id> multiaddr.
	p2pComponent, err := multiaddr.NewComponent("p2p", h.ID().String())
	if err != nil {
		fatalf("p2p component: %v", err)
	}
	var full multiaddr.Multiaddr
	for _, addr := range h.Addrs() {
		// Prefer the loopback quic-v1 address.
		if strings.Contains(addr.String(), "127.0.0.1") && strings.Contains(addr.String(), "quic-v1") {
			full = addr.Encapsulate(p2pComponent)
			break
		}
	}
	if full == nil {
		fatalf("no loopback quic-v1 listen address found in %v", h.Addrs())
	}
	fmt.Printf("listener advertising %s\n", full)
	if a.addrFile != "" {
		if err := os.WriteFile(a.addrFile, []byte(full.String()+"\n"), 0o644); err != nil {
			fatalf("write addr file: %v", err)
		}
	}
}

func runDialer(ctx context.Context, h host.Host, a args) {
	// Connect to every configured peer. gossipsub forms a mesh over whatever
	// connections exist, so a node that dials several peers can graft into a
	// mesh spanning all of them. A failed dial to one peer does not abort the
	// others — as long as at least one succeeds the node can still mesh.
	dialed := 0
	for _, peerText := range a.dialTargets() {
		maddr, err := multiaddr.NewMultiaddr(peerText)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse peer multiaddr %q: %v\n", peerText, err)
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "addr info from %q: %v\n", peerText, err)
			continue
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
		err = h.Connect(dialCtx, *info)
		dialCancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect to %s: %v\n", info.ID, err)
			continue
		}
		fmt.Printf("dialer connected to %s\n", info.ID)
		dialed++
	}
	if dialed == 0 {
		fatalf("dial role: no peers connected")
	}
}

// seqnoBytes is unused but documents the wire shape go uses for seqno (a u64);
// kept for reference parity with the zig node's big-endian seqno.
func seqnoBytes(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
