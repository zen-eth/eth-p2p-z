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
// sign=nosign (alias anonymous) to switch to StrictNoSign with a
// content-addressed message-id (sha256 of topic||data) so an all-anonymous
// mixed mesh dedups consistently with the zig node, whose anonymous policy uses
// the same sha256(topic ++ data) id.
//
// Set protocols=meshsub/1.1.0 to advertise ONLY gossipsub 1.1.0 (instead of the
// default 1.2.0+1.1.0+1.0.0 set) so a version-fallback scenario can confirm a
// 1.2-capable peer negotiates down to 1.1.0 with this node.
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
	"github.com/libp2p/go-libp2p/core/protocol"
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
	peerFile   string // poll this file for a peer multiaddr (Shadow shared-FS coordination)
	addrFile   string
	topic      string
	message    string
	durationMs int
	sign       string // strict (default) | nosign
	protocols  string // empty = default (1.2/1.1/1.0); "meshsub/1.1.0" = advertise only 1.1.0
	px         bool   // peer-exchange on PRUNE (WithPeerExchange); default off
	dHigh      int    // mesh upper bound (Dhi); 0 = default. Lower it so a tiny topology over-degree-prunes WITH PX.
	d          int    // mesh target degree (D); 0 = default
	dLow       int    // mesh lower bound (Dlo); 0 = default
	announce   string // when set, bind 0.0.0.0 and advertise this IP (Shadow assigns one IP per host)
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
		case "peer_file":
			a.peerFile = val
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
		case "protocols":
			a.protocols = val
		case "px":
			a.px = val == "on" || val == "true"
		case "d_high":
			fmt.Sscanf(val, "%d", &a.dHigh)
		case "d":
			fmt.Sscanf(val, "%d", &a.d)
		case "d_low":
			fmt.Sscanf(val, "%d", &a.dLow)
		case "announce":
			a.announce = val
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
	if a.role == "dial" && a.peer == "" && a.peers == "" && a.peerFile == "" {
		fatalf("dial role requires peer=<multiaddr>, peers=<ma1>,<ma2>,... or peer_file=<path>")
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
		// A listener may ALSO dial upstream peers (peers=/peer=/peer_file=): it is
		// then both reachable (bound + advertised) AND a member of the mesh it
		// dials into. The testground plan launches subscribers as role=listen with
		// peers=<publisher> (so they advertise a data-net address yet still connect
		// outbound to the publisher), matching the zig node whose listener also
		// dials. Without this a go subscriber would bind and wait to be dialed, but
		// the publisher (also role=listen) never dials it, so no connection forms
		// and nothing is delivered. Dials are optional — a pure bootstrap (no
		// peers=) dials nothing.
		if a.peer != "" || a.peers != "" || a.peerFile != "" {
			runDialer(ctx, h, a)
		}
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
			// `author` is the message's signed source peer-id; under StrictNoSign
			// it is absent (empty), which the anonymous scenario asserts. `from` is
			// the immediate relayer (always present). Print both so a scenario can
			// distinguish "no author" from "no propagation source".
			author := ""
			if id := msg.GetFrom(); len(id) > 0 {
				author = id.String()
			}
			fmt.Printf("RECV topic=%s author=%s from=%s data=%s\n", a.topic, author, msg.ReceivedFrom, string(msg.Data))
		}
	}()

	// Publisher: republish every 500ms so at least one publish lands after the
	// mesh forms (matching the zig node's strategy). Under anonymous the
	// message-id is content-derived sha256(topic||data), so an identical payload
	// would be suppressed by the seen-cache after the first publish (which fires
	// before the mesh grafts); append a #<n> suffix so each publish is unique and
	// propagates once the mesh is up. A subscriber asserts on the message prefix.
	anon := a.sign == "nosign" || a.sign == "anonymous"
	payload := func(n int) []byte {
		if anon {
			return []byte(fmt.Sprintf("%s#%d", a.message, n))
		}
		return []byte(a.message)
	}
	deadline := time.After(time.Duration(a.durationMs) * time.Millisecond)
	if a.mode == "pub" {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		n := 0
		// Publish once immediately, then on each tick.
		_ = topic.Publish(ctx, payload(n))
		n++
	loop:
		for {
			select {
			case <-deadline:
				break loop
			case <-ticker.C:
				if err := topic.Publish(ctx, payload(n)); err != nil {
					fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
				}
				n++
			}
		}
	} else {
		<-deadline
	}

	cancel()
	<-done

	fmt.Printf("{\"role\":\"%s\",\"mode\":\"%s\",\"received\":%t}\n", a.role, a.mode, received)
	// Hard-exit after the final result line. The deferred h.Close() would otherwise
	// run a graceful go-libp2p/quic-go shutdown that lingers ~2s on CONNECTION_CLOSE,
	// making this node exit well after the zig/rust peers (which hard-exit). Under
	// testground that lag desyncs the per-instance teardown and the run client can
	// report "canceled" instead of success. We are done here, so exit immediately.
	os.Exit(0)
}

func newHost(a args) host.Host {
	// Generate a fresh Ed25519 identity each run (the peer id is advertised via
	// the multiaddr, so it need not be fixed).
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		fatalf("generate key: %v", err)
	}
	// Default: loopback (multi-node localhost runs). Under Shadow each host has a
	// single assigned IP and no loopback aliasing across hosts, so announce=<ip>
	// binds 0.0.0.0 (accept on the host IP) and the listener advertises <ip>.
	bindIP := "127.0.0.1"
	if a.announce != "" {
		bindIP = "0.0.0.0"
	}
	listen := fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", bindIP, a.port)
	if a.role == "dial" {
		// A dialer still needs a transport-bound host; bind an ephemeral port.
		listen = fmt.Sprintf("/ip4/%s/udp/0/quic-v1", bindIP)
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
	if a.sign == "nosign" || a.sign == "anonymous" {
		// StrictNoSign / anonymous mesh: accept unsigned messages and key dedup on
		// a content-addressed message-id. The id MUST match every other impl in the
		// topic. The zig node's anonymous policy uses sha256(topic ++ data), so this
		// hashes topic then data in the same order. (go's signed default keys on
		// from++seqno, which an anonymous node cannot reproduce, so a content id is
		// mandatory here.)
		// WithMessageSignaturePolicy(StrictNoSign) disables signing + verification
		// but, on its own, leaves signID = host.ID() so go still attaches the
		// author (From) + seqno on publish. WithNoAuthor clears signID so the
		// published message truly carries no author/seqno — matching the zig and
		// rust anonymous wire format the scenario asserts.
		opts = append(opts,
			pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
			pubsub.WithNoAuthor(),
			pubsub.WithMessageIdFn(func(m *pubsubpb.Message) string {
				hsh := sha256.New()
				hsh.Write([]byte(m.GetTopic()))
				hsh.Write(m.Data)
				return string(hsh.Sum(nil))
			}),
		)
	}
	if a.protocols == "meshsub/1.1.0" {
		// Advertise ONLY gossipsub 1.1.0 (not the default 1.2/1.1/1.0 set) so a
		// 1.2-capable peer must negotiate down to 1.1.0 to talk to us. Use the
		// default feature test restricted to the single id so mesh/gossip features
		// still light up for 1.1.0.
		only11 := []protocol.ID{pubsub.GossipSubID_v11}
		feat := func(feat pubsub.GossipSubFeature, proto protocol.ID) bool {
			return proto == pubsub.GossipSubID_v11
		}
		opts = append(opts, pubsub.WithGossipSubProtocols(only11, feat))
	}
	if a.px {
		// Peer-exchange: when this node over-degree-prunes a mesh peer, the PRUNE
		// carries up to PrunePeers other mesh peers as offers, each with the offered
		// peer's SIGNED peer record drawn from the certified addr book (which
		// go-libp2p populates automatically from each peer's identify signedPeerRecord).
		// A peer that accepts the PX (its score clears AcceptPXThreshold — with no
		// scoring the score is 0 and the default threshold is 0, so 0 < 0 is false and
		// PX is accepted) verifies the records and dials the offered peers.
		opts = append(opts, pubsub.WithPeerExchange(true))
	}
	if a.dHigh != 0 || a.d != 0 || a.dLow != 0 {
		// Override the mesh DEGREE so a small topology goes over-degree and the
		// heartbeat prunes WITH PX. go prunes the mesh down to D when it has >= Dhi
		// peers, keeping Dscore peers by score and Dout outbound peers; with scoring
		// off and a tiny mesh those selection sizes must not exceed the mesh, so set
		// Dscore=0 and Dout=0 (the over-degree prune slices plst[Dscore:] and the
		// outbound bubble-up runs over plst[D:], both of which would panic if Dscore
		// or D exceeded the peer list). Each degree field falls back to the default.
		params := pubsub.DefaultGossipSubParams()
		if a.d != 0 {
			params.D = a.d
		}
		if a.dLow != 0 {
			params.Dlo = a.dLow
		}
		if a.dHigh != 0 {
			params.Dhi = a.dHigh
		}
		params.Dscore = 0
		params.Dout = 0
		opts = append(opts, pubsub.WithGossipSubParams(params))
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
	if a.announce != "" {
		// Shadow: bind is 0.0.0.0, so h.Addrs() reports the unspecified address.
		// Advertise the assigned host IP on the configured port instead.
		ann, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", a.announce, a.port))
		if err != nil {
			fatalf("build announce multiaddr: %v", err)
		}
		full = ann.Encapsulate(p2pComponent)
	} else {
		for _, addr := range h.Addrs() {
			// Prefer the loopback quic-v1 address.
			if strings.Contains(addr.String(), "127.0.0.1") && strings.Contains(addr.String(), "quic-v1") {
				full = addr.Encapsulate(p2pComponent)
				break
			}
		}
	}
	if full == nil {
		fatalf("no quic-v1 listen address found in %v", h.Addrs())
	}
	fmt.Printf("listener advertising %s\n", full)
	if a.addrFile != "" {
		if err := os.WriteFile(a.addrFile, []byte(full.String()+"\n"), 0o644); err != nil {
			fatalf("write addr file: %v", err)
		}
	}
}

// readPeerFile polls path until it holds a non-empty first line (a peer
// multiaddr), returning it. Under Shadow the bootstrap writes its addr to a
// shared-FS file after binding; a dialer that starts concurrently polls for it.
func readPeerFile(ctx context.Context, path string) (string, error) {
	for {
		if data, err := os.ReadFile(path); err == nil {
			if line := strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0]); line != "" {
				return line, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("peer_file %q never became readable", path)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func runDialer(ctx context.Context, h host.Host, a args) {
	// If a peer_file was given, poll it for the bootstrap multiaddr and fold it
	// into the dial set (Shadow shared-FS coordination, mirroring the zig node).
	if a.peerFile != "" {
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		addr, err := readPeerFile(waitCtx, a.peerFile)
		cancel()
		if err != nil {
			fatalf("peer_file: %v", err)
		}
		if a.peers == "" {
			a.peers = addr
		} else {
			a.peers = a.peers + "," + addr
		}
	}

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
