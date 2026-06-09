// A minimal rust-libp2p gossipsub peer for cross-implementation interop smoke
// testing against the zig node. It mirrors the zig interop binary's argument
// convention (key=value): role=listen|dial, mode=pub|sub, port=, peer=,
// addr_file=, topic=, message=, duration_ms=. A listener writes its full
// /p2p/<peer-id> multiaddr to addr_file once bound; a dialer reads peer= and
// connects. Both subscribe to the topic; a publisher publishes the message
// every 500ms (mesh-formation tolerant, matching the zig node); a subscriber
// prints "RECV ..." on receipt and a final {"received":..} JSON line.
//
// Signing policy defaults to MessageAuthenticity::Signed (rust-libp2p's
// StrictSign equivalent and the default for go-libp2p): the publisher signs
// every message and the subscriber rejects unsigned inbound. This matches the
// zig node, which also signs per the libp2p pubsub scheme, so both directions
// verify. Set sign=anonymous (alias nosign) for MessageAuthenticity::Anonymous
// with a content-addressed message-id sha256(topic||data), matching the zig
// node's anonymous policy so an all-anonymous mixed mesh dedups consistently.
//
// Set protocols=meshsub/1.1.0 to advertise ONLY gossipsub 1.1.0 so a
// version-fallback scenario can confirm a 1.2-capable peer negotiates down.
//
// Identify is included because rust-libp2p's gossipsub (like go's) only opens a
// /meshsub stream to a peer once it learns, via the /ipfs/id/1.0.0 exchange,
// that the peer speaks meshsub. Without identify the peers wire up over QUIC but
// never graft into each other's mesh.

use std::time::Duration;

use futures::StreamExt;
use libp2p::gossipsub::{self, IdentTopic, MessageAuthenticity};
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, identity, quic, Multiaddr, PeerId, Swarm, Transport};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::time::{interval, sleep, Instant};

const PUBLISH_INTERVAL: Duration = Duration::from_millis(500);

struct Args {
    role: String,    // listen | dial
    mode: String,    // pub | sub
    port: String,
    peer: Option<String>,  // a single peer multiaddr (kept for two-node runs)
    peers: Option<String>, // comma-separated peer multiaddrs for mesh runs
    addr_file: Option<String>,
    topic: String,
    message: String,
    duration_ms: u64,
    sign: String,      // strict (default) | anonymous (alias nosign)
    protocols: String, // empty = default (1.2/1.1/1.0); "meshsub/1.1.0" = only 1.1.0
    announce: Option<String>, // host IP to bind 0.0.0.0 for and advertise (Shadow/testground)
    px: bool,          // peer-exchange on PRUNE (do_px + prune_peers); default off
    d_high: usize,     // mesh upper bound (mesh_n_high); 0 = default
    d: usize,          // mesh target degree (mesh_n); 0 = default
    d_low: usize,      // mesh lower bound (mesh_n_low); 0 = default
}

impl Args {
    // Every peer multiaddr to dial: each comma-separated entry of `peers` plus a
    // lone `peer` if set. Blank entries are skipped (a trailing comma is fine).
    fn dial_targets(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(peers) = &self.peers {
            for p in peers.split(',') {
                let t = p.trim();
                if !t.is_empty() {
                    out.push(t.to_string());
                }
            }
        }
        if let Some(p) = &self.peer {
            let t = p.trim();
            if !t.is_empty() {
                out.push(t.to_string());
            }
        }
        out
    }
}

fn parse_args() -> Args {
    let mut a = Args {
        role: String::new(),
        mode: String::new(),
        port: "4101".into(),
        peer: None,
        peers: None,
        addr_file: None,
        topic: "interop-test".into(),
        message: "hello-from-rust".into(),
        duration_ms: 12000,
        sign: "strict".into(),
        protocols: String::new(),
        announce: None,
        px: false,
        d_high: 0,
        d: 0,
        d_low: 0,
    };
    for arg in std::env::args().skip(1) {
        let (key, val) = arg
            .split_once('=')
            .unwrap_or_else(|| fatal(&format!("expected key=value argument, got {arg:?}")));
        match key {
            "role" => a.role = val.into(),
            "mode" => a.mode = val.into(),
            "port" => a.port = val.into(),
            "peer" => a.peer = Some(val.into()),
            "peers" => a.peers = Some(val.into()),
            "addr_file" => a.addr_file = Some(val.into()),
            "topic" => a.topic = val.into(),
            "message" => a.message = val.into(),
            "duration_ms" => a.duration_ms = val.parse().unwrap_or(12000),
            "sign" => a.sign = val.into(),
            "protocols" => a.protocols = val.into(),
            "announce" => a.announce = Some(val.into()),
            "px" => a.px = val == "on" || val == "true",
            "d_high" => a.d_high = val.parse().unwrap_or(0),
            "d" => a.d = val.parse().unwrap_or(0),
            "d_low" => a.d_low = val.parse().unwrap_or(0),
            other => fatal(&format!("unknown argument key {other:?}")),
        }
    }
    if a.role != "listen" && a.role != "dial" {
        fatal(&format!("role must be listen|dial, got {:?}", a.role));
    }
    if a.mode != "pub" && a.mode != "sub" {
        fatal(&format!("mode must be pub|sub, got {:?}", a.mode));
    }
    if a.role == "dial" && a.peer.is_none() && a.peers.is_none() {
        fatal("dial role requires peer=<multiaddr> or peers=<ma1>,<ma2>,...");
    }
    a
}

// Combine gossipsub + identify so protocol discovery drives mesh formation.
#[derive(libp2p::swarm::NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
}

#[tokio::main]
async fn main() {
    let a = parse_args();

    // Fresh Ed25519 identity each run; the peer id is advertised via the
    // multiaddr, so it need not be fixed.
    let keypair = identity::Keypair::generate_ed25519();
    let local_peer = PeerId::from(keypair.public());

    let behaviour = build_behaviour(&keypair, &a);

    // QUIC v1 transport, mapped to the swarm's (PeerId, StreamMuxer) output.
    let quic_transport = quic::tokio::Transport::new(quic::Config::new(&keypair))
        .map(|(peer, conn), _| (peer, libp2p::core::muxing::StreamMuxerBox::new(conn)))
        .boxed();

    let mut swarm = Swarm::new(
        quic_transport,
        behaviour,
        local_peer,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(60)),
    );

    let topic = IdentTopic::new(&a.topic);
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic)
        .unwrap_or_else(|e| fatal(&format!("subscribe: {e}")));

    match a.role.as_str() {
        "listen" => {
            // Default: loopback (multi-node localhost runs). Under Shadow/testground
            // each host has a single assigned IP and no loopback aliasing across
            // hosts, so announce=<ip> binds 0.0.0.0 (accept on the host IP) and the
            // listener advertises <ip>.
            let bind_ip = if a.announce.is_some() {
                "0.0.0.0"
            } else {
                "127.0.0.1"
            };
            let listen: Multiaddr = format!("/ip4/{}/udp/{}/quic-v1", bind_ip, a.port)
                .parse()
                .unwrap_or_else(|e| fatal(&format!("parse listen addr: {e}")));
            swarm
                .listen_on(listen)
                .unwrap_or_else(|e| fatal(&format!("listen_on: {e}")));
            // Bound to 0.0.0.0, so the swarm's observed/listen addrs report the
            // unspecified host and identify would advertise an undialable address.
            // Register the assigned host IP as an external address so identify
            // advertises it to peers (matching go-peer's announce advertisement).
            if let Some(announce) = &a.announce {
                let ext: Multiaddr = format!("/ip4/{}/udp/{}/quic-v1", announce, a.port)
                    .parse()
                    .unwrap_or_else(|e| fatal(&format!("parse announce addr: {e}")));
                swarm.add_external_address(ext);
            }
            // A listener may ALSO dial upstream peers (peers=/peer=): it is then both
            // reachable (bound + advertised) AND a member of the mesh it dials into.
            // The testground plan launches subscribers as role=listen with
            // peers=<publisher> (so they advertise a data-net address yet still
            // connect outbound to the publisher), matching the zig node whose
            // listener also dials. Without this a rust subscriber would bind and wait
            // to be dialed, but the publisher (also role=listen) never dials it, so
            // no connection forms and nothing is delivered. Dials are optional — a
            // pure bootstrap (no peers=) dials nothing, so do not fatal on no targets.
            if !a.dial_targets().is_empty() {
                dial_peers(&mut swarm, &a);
            }
        }
        "dial" => {
            // dial role requires at least one target (enforced in parse_args), so a
            // zero-dial outcome here means every configured dial failed — fatal.
            if dial_peers(&mut swarm, &a) == 0 {
                fatal("dial role: no peers dialed");
            }
        }
        _ => unreachable!(),
    }

    run_event_loop(&mut swarm, &a, &topic, local_peer).await;
}

// Dial every configured peer (peers=/peer=). gossipsub forms a mesh over whatever
// connections exist, so a node that dials several peers can graft into a mesh
// spanning all of them. A failed dial to one peer does not abort the others.
// Returns the number of peers for which the dial was successfully initiated.
fn dial_peers(swarm: &mut Swarm<Behaviour>, a: &Args) -> usize {
    let mut dialed = 0;
    for peer_text in a.dial_targets() {
        let remote: Multiaddr = match peer_text.parse() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("parse peer multiaddr {peer_text:?}: {e}");
                continue;
            }
        };
        match swarm.dial(remote) {
            Ok(()) => {
                eprintln!("dialer dialing {peer_text}");
                dialed += 1;
            }
            Err(e) => eprintln!("dial {peer_text}: {e}"),
        }
    }
    dialed
}

fn build_behaviour(keypair: &identity::Keypair, a: &Args) -> Behaviour {
    let anonymous = a.sign == "anonymous" || a.sign == "nosign";

    // Authenticity: Signed = StrictSign (sign outbound, require + verify signed
    // inbound; matches go-libp2p's default and the zig node's signing). Anonymous
    // = StrictNoSign (no author/seqno on the wire, no verification) for the
    // anonymous mixed mesh.
    let authenticity = if anonymous {
        MessageAuthenticity::Anonymous
    } else {
        MessageAuthenticity::Signed(keypair.clone())
    };

    let mut config = gossipsub::ConfigBuilder::default();
    config.heartbeat_interval(Duration::from_secs(1));
    if anonymous {
        // StrictNoSign: the default ValidationMode is Strict, which requires a
        // signature on every inbound message and rejects unsigned ones — so the
        // Anonymous authenticity must pair with ValidationMode::Anonymous (no
        // author/seqno/signature expected) or the behaviour refuses to build.
        config.validation_mode(gossipsub::ValidationMode::Anonymous);
        // Content-addressed id sha256(topic||data), matching the zig and go peers.
        // Under StrictNoSign there is no from/seqno to key on, so all impls in the
        // topic MUST agree on this id or dedup/propagation breaks.
        config.message_id_fn(|m: &gossipsub::Message| {
            let mut hasher = Sha256::new();
            hasher.update(m.topic.as_str().as_bytes());
            hasher.update(&m.data);
            gossipsub::MessageId::from(hasher.finalize().to_vec())
        });
    }
    if a.protocols == "meshsub/1.1.0" {
        // Advertise ONLY gossipsub 1.1.0 (not the default 1.1.0+1.0.0 set) so a
        // 1.2-capable peer must negotiate down to 1.1.0 to talk to us.
        config.protocol_id("/meshsub/1.1.0", gossipsub::Version::V1_1);
    }
    if a.px {
        // Peer-exchange EMIT: do_px makes an over-degree PRUNE carry up to
        // prune_peers other mesh peers as offers (prune_peers defaults to 0, which
        // disables PX even with do_px, so it must be set > 0). rust-libp2p emits
        // each offer as a bare peer id WITHOUT a signed peer record — its PeerInfo
        // has only a peer_id field, and make_prune builds `PeerInfo { peer_id }`
        // with no record. A consumer that needs an ADDRESS to dial a never-seen
        // peer (the zig and go nodes both require the signed record's addresses)
        // therefore gets nothing dialable from a rust emitter.
        //
        // Peer-exchange CONSUME is likewise address-blind here: px_connect dials
        // offered peers by peer_id only and explicitly does NOT read a signed
        // record ("Until SignedRecords are spec'd this remains a stub"), so a rust
        // consumer can only dial a PX-offered peer whose address it ALREADY knows.
        //
        // So rust participates in PX framing but cannot complete a record-driven
        // new-peer dial in either direction with libp2p-gossipsub 0.49.4. The
        // cross-impl record-driven PX scenario uses go (which emits + consumes
        // signed records); these knobs let rust be exercised for framing only.
        config.do_px();
        config.prune_peers(16);
    }
    if a.d_high != 0 || a.d != 0 || a.d_low != 0 {
        // Override the mesh DEGREE so a small topology goes over-degree and the
        // heartbeat prunes WITH PX. Each field falls back to the builder default
        // when left at 0.
        if a.d != 0 {
            config.mesh_n(a.d);
        }
        if a.d_low != 0 {
            config.mesh_n_low(a.d_low);
        }
        if a.d_high != 0 {
            config.mesh_n_high(a.d_high);
        }
    }
    let config = config
        .build()
        .unwrap_or_else(|e| fatal(&format!("gossipsub config: {e}")));

    let gossipsub = gossipsub::Behaviour::new(authenticity, config)
        .unwrap_or_else(|e| fatal(&format!("gossipsub behaviour: {e}")));

    // Advertise our protocols (identify + meshsub) so the peer's gossipsub learns
    // we speak meshsub and grafts us into its mesh.
    let identify = identify::Behaviour::new(identify::Config::new(
        "/ipfs/id/1.0.0".into(),
        keypair.public(),
    ));

    Behaviour { gossipsub, identify }
}

async fn run_event_loop(
    swarm: &mut Swarm<Behaviour>,
    a: &Args,
    topic: &IdentTopic,
    local_peer: PeerId,
) {
    let deadline = Instant::now() + Duration::from_millis(a.duration_ms);
    let mut received = false;
    let mut ticker = interval(PUBLISH_INTERVAL);
    // Under anonymous the message-id is content-derived sha256(topic||data), so
    // an identical payload is suppressed by the seen-cache after the first
    // publish (which fires before the mesh grafts); append a #<n> suffix so each
    // publish is unique and propagates once the mesh is up. A subscriber asserts
    // on the message prefix. Signed publishes keep the plain payload.
    let anonymous = a.sign == "anonymous" || a.sign == "nosign";
    let mut seq: u64 = 0;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,

            // Publisher: republish every 500ms so at least one publish lands
            // after the mesh forms (matching the zig node's strategy).
            _ = ticker.tick(), if a.mode == "pub" => {
                let payload = if anonymous {
                    format!("{}#{}", a.message, seq).into_bytes()
                } else {
                    a.message.as_bytes().to_vec()
                };
                seq += 1;
                if let Err(e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), payload)
                {
                    // No mesh peers yet before the mesh forms is expected; only
                    // surface other publish errors.
                    if !matches!(e, gossipsub::PublishError::NoPeersSubscribedToTopic) {
                        eprintln!("publish failed: {e}");
                    }
                }
            }

            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    // When announce=<ip> is set the listener is bound to 0.0.0.0, so
                    // `address` carries the unspecified host and is undialable
                    // cross-container. Advertise the assigned host IP on the
                    // configured port instead (matching go-peer's announce handling).
                    let advertised: Multiaddr = match &a.announce {
                        Some(announce) => format!("/ip4/{}/udp/{}/quic-v1", announce, a.port)
                            .parse()
                            .unwrap_or_else(|e| fatal(&format!("parse announce addr: {e}"))),
                        None => address.clone(),
                    };
                    let full = advertised.with(libp2p::multiaddr::Protocol::P2p(local_peer));
                    eprintln!("listener advertising {full}");
                    if let Some(path) = &a.addr_file {
                        write_addr_file(path, &full.to_string()).await;
                    }
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    eprintln!("connection established with {peer_id}");
                }
                SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(
                    gossipsub::Event::Message { message, propagation_source, .. },
                )) => {
                    received = true;
                    // `author` is the message's signed source peer-id; under
                    // StrictNoSign (anonymous) it is None, so `author=` prints blank
                    // — which the anonymous scenario asserts. `from` is the immediate
                    // relayer (propagation source, always present).
                    let author = message
                        .source
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                    println!(
                        "RECV topic={} author={} from={} data={}",
                        a.topic,
                        author,
                        propagation_source,
                        String::from_utf8_lossy(&message.data)
                    );
                }
                _ => {}
            }
        }
    }

    println!(
        "{{\"role\":\"{}\",\"mode\":\"{}\",\"received\":{}}}",
        a.role, a.mode, received
    );
}

// tokio::time::sleep that targets an absolute deadline, usable inside select!.
async fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        sleep(deadline - now).await;
    }
}

async fn write_addr_file(path: &str, contents: &str) {
    match tokio::fs::File::create(path).await {
        Ok(mut f) => {
            let line = format!("{contents}\n");
            if let Err(e) = f.write_all(line.as_bytes()).await {
                eprintln!("write addr file: {e}");
            }
            let _ = f.flush().await;
        }
        Err(e) => fatal(&format!("create addr file {path:?}: {e}")),
    }
}

fn fatal(msg: &str) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}
