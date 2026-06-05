// A minimal rust-libp2p gossipsub peer for cross-implementation interop smoke
// testing against the zig node. It mirrors the zig interop binary's argument
// convention (key=value): role=listen|dial, mode=pub|sub, port=, peer=,
// addr_file=, topic=, message=, duration_ms=. A listener writes its full
// /p2p/<peer-id> multiaddr to addr_file once bound; a dialer reads peer= and
// connects. Both subscribe to the topic; a publisher publishes the message
// every 500ms (mesh-formation tolerant, matching the zig node); a subscriber
// prints "RECV ..." on receipt and a final {"received":..} JSON line.
//
// Signing policy is MessageAuthenticity::Signed (rust-libp2p's StrictSign
// equivalent and the default for go-libp2p): the publisher signs every message
// and the subscriber rejects unsigned inbound. This matches the zig node, which
// also signs per the libp2p pubsub scheme, so both directions verify.
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

    let behaviour = build_behaviour(&keypair);

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
            let listen: Multiaddr = format!("/ip4/127.0.0.1/udp/{}/quic-v1", a.port)
                .parse()
                .unwrap_or_else(|e| fatal(&format!("parse listen addr: {e}")));
            swarm
                .listen_on(listen)
                .unwrap_or_else(|e| fatal(&format!("listen_on: {e}")));
        }
        "dial" => {
            // Dial every configured peer. gossipsub forms a mesh over whatever
            // connections exist, so a node that dials several peers can graft
            // into a mesh spanning all of them. A failed dial to one peer does
            // not abort the others.
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
            if dialed == 0 {
                fatal("dial role: no peers dialed");
            }
        }
        _ => unreachable!(),
    }

    run_event_loop(&mut swarm, &a, &topic, local_peer).await;
}

fn build_behaviour(keypair: &identity::Keypair) -> Behaviour {
    // Signed authenticity = StrictSign: sign outbound, require + verify signed
    // inbound. Matches go-libp2p's default and the zig node's signing so both
    // directions verify the from/key binding.
    let gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .build()
            .unwrap_or_else(|e| fatal(&format!("gossipsub config: {e}"))),
    )
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

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,

            // Publisher: republish every 500ms so at least one publish lands
            // after the mesh forms (matching the zig node's strategy).
            _ = ticker.tick(), if a.mode == "pub" => {
                if let Err(e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), a.message.as_bytes())
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
                    let full = address
                        .clone()
                        .with(libp2p::multiaddr::Protocol::P2p(local_peer));
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
                    let from = message
                        .source
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| propagation_source.to_string());
                    println!(
                        "RECV topic={} from={} data={}",
                        a.topic,
                        from,
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
