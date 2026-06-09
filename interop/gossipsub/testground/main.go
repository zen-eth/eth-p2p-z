// Command gossipsub-quic is a Testground "Path B" participant for QUIC-only
// gossipsub interop. The Go plan owns ALL testground coordination (sync
// barriers, peer-address exchange, network shaping, result recording) and
// drives the eth-p2p-z `libp2p-gossipsub-interop` QUIC node as a subprocess via
// its key=value CLI. The Zig node stays a dumb node; testground runs real
// Docker containers on a real tc/netem-shaped network, so real QUIC works
// (unlike the Shadow simulator).
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	stdsync "sync" // aliased: the testground sync package below shadows "sync"
	"sync/atomic"
	"time"

	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
)

// The eth-p2p-z gossipsub node binary, copied into the runtime image by the
// Dockerfile. Override with GOSSIPSUB_BIN if you relocate it.
const defaultBinPath = "/usr/local/bin/libp2p-gossipsub-interop"

// addrTopic carries each publisher's full /p2p/<id> multiaddr (a string) to the
// subscribers. NewTopic's second arg is a SAMPLE value used to derive the
// payload type via reflection; "" => the topic carries strings.
var addrTopic = sync.NewTopic("gossipsub-addrs", "")

func main() {
	run.InvokeMap(map[string]interface{}{
		"publish-deliver": run.InitializedTestCaseFn(publishDeliver),
	})
}

func publishDeliver(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	// --- params (all declared with defaults in manifest.toml; *Param panics if
	// a read has no matching manifest default, so keep them in lockstep) ---
	topic := runenv.StringParam("topic")
	message := runenv.StringParam("message")
	sign := runenv.StringParam("sign")
	durationMs := runenv.IntParam("duration_ms")
	px := runenv.StringParam("px")
	d := runenv.IntParam("d")
	dLow := runenv.IntParam("d_low")
	dHigh := runenv.IntParam("d_high")
	port := runenv.IntParam("port")
	latencyMs := runenv.IntParam("latency_ms")
	bandwidthMb := runenv.IntParam("bandwidth_mb")
	setupSlackSecs := runenv.IntParam("setup_slack_secs")
	nPublishers := runenv.IntParam("n_publishers")

	// Whole-test deadline: node duration plus slack for boot/barrier/teardown.
	total := time.Duration(durationMs)*time.Millisecond +
		time.Duration(setupSlackSecs)*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), total)
	defer cancel()

	client := initCtx.SyncClient
	netclient := initCtx.NetClient

	// --- PHASE 1: wait for the data network, then shape it ---
	// InitializedTestCaseFn already calls MustWaitNetworkInitialized; doing it
	// again is harmless. Shaping is only valid with a sidecar (local:docker has
	// one; local:exec does not).
	if runenv.TestSidecar {
		netclient.MustWaitNetworkInitialized(ctx)
		netclient.MustConfigureNetwork(ctx, &network.Config{
			Network: network.DefaultDataNetwork, // "default"
			Enable:  true,
			Default: network.LinkShape{
				Latency:   time.Duration(latencyMs) * time.Millisecond,
				Bandwidth: uint64(bandwidthMb) * 1024 * 1024, // bytes/sec
			},
			CallbackState:  sync.State("network-configured"),
			CallbackTarget: runenv.TestInstanceCount,
			RoutingPolicy:  network.AllowAll,
		})
	}

	// --- role election: GlobalSeq is the 1-based global ordinal ---
	seq := initCtx.GlobalSeq
	isPublisher := seq == 1
	role := "subscriber"
	if isPublisher {
		role = "publisher"
	}

	// The announce IP MUST be the routable data-network IP (NOT 127.0.0.1) so the
	// published multiaddr is dialable from the other containers over the shaped
	// link. Fall back to 0.0.0.0 only when there is no traffic shaping.
	announceIP := "0.0.0.0"
	if runenv.TestSidecar {
		announceIP = netclient.MustGetDataNetworkIP().String()
	}
	runenv.RecordMessage("role=%s seq=%d announceIP=%s", role, seq, announceIP)

	// Writable scratch dir for the addr_file; TestOutputsPath is captured as a
	// run artifact (handy for debugging).
	addrFile := filepath.Join(runenv.TestOutputsPath, "addr.out")
	durArg := fmt.Sprintf("duration_ms=%d", durationMs)
	portArg := fmt.Sprintf("port=%d", port)

	if isPublisher {
		// --- PHASE 2 (publisher): launch node, mint addr_file, publish multiaddr ---
		args := []string{
			"role=listen", "mode=pub",
			portArg,
			"announce=" + announceIP,
			"addr_file=" + addrFile,
			"topic=" + topic,
			"message=" + message,
			"sign=" + sign,
			"px=" + px,
			fmt.Sprintf("d=%d", d),
			fmt.Sprintf("d_low=%d", dLow),
			fmt.Sprintf("d_high=%d", dHigh),
			durArg,
		}
		cmd, recvSeen, scanWG, err := startNode(ctx, runenv, args, topic)
		if err != nil {
			runenv.RecordFailure(err)
			return err
		}

		myAddr, err := waitForAddrFile(ctx, addrFile)
		if err != nil {
			_ = cmd.Process.Kill()
			runenv.RecordFailure(err)
			return err
		}
		runenv.RecordMessage("publisher multiaddr: %s", myAddr)

		// Publish my multiaddr for the subscribers, then release the ready barrier.
		client.MustPublish(ctx, addrTopic, myAddr)
		client.MustSignalAndWait(ctx, sync.State("ready"), runenv.TestInstanceCount)

		// Wait for the node to finish its duration and hard-exit.
		received, exitErr := finishNode(cmd, recvSeen, scanWG)
		runenv.RecordMessage("publisher done received=%v exitErr=%v", received, exitErr)

		// All nodes converge before any teardown.
		client.MustSignalAndWait(ctx, sync.State("complete"), runenv.TestInstanceCount)

		// A publisher passes if it ran to completion (exit 0). Delivery is asserted
		// on the subscriber side.
		if exitErr != nil {
			runenv.RecordFailure(exitErr)
			return exitErr
		}
		runenv.RecordSuccess()
		return nil
	}

	// --- subscriber ---
	// Subscribe BEFORE the ready barrier so we don't miss the published addr; the
	// node reads peers once at startup, so collect every publisher multiaddr
	// before launching it.
	ch := make(chan string, runenv.TestInstanceCount)
	client.MustSubscribe(ctx, addrTopic, ch)

	// Gate on ready so we never dial before the publisher has published its addr.
	client.MustSignalAndWait(ctx, sync.State("ready"), runenv.TestInstanceCount)

	var peers []string
	for len(peers) < nPublishers {
		select {
		case a := <-ch:
			peers = append(peers, a)
		case <-ctx.Done():
			err := fmt.Errorf("subscriber timed out collecting publisher addrs (got %d/%d)", len(peers), nPublishers)
			runenv.RecordFailure(err)
			return err
		}
	}
	runenv.RecordMessage("subscriber dialing peers: %s", strings.Join(peers, ","))

	// --- PHASE 2 (subscriber): launch node pointing at the publisher(s) ---
	args := []string{
		"role=listen", "mode=sub",
		portArg,
		"announce=" + announceIP,
		"peers=" + strings.Join(peers, ","),
		"topic=" + topic,
		"sign=" + sign,
		"px=" + px,
		fmt.Sprintf("d=%d", d),
		fmt.Sprintf("d_low=%d", dLow),
		fmt.Sprintf("d_high=%d", dHigh),
		durArg,
	}
	cmd, recvSeen, scanWG, err := startNode(ctx, runenv, args, topic)
	if err != nil {
		runenv.RecordFailure(err)
		return err
	}

	received, exitErr := finishNode(cmd, recvSeen, scanWG)
	runenv.RecordMessage("subscriber done received=%v exitErr=%v", received, exitErr)

	// Converge before teardown regardless of result.
	client.MustSignalAndWait(ctx, sync.State("complete"), runenv.TestInstanceCount)

	if exitErr != nil {
		runenv.RecordFailure(exitErr)
		return exitErr
	}
	if !received {
		err := fmt.Errorf("subscriber did not receive a message on topic %q", topic)
		runenv.RecordFailure(err)
		return err
	}
	runenv.RecordSuccess()
	return nil
}

// startNode spawns the eth-p2p-z gossipsub node, streams stdout+stderr into the
// instance log, and flags recvSeen on the first delivery (a "RECV topic=<topic>"
// line) or a final JSON "received":true. The returned WaitGroup is Done()'d by
// the two scan goroutines once they drain their pipe to EOF (see finishNode).
func startNode(ctx context.Context, runenv *runtime.RunEnv, args []string, topic string) (*exec.Cmd, *atomic.Bool, *stdsync.WaitGroup, error) {
	bin := os.Getenv("GOSSIPSUB_BIN")
	if bin == "" {
		bin = defaultBinPath
	}
	runenv.RecordMessage("exec %s %s", bin, strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, bin, args...)
	// Belt-and-suspenders; the binary already selects epoll at BUILD time
	// (-Dzio-backend=epoll), so this is a documented no-op.
	cmd.Env = append(os.Environ(), "ZIO_BACKEND=epoll")

	var recvSeen atomic.Bool
	var scanWG stdsync.WaitGroup
	recvNeedle := "RECV topic=" + topic

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return cmd, &recvSeen, &scanWG, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return cmd, &recvSeen, &scanWG, fmt.Errorf("stderr pipe: %w", err)
	}

	scan := func(r io.Reader, tag string) {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024) // RECV lines can be large
		for sc.Scan() {
			line := sc.Text()
			if strings.Contains(line, recvNeedle) || strings.Contains(line, `"received":true`) {
				recvSeen.Store(true)
			}
			runenv.RecordMessage("[node %s] %s", tag, line)
		}
	}
	if err := cmd.Start(); err != nil {
		return cmd, &recvSeen, &scanWG, fmt.Errorf("start node: %w", err)
	}
	scanWG.Add(2)
	go func() { defer scanWG.Done(); scan(stdout, "out") }()
	go func() { defer scanWG.Done(); scan(stderr, "err") }()
	return cmd, &recvSeen, &scanWG, nil
}

// finishNode waits for the node to exit and returns (received, exitErr).
// Order matters: drain the pipes to EOF (scanWG) BEFORE cmd.Wait(). os/exec
// docs: "it is incorrect to call Wait before all reads from the pipe have
// completed." The node hard-exits after its duration (closing its stdout, which
// gives the scanners EOF), so scanWG.Wait() returns without needing cmd.Wait().
func finishNode(cmd *exec.Cmd, recvSeen *atomic.Bool, scanWG *stdsync.WaitGroup) (bool, error) {
	scanWG.Wait()
	err := cmd.Wait()
	return recvSeen.Load(), err
}

// waitForAddrFile polls addr_file until non-empty (the listener writes its
// /p2p/<id> multiaddr once QUIC is bound), bounded by ctx.
func waitForAddrFile(ctx context.Context, path string) (string, error) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		if b, err := os.ReadFile(path); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				if nl := strings.IndexByte(s, '\n'); nl >= 0 {
					s = strings.TrimSpace(s[:nl])
				}
				return s, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for addr_file %s: %w", path, ctx.Err())
		case <-t.C:
		}
	}
}
