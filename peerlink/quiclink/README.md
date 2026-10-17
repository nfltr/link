# quiclink

`github.com/nfltr/link/peerlink/quiclink` is a `peerlink.Link` over QUIC
([quic-go](https://github.com/quic-go/quic-go)): one QUIC connection per
peer, each session a bidirectional QUIC stream. It is a module of its own,
so quic-go never enters link's dependencies (link stays standard library
only).

`Example` (mutual TLS; `examplePKI` in [example_test.go](example_test.go) makes a throwaway CA):

```go
func Example() {
	pool, certs := examplePKI("a", "b")
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	b, err := quiclink.New(quiclink.Config{PacketConn: pc, TLS: &tls.Config{
		Certificates: []tls.Certificate{certs["b"]},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}})
	if err != nil {
		panic(err)
	}
	go b.Serve(func(peer string, header []byte, s peerlink.Session) {
		defer s.Close()
		msg, _ := io.ReadAll(s)
		fmt.Fprintf(s, "%s %s %s", peer, header, msg)
	})

	a, err := quiclink.New(quiclink.Config{
		Dial: func(ctx context.Context, peer string, qc *quic.Config) (*quic.Conn, error) {
			return quic.DialAddr(ctx, pc.LocalAddr().String(), &tls.Config{
				Certificates: []tls.Certificate{certs["a"]},
				RootCAs:      pool,
				ServerName:   peer,
				NextProtos:   []string{quiclink.ALPNChain, quiclink.ALPN},
				MinVersion:   tls.VersionTLS13,
			}, qc)
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := a.Open(ctx, "b", []byte("echo"))
	if err != nil {
		panic(err)
	}
	_, _ = io.WriteString(s, "hello")
	_ = s.CloseWrite()
	reply, _ := io.ReadAll(s)
	_ = s.Close()
	fmt.Println(string(reply))

	_ = a.Drain(ctx)
	_ = b.Drain(ctx)
	// Output: a echo hello
}
```

## When to use it

Between peers on the internet: laptops, home servers, CI runners behind
NAT, on lossy or long paths. What QUIC gives there that a multiplexer over
one TCP connection cannot:

- **No head-of-line blocking across sessions.** A packet lost on one
  session delays that session only; over TCP every session waits for the
  retransmission.
- **Connection migration.** A dialler whose address changes (Wi-Fi to
  cellular, a NAT rebinding its port) keeps its connection and sessions;
  over TCP the connection is dead and its sessions lost.
- **One round trip less to connect.** QUIC's handshake carries TLS 1.3
  (1-RTT); TCP + TLS 1.3 take two.

What it costs, and when not to use it:

- **UDP must get through.** Some networks block or throttle UDP (corporate
  firewalls, some hotel and mobile networks). A caller on such paths needs
  a TCP fallback: `peerlink.Node` (link over TLS) behind the same
  `peerlink.Link`, tried when QUIC fails to connect.
- **CPU.** QUIC runs in user space: per byte it costs several times what
  TCP + TLS cost (see the results). It suits the internet hop, where the
  path is the limit; between replicas in one datacenter, link over TCP
  moves more bytes per core.
- **quic-go is nfltr's fork** (`github.com/nfltr/quic-go`, branch
  `nfltr`: upstream plus fixes, each offered upstream). It fixes a slow
  direction after a burst the other way: a side that had only ACKed the
  other's burst left slow start at its initial window (its RTT samples
  rode behind the burst's queue, and HyStart read that as congestion
  though the side was not using its window), then grew one packet per
  round trip (`TestThroughputAfterBurstTheOtherWay`, 100 ms RTT, 20 MB/s
  bottleneck: 0.7 MB/s before, 17-20 MB/s now). It also fixes a panic
  (`numOutStandingFrames negative`) when a stream was aborted after the
  peer's STOP_SENDING.
- **No 0-RTT.** 0-RTT data can be replayed by an attacker; quiclink turns
  it off and every connection pays the 1-RTT handshake.

## Behaviour

- Sessions: a stream whose first bytes are the header's length (2 bytes,
  big endian) and the header (up to 65535 bytes). `CloseWrite` sends FIN;
  `Close` sends FIN and stops reading (the peer's writes fail); `Reset`
  cancels both directions with code 2, so the peer reads an error, never
  `io.EOF` (on an `ALPNChain` connection a `*link.Abort`, below). Read
  and write deadlines.
- Peers: a dialled connection's peer is the name `Open` was given; an
  accepted one's is `Config.ServedPeer`, else the verified client
  certificate's Subject CommonName (none: the connection is closed).
- Liveness from a `link.Liveness`: a keep-alive PING `Interval` after the
  last packet received, QUIC's idle timeout of `Timeout` (from the first
  packet sent after the last one received), and a check of its own: a
  connection that received no packet for `Interval + Timeout/2`, sampled
  every `Timeout/4`, is closed (`CodeDeadPeer`). `DeadAfter` = Interval +
  Timeout (`LivenessFast`: 4.5 s). The check is needed because QUIC's
  idle timer starts only once a packet is sent: a side that cannot send
  when the path goes silent (writes stalled, congestion window full)
  would time out late by as long as it could not send
  (`TestDeadPeerSendStalled`: 5.4 s before, within 4.5 s now).
- Drain: refuses new connections (CONNECTION_REFUSED) and new streams
  (code 1); sends GOAWAY on each connection's control stream, then DONE
  once its own sessions there ended; a connection closes once one side
  holds the other's DONE and has no session left on it, so nothing either
  side still reads is in flight. A peer that got GOAWAY dials a new
  connection for its next `Open`.
- The reverse direction (peers behind NAT): a Node without `Config.Dial`
  opens its sessions on the connections peers dialled in; a Node listing a
  peer in `Config.Connect` keeps a connection to it up.
- Settings: window per session `Config.Window` (default 1 MiB, link's),
  16 windows per connection, `Config.MaxSessions` streams per connection
  (default 1024, link's). TLS 1.3 only; a listener without NextProtos
  offers ALPN `peerlink-chain` (`ALPNChain`), then `peerlink` (`ALPN`).
- Chaining (v0.4.0): on a connection that negotiated `ALPNChain`
  sessions are `peerlink.ChainSession`s (`Answers()` true) and work with
  `peerlink.Forward` and `peerlink.Join` as link's do: an open carries
  its `link.Chain` (hops and trace, 17 bytes before the header
  preamble); the accepter's stream starts with its answer (accept with
  the path's length, or a refusal with code, hops and a message of at
  most 1024 bytes, then FIN); `Abort` resets with its code and hops
  packed in the stream error code (QUIC carries no message), after
  delivering what was written before it where both ends support
  RESET_STREAM_AT (on in a Node's QUIC config). A dialler offering only
  `ALPN`, or a v0.3 listener, gets plain sessions (`Answers()` false).
  `TestChainQUIC*` run paths of 2 to 5 links over QUIC, and over QUIC
  and TCP in turn.

## Checks

`make check-quiclink` (in link's root): build, vet, and the tests with
`-race`: the peerlink conformance suite in both directions, through
`peerlinktest.UDPProxy` (black hole, cut, throttle, rebind),
`TestMigration` (32 MiB each way across three address changes of the
dialler, both directions) and `TestListenerSettings`.

## Results

`compare/` (its README) with `lib quic` beside link (window 1 MiB per
session, mutual TLS 1.3, one connection, liveness `default`). Docker:
each side in an OrbStack Linux container of its own on an arm64 Mac
(cpusets 0-3 and 4-7), tc netem on both sides (one-way delay: the RTT is
twice it), medians of 3 runs, the 1-minute load average 0.6-3.0 (median
1.9). Host: two processes on macOS, load 1.4-3.0 (median 2.9). quic-go
v0.63.0, link v0.7.1.

| | link (TCP) | quiclink (QUIC) |
|---|---|---|
| throughput, no netem, 1 / 8 sessions (Linux) | 2140 / 2590 MB/s | 934 / 961 MB/s |
| CPU per MB, dialler / listener, no netem (Linux) | 370 / 505 µs | 859 / 854 µs |
| throughput, 1 / 8 sessions (macOS loopback) | 2695 / 3081 MB/s | 305 / 318 MB/s |
| CPU per MB (macOS loopback) | 0.57-0.64 ms | 7.5-11.0 ms |
| 8 sessions at 2 ms / 0.1 % | 71.4 MB/s | 22.9 MB/s |
| 8 sessions at 50 ms / 1 % | 0.43 MB/s | 0.68 MB/s |
| 8 sessions at 100 ms / 3 % | 0.082 MB/s | 0.175 MB/s |
| 8 sessions at 150 ms / 5 % | 0.049 MB/s | 0.051 MB/s |
| 1 session at 50 / 100 / 150 ms | 0.37 / 0.074 / 0.090 MB/s | 0.40 / 0.125 / 0.074 MB/s |
| cold connect to first byte, p50 (no netem / 2 / 50 / 100 / 150 ms) | 0.6 / 30 / 418 / 819 / 1220 ms | 0.7 / 19 / 212 / 412 / 615 ms |
| head of line: 8 downloads, 3 s of 5 % loss at 50 ms: longest gap per session, median / max | 1010 / 1280 ms | 170 / 240 ms |
| the same at 20 ms | 1460 / 1720 ms | 110 / 120 ms |
| dialler's address changes mid-download | session lost (`peer silent past the liveness timeout`), bytes again after 4.5 s (`fast`) / 15.0 s (`default`) on a new session | session lives on, longest gap 20-23 ms |
| held session echo, p50 / p99 (macOS loopback) | 21 / 37 µs | 30 / 41 µs |
| fresh session (open + echo + close), p50 | 31 µs | 49 µs |
| heap per idle session, dialler | 0.9 KiB | 2.2 KiB |
| heap per session after 1 MiB echoed, dialler | 10.9 KiB | 3.6 KiB |
| heap per streaming session, dialler | 151 KiB | 98 KiB |
| goroutines per session, dialler / listener (its handler) | 0 / 1 | 0 / 1 |
| stripped linux/amd64 binary, dialler + listener over peerlink | (baseline) | +2.7 MB |

Read with care: netem and iptables drop whole offload batches (GSO on
send, GRO on receive), which hold many TCP segments and several QUIC
packets each, so the same loss rate is not the same loss per byte for the
two; CPU per MB on lossy paths is mostly fixed cost over few bytes and is
left out. What the numbers say: QUIC connects in half the round trips,
keeps every session across an address change, and does not stall every
session behind one loss; on long lossy paths it moves as much or more.
On a short path it moves less (quic-go's recovery at 2 ms) and on a fast
one it costs 2-2.5 times the CPU per byte on Linux and about 15 times on
macOS (no UDP offloads there).
