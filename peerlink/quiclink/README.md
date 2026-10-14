# quiclink

`github.com/nfltr/link/peerlink/quiclink` is a `peerlink.Link` over QUIC
([quic-go](https://github.com/quic-go/quic-go)): one QUIC connection per
peer, each session a bidirectional QUIC stream. It is a module of its own,
so quic-go never enters link's dependencies (link stays standard library
only).

```go
// The listener: a UDP socket and its TLS; peers named by their certificates.
pc, _ := net.ListenPacket("udp", ":7000")
b := quiclink.New(quiclink.Config{PacketConn: pc, TLS: serverTLS, Liveness: link.LivenessNAT})
go b.Serve(handle)

// The dialler: QUIC settings from the Node, TLS and address from the caller.
a := quiclink.New(quiclink.Config{
	Liveness: link.LivenessNAT,
	Dial: func(ctx context.Context, peer string, qc *quic.Config) (*quic.Conn, error) {
		return quic.DialAddr(ctx, addrOf(peer), clientTLS(peer), qc) // NextProtos: quiclink.ALPN
	},
})
s, err := a.Open(ctx, "b", header)
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
- **No 0-RTT.** 0-RTT data can be replayed by an attacker; quiclink turns
  it off and every connection pays the 1-RTT handshake.

## Behaviour

- Sessions: a stream whose first bytes are the header's length (2 bytes,
  big endian) and the header (up to 65535 bytes). `CloseWrite` sends FIN;
  `Close` sends FIN and stops reading (the peer's writes fail); `Reset`
  cancels both directions with code 2, so the peer reads an error, never
  `io.EOF`. Read and write deadlines.
- Peers: a dialled connection's peer is the name `Open` was given; an
  accepted one's is `Config.ServedPeer`, else the verified client
  certificate's Subject CommonName (none: the connection is closed).
- Liveness from a `link.Liveness`: a keep-alive PING `Interval` after the
  last packet received, QUIC's idle timeout of `Timeout` (from the first
  packet sent after the last one received). `DeadAfter` = Interval +
  Timeout (`LivenessFast`: 4.5 s).
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
  (default 1024, link's). TLS 1.3 only, ALPN `peerlink` by default.

## Checks

`make check-quiclink` (in link's root): build, vet, and the tests with
`-race`: the peerlink conformance suite in both directions, through
`peerlinktest.UDPProxy` (black hole, cut, throttle, rebind),
`TestMigration` (32 MiB each way across three address changes of the
dialler, both directions) and `TestListenerSettings`.

## Results

RESULTS
