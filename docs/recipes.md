# Recipes

Each recipe is a runnable Example or program in this repository (`go
test` runs it; `go doc` shows it); the code below is copied from it
verbatim, and a test (`TestDocCodeBlocksAreCompiled`) fails if it drifts.
The helpers the Examples use (`echo`, `call`, `drain`, `must`, `dialTCP`)
are at the top of [peerlink/example_test.go](../peerlink/example_test.go);
the programs under [examples](../examples) need nothing else.

- [Plaintext inside a cluster, or mutual TLS](#plaintext-inside-a-cluster-or-mutual-tls)
- [Choosing a liveness preset](#choosing-a-liveness-preset)
- [gRPC over a link](#grpc-over-a-link)
- [HTTP over a link](#http-over-a-link)
- [Graceful drain on shutdown](#graceful-drain-on-shutdown)
- [Reconnects and backoff](#reconnects-and-backoff)
- [Observing: metrics](#observing-metrics)
- [When a handshake fails](#when-a-handshake-fails)

## Plaintext inside a cluster, or mutual TLS

A link runs over whatever connection your `Dial` returns and your
`Listener` accepts: plain TCP, TLS, mutual TLS. Who a peer is comes from
`link.Config.Auth`, an `Authenticator`:

| where | connection | `Auth` | the peer's name |
|---|---|---|---|
| a trusted network (inside a cluster, a pipe) | plain TCP | `link.NameAuth("me")` on each end | the name it sends (proves nothing) |
| anywhere else | mutual TLS (`ClientAuth: tls.RequireAndVerifyClientCert`) | `link.CertAuth` | its certificate's CommonName, else its first DNS name |
| peers need no names | anything | `link.NoAuth` | `""` for every peer |
| a shared secret, tokens | anything | your own `Authenticator` ([SECURITY.md](../SECURITY.md)) | what its `Verify` returns |

Authentication runs one way: the listener learns who dialled it. The
name you give `Open` is only what your `Dial` resolves, and is not
checked against the listener; to be sure whom you reached, dial with TLS
and set `ServerName` to the peer (as [examples/mtls](../examples/mtls/main.go) does). For
`CertAuth`, each certificate names its peer (CommonName or DNS name), the
listener's DNS name matches the dialer's `ServerName`, and a certificate
used for both roles has both `ExtKeyUsage`s, ServerAuth and ClientAuth
(its `pki` makes such certificates).

A listening Node refuses to start without `Auth` (`peerlink.New` names
the field). Plaintext, from [examples/quickstart](../examples/quickstart/main.go):

```go
	// b listens. NameAuth: each peer sends its name and is known by it
	// (a trusted network only; see docs/recipes.md for mutual TLS).
	b, err := peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.NameAuth("b")},
	})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(reply("b"))

	// a dials b, and keeps a connection to it (Connect) so that b can
	// open sessions to a as well.
	a, err := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.NameAuth("a")},
		Connect: []string{"b"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String()) // peer is "b"
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	go a.Serve(reply("a"))
```

Mutual TLS, from [examples/mtls](../examples/mtls/main.go), a program
of its own (it makes a throwaway CA and a certificate for each peer):
`go run github.com/nfltr/link/examples/mtls@latest`. Only the connections
change, and `CertAuth` names peers by their certificates:

```go
	// b listens: TLS verifies the dialer's certificate, CertAuth names the
	// dialer by it (its CommonName, here "a").
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certs["b"]},
		ClientCAs:    ca,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		log.Fatal(err)
	}
	b, err := peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.CertAuth},
	})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(reply("b"))

	// a dials b with its own certificate and checks b's: ServerName is the
	// peer's name, which b's certificate must carry.
	a, err := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.CertAuth},
		Connect: []string{"b"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			d := tls.Dialer{Config: &tls.Config{
				Certificates: []tls.Certificate{certs["a"]},
				RootCAs:      ca,
				ServerName:   peer,
				MinVersion:   tls.VersionTLS13,
			}}
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	go a.Serve(reply("a"))
```

## Choosing a liveness preset

A connection is dead once nothing arrived for `Interval + Timeout`
(`link.ErrDeadPeer` on every session): each side sends a small frame
after `Interval` without traffic. Choose by how fast a dead peer must be
noticed against how often idle connections wake up:

| preset | dead after | idle frame every | for |
|---|---|---|---|
| `link.LivenessFast` | 4.5 s | 1.5 s | failover and interactive use on a good network |
| `link.LivenessDefault` | 15 s | 5 s | most services; the zero `Config` |
| `link.LivenessNAT` | 30 s | 10 s | paths through NAT, conntrack or load balancers that drop mappings idle 30 s or more |
| `link.LivenessDatacenter` | 45 s | 15 s | stable networks, many connections: fewer wakeups over fast failover |
| `link.LivenessMobile` | 60 s | 15 s | laptops and phones: a sleeping or roaming radio is not taken for a dead peer |

Set it on the dialling side (`link.Config.Liveness`); a listener takes the
dialer's unless it sets its own. `link.ParseLiveness` reads a preset by
name with overrides, for a flag or a config file (`ExampleParseLiveness`):

```go
func ExampleParseLiveness() {
	for _, spec := range []string{"fast", "default", "nat", "datacenter", "mobile", "nat,timeout=40s"} {
		l, err := link.ParseLiveness(spec)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%-16s a frame every %v idle, dead after %v\n", spec, l.Interval, l.Interval+l.Timeout)
	}
	// Output:
	// fast             a frame every 1.5s idle, dead after 4.5s
	// default          a frame every 5s idle, dead after 15s
	// nat              a frame every 10s idle, dead after 30s
	// datacenter       a frame every 15s idle, dead after 45s
	// mobile           a frame every 15s idle, dead after 1m0s
	// nat,timeout=40s  a frame every 10s idle, dead after 50s
}
```

quiclink takes the same `link.Liveness` and keeps the same bound;
grpclink's is longer (gRPC limits client pings): see
[peerlink/README.md](../peerlink/README.md#implementations).

## gRPC over a link

Give a `grpc.Server` a `peerlink.Listener` (the sessions a Node takes),
and dial with `Open` through `grpc.WithContextDialer`. gRPC's connection
is then one session, beside the others on the same link connections.
(`peerlink/grpclink` is the other way round: peerlink sessions carried
over gRPC, for networks that pass only gRPC.) `Example_grpcOverLink` in
[peerlink/grpclink](../peerlink/grpclink/example_test.go):

```go
func Example_grpcOverLink() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server, err := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}})
	if err != nil {
		panic(err)
	}
	sessions := peerlink.NewListener()
	go server.Serve(sessions.Handle) //nolint:errcheck // ends with Drain
	gs := grpc.NewServer()
	healthpb.RegisterHealthServer(gs, health.NewServer()) // any gRPC service
	go gs.Serve(sessions)                                 //nolint:errcheck // ends with Stop

	client, err := peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
	})
	if err != nil {
		panic(err)
	}
	cc, err := grpc.NewClient("passthrough:///server", // the target names the peer
		grpc.WithContextDialer(func(ctx context.Context, peer string) (net.Conn, error) {
			return client.Open(ctx, peer, []byte("grpc"))
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // the link's connection is secured, or not, below
	)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.GetStatus())

	_ = cc.Close()
	gs.Stop()
	_ = client.Drain(ctx)
	_ = server.Drain(ctx)
	// Output: SERVING
}
```

When gRPC shares the Node with other sessions, route by header: call
`sessions.Handle` from your accept for the header `"grpc"` only.

## HTTP over a link

The same `peerlink.Listener` serves an `http.Server`; the client's
`http.Transport.DialContext` opens a session to the peer the URL names
(`Example_http`):

```go
func Example_http() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	sessions := peerlink.NewListener()
	defer sessions.Close()
	go server.Serve(sessions.Handle)
	go http.Serve(sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s over a link", r.URL.Path)
	}))
	defer drain(server)

	client := must(peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: dialTCP(ln.Addr().String()),
	}))
	defer drain(client)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			peer, _, _ := net.SplitHostPort(addr) // "server:80"
			return client.Open(ctx, peer, []byte("http"))
		},
	}
	defer transport.CloseIdleConnections() // they are sessions: Drain waits for them
	resp := must((&http.Client{Transport: transport}).Get("http://server/hello"))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Println(string(body))
	// Output: /hello over a link
}
```

`http.Transport` keeps idle connections: those are open sessions, which a
`Drain` waits for. Close them first (`CloseIdleConnections`).

## Graceful drain on shutdown

`Drain` stops taking new sessions (a peer's `Open` toward it fails; its
own `Open` returns `peerlink.ErrDraining`), lets open ones run to their
end, then releases everything; at its context's end it cuts what is
left. Call it when you are ready, after your own shutdown steps, for
example once `signal.NotifyContext(ctx, syscall.SIGTERM)` is done
(`ExampleNode_Drain`):

```go
func ExampleNode_Drain() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	client := must(peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("client")}, Dial: dialTCP(ln.Addr().String())}))
	defer drain(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := must(client.Open(ctx, "server", []byte("echo")))
	defer s.Close()

	// SIGTERM, say: drain for up to 10 s.
	drained := make(chan error, 1)
	go func() { drained <- server.Drain(ctx) }()

	_, _ = io.WriteString(s, "last request")
	_ = s.CloseWrite()
	reply, _ := io.ReadAll(s)
	fmt.Println(string(reply))
	fmt.Println("drained:", <-drained)
	_, err := server.Open(ctx, "client", nil)
	fmt.Println(errors.Is(err, peerlink.ErrDraining))
	// Output:
	// client echo last request
	// drained: <nil>
	// true
}
```

A dialler's connections to a draining listener get GOAWAY, and its new
sessions go to its other connections; `link.Pool.Drain` replaces a pool's
connections make-before-break.

## Reconnects and backoff

- A Node dials a peer at its first `Open` and keeps a `link.Pool` of
  connections to it: `Config.Pool.Min` stay up (redialled as they end),
  up to `Max` open as sessions need them.
- `Config.Pool.Backoff(attempt)` is how long to wait before redialling
  after failed dials. With it, `Open` waits and redials until its context
  ends; without it (nil), `Open` fails at once with the dial's error, and
  retries are yours.
- `Config.Connect` keeps a connection to each peer named up from `Serve`
  on, so that the peer can open sessions toward you (behind NAT).
- quiclink redials `Connect` peers after 100 ms, doubling to 5 s.

`Example_reconnect`: the first two dials fail, `Backoff` redials.

```go
func Example_reconnect() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	defer drain(server)

	var dials atomic.Int32
	flaky := func(ctx context.Context, peer string) (net.Conn, error) {
		if dials.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return dialTCP(ln.Addr().String())(ctx, peer)
	}
	client := must(peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: flaky,
		Pool: link.PoolConfig{Backoff: func(attempt int) time.Duration {
			return min(time.Duration(attempt)*50*time.Millisecond, 2*time.Second)
		}},
	}))
	defer drain(client)
	fmt.Println(call(client, "server", "hello"), "after", dials.Load(), "dials")

	dials.Store(0)
	failFast := must(peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("client")}, Dial: flaky}))
	defer drain(failFast)
	fmt.Println(call(failFast, "server", "hello"))
	// Output:
	// client echo hello after 3 dials
	// peerlink: open to "server": connection refused
}
```

## Observing: metrics

`Config.Observer(peer)` gives each peer a `link.Observer`; its methods
are called from the connection's goroutines and must not block. Embed
`link.NopObserver` and count what you need: `ConnUp`, `ConnDown(id, err)`
(why it ended: `ErrDeadPeer`, the network's error, nil when closed on
purpose), `SessionOpen`, `SessionClose` (also for each session its
connection's end cut, before `ConnDown`), `CreditWait` (a sender waited
for its reader: backpressure), `PingRTT`, `GoAway`, `SessionAbort(a,
byPeer)` (an ABORT with its code, sent or received) (`Example_observer`).
A `Credit` from `NewCredit` takes one with `SetObserver`.

Counters need no Observer: `Conn.Stats()` (a `link.ConnStats`: bytes in
and out, sessions opened and aborted, credit waits and their time, bytes
queued for readers), `Pool.ConnStats()` (every connection it had) and
quiclink's `Node.Stats()` are atomics read when called, so a metrics
collector reads them at scrape time and nothing runs between scrapes.

An Observer that counts a peer's connections and sessions:

```go
// counter counts a peer's connections and the sessions it opened.
type counter struct {
	link.NopObserver // the events it does not count
	conns, sessions  atomic.Int32
}

func (c *counter) ConnUp(link.Identity) { c.conns.Add(1) }
func (c *counter) SessionOpen()         { c.sessions.Add(1) }
```

```go
func Example_observer() {
	perPeer := map[string]*counter{"client": {}}
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.NameAuth("server")},
		Observer: func(peer string) link.Observer {
			if c := perPeer[peer]; c != nil {
				return c
			}
			return nil // peers it does not count
		},
	}))
	go server.Serve(echo)
	defer drain(server)
	client := must(peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("client")}, Dial: dialTCP(ln.Addr().String())}))
	defer drain(client)

	call(client, "server", "one")
	call(client, "server", "two")
	c := perPeer["client"]
	fmt.Println("client:", c.conns.Load(), "connection,", c.sessions.Load(), "sessions")
	// Output: client: 1 connection, 2 sessions
}
```

## When a handshake fails

The dialer's `Open` returns the reason (test it with `errors.Is`: the
[README's table](../README.md#errors)). The listener closes such a
connection and goes on; to hear of it, set
`peerlink.Config.HandshakeError`: a wrong credential is `link.ErrAuth`
(wrapping your `Verify`'s error); a TLS mismatch, a certificate TLS refused or a
dialer that is not link's is `link.ErrHandshake`; each says what to check.
