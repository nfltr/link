# Security

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's private vulnerability
reporting: the repository's **Security** tab, **Report a vulnerability**
(a draft security advisory only the maintainers see). Please do not open a
public issue. Include the version (tag or commit), what an attacker
controls, and a reproduction if you have one. We aim to acknowledge a
report within a week, and to publish a fix and an advisory (with a CVE
where one applies) once a fixed release is tagged.

Supported versions: the latest minor release (v0.x: see the README's
compatibility policy). Fixes are not backported to older minors.

## Threat model

link carries sessions over one byte stream the caller dials and secures.
This section says what the library guarantees against each kind of peer,
and what is left to the caller. "Peer" is whoever is at the other end of
the stream; [WIRE.md](WIRE.md) defines the bytes.

### What link does not do

- **No encryption or integrity.** link puts frames on the caller's stream
  as they are. Use TLS (or another secure channel) for anything that
  crosses a network you do not control; the TLS state is passed to the
  `Authenticator` for channel binding.
- **No authentication of its own.** The `Authenticator` decides who a
  dialer is. `NoAuth` accepts any dialer that sends no credential: use it
  only where the stream is already authenticated (mutual TLS, a pipe).
- **No authorization.** A bound peer may open sessions (up to the limits
  below) and send anything on them. What a session may do, given the
  `Identity` and the header, is the caller's decision.

### An unauthenticated peer (before the hello is verified)

A listener reads at most 5 + 24 + 4096 bytes from a dialer before it calls
`Authenticator.Verify` (magic and version, the fixed hello, at most
`MaxAuth` of credential), allocates at most `MaxAuth` bytes for it, and
validates every field (window, max sessions, liveness) against fixed
bounds; anything else is refused before a reply. The fuzz targets
`FuzzServerHello` and `FuzzClientReply` check these bounds on arbitrary
bytes. A refused dialer reads one status byte, never the reason.

The caller must:

- **Bound the handshake.** `Server` waits as long as its `ctx` allows: a
  dialer that sends its hello one byte at a time (slowloris), or not at
  all, holds the connection and a goroutine until then. Pass a `ctx` with
  a deadline (the example uses 10 s; `peerlink.Node` uses its dead-peer
  bound), and limit concurrent handshakes per source as for any listener.
- **Write a safe `Verify`.** It runs on attacker-controlled bytes: compare
  secrets in constant time (`crypto/subtle.ConstantTimeCompare`), bound
  its own work by `ctx`, and never trust `method` beyond choosing how to
  check `payload`. Its error is not sent to the dialer.
- **Name peers uniquely.** The returned `Identity.Subject` is how callers
  tell peers apart (`peerlink.Node` names an accepted peer by it, and opens
  reverse-direction sessions to the connections of that name). Two peers
  with the same Subject (or an empty one, as with `NoAuth`) are the same
  peer to such a caller; give each a distinct Subject, or name them with
  `peerlink.Config.ServedPeer`.

### A peer after authentication (bound, possibly malicious)

A bound peer can only affect its own connection. Every frame it sends is
validated (`ErrFrame`, `ErrCredit`, `ErrFull` end the connection) and
`FuzzRecv`, `FuzzRecvV2` and `FuzzMuxServeV2` check the receive path on
arbitrary bytes. Resource use per connection is bounded:

| resource | bound | set by |
|---|---|---|
| buffered session data | max_sessions x window per direction | this side's `Config.MaxSessions` and `Config.Window` (defaults 1024 x 1 MiB) |
| sessions the peer opens | `Config.MaxSessions` (an open past it ends the connection) | this side |
| an open's header | 64 KiB per open session | the wire |
| one frame in flight | 1 MiB + 256 bytes (read, then delivered or dropped) | the wire |
| credit | a peer that sends past a window, or grants past its own, is `ErrCredit` | the wire |
| answers to pings | one pong buffered at a time: a ping flood from a peer that does not read is answered once per write, never buffered without bound (`TestPingFloodBoundsBuffer`) | the library |
| refused opens | answered with a reset that waits for room in the write buffer, so a peer that opens without reading stalls its own connection, which liveness then ends | the library |

Choose `MaxSessions` and `Window` so that their product, times the
connections you accept, fits your memory: the defaults allow 1 GiB of
buffered data per connection from a peer that sends without reading.

Liveness: a listener takes the dialer's interval and timeout unless it
sets `Config.Liveness`. The interval is bounded (1 s to 15 s); the timeout
is not, so a listener that must bound how long a vanished dialer holds a
connection sets its own `Liveness`.

Things a peer can do to itself, by design: send GOAWAY (this side then
opens nothing more on that connection), reset or close its sessions, send
pongs that were never asked for (`Observer.PingRTT` reports what the peer
echoes, so treat RTTs from untrusted peers as hints), or answer a GOAWAY's
ping early (its own sessions then end sooner).

### A dialer's view of a listener

A dialer reads at most 14 bytes of reply before binding, with every field
validated. A `Pool` redials `Min` connections with its `Backoff`, also
when a listener takes connections and refuses them (GOAWAY, or a close
within the liveness interval, before any session): a listener that is
shutting down does not cause a reconnect storm
(`TestPoolBacksOffRefusingPeer`).

## peerlink, grpclink, quiclink

`peerlink.Node` inherits link's guarantees and bounds each accepted
connection's handshake by its dead-peer bound. `quiclink` uses QUIC's
TLS 1.3 (0-RTT off: its data can be replayed); a stream's header preamble
is read under the dead-peer bound and is at most 64 KiB
(`FuzzPreamble`). `grpclink` leaves transport security and peer naming to
gRPC's credentials and `Config.Identify`.

## Review record

2026-10-14, v0.8.0: review of hello parsing, the `Authenticator`
contract and resource exhaustion. Fixed: a ping flood from a peer that
does not read grew the write buffer without bound (pongs now coalesce);
a `Pool` redialled a refusing listener in a tight loop (now with
`Backoff`). Documented: the handshake deadline, the `Verify` contract, the
liveness timeout's missing upper bound, and the memory budget.
