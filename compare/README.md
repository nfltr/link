# link vs yamux

A harness that compares this checkout of `github.com/nfltr/link` with
`github.com/hashicorp/yamux` (v0.1.2, the latest release) for one use: many
byte-stream sessions between two processes over TLS 1.3 or plaintext TCP,
as the nfltr relay's replica-to-replica link uses link.

It is a module of its own (`github.com/nfltr/link/compare`), outside
`./...` of the link module: yamux never enters link's dependencies (link
stays standard library only, `TestModuleBoundary`). `go.mod` replaces link
with `../`, so the harness measures the checkout it sits in.

Results and the decision they fed: nfltr
`docs/decisions/link-vs-yamux-2026-10-11.md`.

A third library, `quic` (`lib_quic.go`: `peerlink/quiclink`, QUIC over
UDP, mutual TLS only), compares link with QUIC for the internet hop
(agents behind NAT, lossy and long paths): `LIBS=link,quic
TRANSPORTS=mtls`, with the cases `hs`, `hol` and `netchange` below and
the netems `50ms-1%`, `100ms-3%`, `150ms-5%` (one-way delay: the RTT is
twice it). Results: nfltr `docs/decisions/quiclink-2026-10-14.md` and
`peerlink/quiclink/README.md`.

## Running it

```sh
make compare          # two processes on this machine (OUT=compare/out)
make compare-docker   # each side in a container of its own: cpusets, tc netem, iptables
make compare-summary  # medians of what ran, link and yamux side by side
```

Both write one JSON line per run to `$(OUT)/host.jsonl` or
`$(OUT)/docker.jsonl`. Environment: `REPS` (default 5), `CASES`,
`NETEMS`, `DUR`, `FD_DUR`, `KEEP=1` (`rig.py` says what each does).
`compare-docker` needs docker with `NET_ADMIN` (OrbStack, Linux docker),
takes about an hour with the defaults, and builds the image
`nfltr-link-faults-rig:1` (python slim + iptables, iproute2) if it is
missing; it removes its containers and network (`lcmp*`) at the end.

Measure on a quiet machine (`uptime` load below 3); every row records the
load average at its run.

## Shape

- `lc` (this directory): `lc serve` is the listener side B (sessions by
  header: `echo`, `src N`, `sink`, `stall`; a raw TLS/TCP echo port as the
  latency baseline; a control HTTP port: `/stats`, `/goaway`, `/die`);
  `lc run -test CASE` is the dialer side A and prints one JSON object.
- One interface for both libraries (`transport.go`): a dialer pool that
  opens sessions with a header, a listener that serves one connection's
  sessions, listener GOAWAY. `lib_link.go` is link as relay-server's
  replica link configures it; `lib_yamux.go` is yamux plus the thin layer
  it needs for the same job (pool, header, hello with a credential,
  GOAWAY close-when-idle, half-close as `CloseWrite`, dead-peer events),
  whose size is part of the comparison.
- Settings, the same on both: window 1 MiB per session (yamux
  `MaxStreamWindowSize`), liveness `default` = 5 s / 10 s (link
  `LivenessDefault`; yamux `KeepAliveInterval` 5 s,
  `ConnectionWriteTimeout` 10 s) or `fast` = 1.5 s / 3 s, pool of up to
  N connections growing one per 2 sessions, mutual TLS 1.3 with one
  P-256 certificate, 64 KiB writes.

## Cases (`-test`)

| case | measures |
|---|---|
| `tput` | MB/s of 1 or 8 sessions (down: B sends; up: A sends) over 1 or N connections; per-session spread; CPU s per MB and cores on both sides; allocations per MB; peak heap |
| `lat` | 64-byte echo p50/p99: held raw connection (baseline), held session, fresh session (open + echo + close) |
| `mem` | heap and goroutines per idle session on both sides (after a 64-byte echo, or after `-preload` bytes echoed) |
| `busy` | heap and goroutines per session while `-n` sessions stream |
| `iso` | one session whose reader stops for good (mode a: A's, b: B's) beside 8 that must each move 16 MiB intact |
| `fair` | 8 sessions reading B's streams for `-dur`: MiB per session, longest gap, how each ended |
| `part` | A drops every packet to and from B (iptables, nothing closed) for `-dur`, then heals: when each end declared the connection dead, how sessions ended, when a new session worked, goroutine/FD leaks |
| `kill` | B SIGKILLs itself mid-transfer (its supervisor restarts it): how fast and how A's sessions ended, recovery, leaks |
| `drain` | B sends GOAWAY with sessions open: old sessions finish intact, new sessions open on another connection, old connections close |
| `falsedead` | 8 echo sessions busy then idle for `-dur` each under netem loss and a CPU limit (`-gcpressure`): dead-peer declarations on both sides |
| `hs` | cold connect to first byte, `-n` times: a new client, a session, 64 bytes echoed (TCP + TLS + link hello, or QUIC's 1-RTT handshake) |
| `hol` | 8 downloads; a burst of 5 % random loss on this side's input (iptables statistic) for `-dur`: per 10 ms tick, ticks where every session stalled vs some did, each session's longest gap, before and during the burst |
| `netchange` | one checked download; this side's address changes (`-oldip` removed, `-newip` added, as a laptop changing networks): did the session live on, the longest stretch without bytes, sessions it took |

`rig.py docker` runs `tput` under netem (none, 2 ms/0.1 %, 20 ms/1 %,
50 ms/2 %, both directions), `fair` at 50 ms/2 %, `part` with the
`default` and `fast` presets, and `falsedead` at 0.25 and 0.1 CPU; the
loss-bound and long cases run both libraries at once on their own
container pairs, the CPU-bound ones one after the other (interleaved).
