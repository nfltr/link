# Fuzzing

Every decoder has a Go fuzz target, seeded from the wire vectors
(`testdata/wire_vectors.json`) and the inputs checked in under
`testdata/fuzz/<Target>/` (and `peerlink/quiclink/testdata/fuzz/`), which
every plain `go test` runs. `make fuzz-short` runs each target for
`FUZZTIME` (default 10 s, part of `make check`); `make fuzz-long` for 30
minutes each; GitHub Actions for 5 minutes each, weekly. A crasher is
committed with its fix as a regression input.

| target | decoder |
|---|---|
| `FuzzServerHello` | a listener reading a hello (`Server`) |
| `FuzzClientReply` | a dialer reading a reply (`Client`) |
| `FuzzRecv` | `Conn.Recv` on arbitrary frames, a session open |
| `FuzzRecvV2` | `Conn.Recv`: credit, session limits, ping, pong, GOAWAY; on a `FeatureChain` connection too: accept, abort and chain-open, one answer per open |
| `FuzzMuxServeV2` | `Mux.Serve` and its sessions on arbitrary frames, without and with `FeatureChain` |
| `FuzzSendRecvRoundTrip` | what the senders write, `Recv` reads back |
| `FuzzParseWindow` | `ParseWindow`, the window body of a caller's own transport |
| `FuzzParseLiveness` | `ParseLiveness`, liveness specs |
| `FuzzPreamble` (quiclink) | a QUIC session stream's header preamble |
| `FuzzForward` | a `Forward` hop (`FeatureChain`) on arbitrary frames, its Route opening to an echo: no leak of onward sessions |
| `FuzzAnswer` (quiclink) | an ALPNChain accepter's answer (accept or refusal) |

## Runs

Minutes per target and executions (M = million).

| target | 2026-10-14 long run (macOS arm64, 4 workers) | 2026-10-14 CI (ubuntu, 5 min) | total minutes |
|---|---|---|---:|
| FuzzServerHello | 25 min, 120 M | 5 | 30 |
| FuzzClientReply | 25 min, 96 M | 5 | 30 |
| FuzzRecv | 25 min, 148 M | 5 | 30 |
| FuzzRecvV2 | 25 min, 132 M | 5 | 30 |
| FuzzMuxServeV2 | 25 min, 132 M | 5 | 30 |
| FuzzSendRecvRoundTrip | 25 min, 137 M | 5 | 30 |
| FuzzParseWindow | 25 min, 153 M | 5 | 30 |
| FuzzParseLiveness | 25 min, 112 M | 5 | 30 |
| FuzzPreamble | 25 min, 148 M | 5 | 30 |
| **all** | 3.8 h, 1.18 billion | 0.75 h | **4.5 h** |

Crashers: one, in a harness, not the library. `FuzzRecv` dereferenced
its session's credit after the input closed the session (a close frame
forgets a session's credit since wire version 2); found in 2 s, fixed in
the harness, kept as `testdata/fuzz/FuzzRecv/f06a40de4b6d841c`. No crasher
in the library.

v0.10.0 (chaining), 2026-10-16, macOS arm64 (6 workers, a shared host),
before the tag: `FuzzRecvV2` and `FuzzMuxServeV2` (now also on
`FeatureChain` connections), `FuzzForward` and `FuzzAnswer` (new) 5 min
each, no crasher; with `make fuzz-short` runs, 21 min in all. One
crasher while writing them, in the harness: `FuzzRecvV2`'s "no data
before the answer" check counted data for a session the input had
already closed (its credit forgotten, so the data is dropped); found in
1 s, the check fixed, kept as `testdata/fuzz/FuzzRecvV2/3eab7b47994aeb39`.

| target | 2026-10-16 (v0.10.0) | total minutes |
|---|---|---:|
| FuzzRecvV2 | 5 min | 35 |
| FuzzMuxServeV2 | 5 min | 35 |
| FuzzForward | 5 min | 5 |
| FuzzAnswer | 5 min | 5 |

Cumulative: 4.85 h.

Target for v1.0 (README, Maturity): 24 h cumulative without a crasher in
the library. Add each run's minutes to the table.
