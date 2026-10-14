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
| `FuzzRecvV2` | `Conn.Recv`: credit, session limits, ping, pong, GOAWAY |
| `FuzzMuxServeV2` | `Mux.Serve` and its sessions on arbitrary frames |
| `FuzzSendRecvRoundTrip` | what the senders write, `Recv` reads back |
| `FuzzParseWindow` | `ParseWindow`, the window body of a caller's own transport |
| `FuzzParseLiveness` | `ParseLiveness`, liveness specs |
| `FuzzPreamble` (quiclink) | a QUIC session stream's header preamble |

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

Target for v1.0 (README, Maturity): 24 h cumulative without a crasher in
the library. Add each run's minutes to the table.
