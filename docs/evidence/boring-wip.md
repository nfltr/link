# link "boring" evidence, WIP (2026-10-17, stopped early)

Host: macOS arm64; Linux runs are Docker (OrbStack) golang:1.27.2 linux/arm64 containers, 10 CPUs, 15.7 GiB (not a native Linux host). Source: main f093cf5 (one commit after v0.10.11; quiclink v0.4.12).

## Results
| step | command | result |
|---|---|---|
| baseline | `CI_FORCE=1 make ci` (container) | step 1 `make check`: build, vet, race tests (root 98s, peerlink 27s), fuzz-short 11x10s, gosec all PASS; check-semgrep FAILED: `docker: command not found` (no docker inside the container; environment, not a link bug). ci aborted there after 289s. |
| baseline rest | `make check-grpclink check-quiclink check-soak check-api` | all PASS (quiclink -race suite 150s) |
| fuzz | `FUZZTIME=3m FUZZPARALLEL=2 make fuzz-long` | 5 of 11 targets finished (FuzzRecv, FuzzSendRecvRoundTrip, FuzzParseLiveness, FuzzServerHello, FuzzClientReply), 3m each, no crasher. Stopped; 6 targets not run. About 0.25 h, under CPU contention. |
| quiclink race | 5 parallel x `go test -race -count=10 -run TestChainMiddleHopFaultsMixed` in peerlink/quiclink | 50/50 PASS (~630s each) |
| root race | 8 parallel x `go test -race -count=25 -timeout 3h -run 'TestChain\|Drain\|DeadPeer\|GoAway\|Isolation\|Fair\|Pool' .` | started 13:39, killed ~13:58 before completing (go test prints nothing until done): result unknown |
| peerlink race | `go test -race -count=200 -run 'TestChain\|Drain\|DeadPeer\|GoAway\|Isolation\|Fair\|Pool' ./peerlink` | FAIL, see bug 1 |
| soak | not run | |
| compare | not run | no medians |

## Bug 1: flaky TestDrainDuringHandshake (peerlink/node_test.go:509)
Message: `peerlink: open to "server": dial tcp 127.0.0.1:PORT: connect: connection refused` (from the Open goroutine's `t.Error`; the client Open dials after the server's listener is closed by Drain).

Repro (Linux container, golang:1.27.2):
`go test -race -count=200 -run '^TestDrainDuringHandshake$' ./peerlink`
- original wide-regex run failed; the narrow repro failed in 4 of 6 runs.
- three later reruns of the wide regex with `-v` passed 200/200 each, so it is load dependent.
- one process per run (compiled test binary, 300 runs, ~14 containers of load): race build 1/300 failed, plain build 0/300.
No seed (not a seeded test).

## Not run
Soak, compare (LIBS=link,yamux), fuzz targets 6-11 (FuzzRecvV2, FuzzMuxServeV2, FuzzParseWindow, FuzzForward, quiclink FuzzPreamble and FuzzAnswer), root race count=200, semgrep, stress-ng (not installed).

Test executions that completed: quiclink chain 50, peerlink 200 (1 failed batch), baseline suites 1x. Fuzz ~0.25 h, soak 0 h.
