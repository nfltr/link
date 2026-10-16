# Performance

What one connection costs per message, how it is measured, and each
optimization made for it: why the old code was slow, what it bought,
which guarantee it could have broken and the test that shows it did not,
the guard that keeps it, and what a later edit must not undo. The README's
"Performance" section has the summary.

## How it is measured

`perf_bench_test.go` (all in process, Apple M5, 10 CPUs):

- `BenchmarkThroughput`: messages of 64 B, 1 KiB, 4 KiB, 64 KiB and
  1 MiB, on 1 and 8 sessions of one connection, one way (`oneway`) or
  both ways at once (`both`: each side opens the sessions and writes),
  over TCP loopback and TLS 1.3 (`net.Pipe` as an extra). The reader
  reads with a 32 KiB buffer. Each run also reports, per message, the
  writes, frames and data frames of the sender's connection and the
  reads of the receiver's (`writes/msg`, `frames/msg`, `rx-reads/msg`),
  counted on the `net.Conn` the `link.Conn` holds; over TLS
  `sock-writes/msg` counts the socket's writes below `tls.Conn`.
- `BenchmarkLatency`: a 64-byte echo on a held session, and a fresh
  session's open, echo and close with (`fresh-answered`) and without
  (`fresh`) waiting for the far end's answer; p50 and p99.
- `BenchmarkIdleSessions`: 8 sessions echoing 4 KiB beside 0 and 1,000
  idle ones; `link-goroutines/idle` is link's goroutines per idle session.

`scripts/perf.sh ab OLD NEW OUT [COUNT] [BENCH] [BENCHTIME]` builds the
test binary at two refs and runs them alternating (old, new, old, ...),
so that load from other work on a shared machine hits both alike, then
benchstat; `PERF_CPU=1,10` runs each at those GOMAXPROCS.
`scripts/perf.sh profile REF OUT` records cpu, heap (alloc and inuse),
mutex, block, goroutine (debug=2) and goroutine-leak profiles of four
representative benchmarks. Every run writes a `manifest.json` (repo, sha,
Go, machine, load average, command, date). The evidence of this work is
under `~/nfltr-evidence/profiles/2026-10-17-link-perf/` on the operator's
machine:

- `link@ecb8130/profile/`: the baseline profiles (v0.10.8 plus the
  benchmarks);
- `link@1fd57b5/ab-vs-ecb8130/`: the before/after A/B below (and
  `link@b1ce38f/ab-vs-ecb8130/`, the same before P7);
- `link@1fd57b5/profile/`: the profiles after (no goroutine reported by
  the goroutine-leak profile);
- `link@<commit>/ab-P<n>/`: each optimization against its parent.

The machine was shared with other work throughout (load average 2-20),
so the A/B runs alternate binaries and benchstat reports medians with
their spread; read deltas below 10 % as noise unless benchstat marks them.

## Results: v0.10.8 against v0.10.9

`scripts/perf.sh ab ecb8130 1fd57b5 OUT 8 '^Benchmark(Throughput|Latency|IdleSessions)$' 0.3s`:
v0.10.8 (with the benchmarks, ecb8130) against v0.10.9's code (1fd57b5),
8 runs each, alternating; load average 8.6-11.4 on 10 CPUs. Medians;
`~` is no significant difference (p >= 0.05). MB/s counts the bytes of
one direction (`both`: of both). allocs/msg is the whole round, sender
and receiver; reads/msg the receiver's reads of its connection (the
`tls.Conn`'s Reads over TLS).

| transport | size | sessions | direction | MB/s v0.10.8 | MB/s now | change | allocs/msg | reads/msg (rx) |
|---|---:|---:|---|---:|---:|---:|---|---|
| tcp | 64 B | 1 | oneway | 23 | 32 | +36.08% | 2 -> 0 | 3 -> 0.451 |
| tcp | 64 B | 1 | both | 32 | 87 | +170.89% | 4 -> 0 |  |
| tcp | 64 B | 8 | oneway | 56 | 335 | +499.24% | 2 -> 0 | 3 -> 0.00115 |
| tcp | 64 B | 8 | both | 65 | 263 | +306.96% | 4 -> 0 |  |
| tcp | 1 KiB | 1 | oneway | 394 | 489 | +24.05% | 2 -> 0 | 3 -> 0.565 |
| tcp | 1 KiB | 1 | both | 526 | 3155 | +499.35% | 4 -> 0 |  |
| tcp | 1 KiB | 8 | oneway | 676 | 1180 | +74.54% | 2 -> 0 | 3 -> 0.084 |
| tcp | 1 KiB | 8 | both | 741 | 2336 | +215.36% | 4 -> 0 |  |
| tcp | 4 KiB | 1 | oneway | 1465 | 1946 | +32.83% | 1 -> 0 | 3 -> 2 |
| tcp | 4 KiB | 1 | both | 2130 | 2839 | +33.33% | 2 -> 0 |  |
| tcp | 4 KiB | 8 | oneway | 2209 | 2373 | ~ | 1 -> 0 | 3 -> 2 |
| tcp | 4 KiB | 8 | both | 2507 | 2661 | ~ | 2 -> 0 |  |
| tcp | 64 KiB | 1 | oneway | 7023 | 8707 | ~ | 1 -> 0 | 3.01 -> 2.03 |
| tcp | 64 KiB | 1 | both | 8557 | 8869 | ~ | 2 -> 0 |  |
| tcp | 64 KiB | 8 | oneway | 7557 | 7953 | ~ | 1 -> 0 | 3.12 -> 2.13 |
| tcp | 64 KiB | 8 | both | 7524 | 7530 | ~ | 2 -> 0 |  |
| tcp | 1 MiB | 1 | oneway | 8834 | 9701 | ~ | 20 -> 0 | 49.2 -> 33.6 |
| tcp | 1 MiB | 1 | both | 8887 | 9142 | ~ | 41 -> 2 |  |
| tcp | 1 MiB | 8 | oneway | 7023 | 7082 | ~ | 16 -> 0 | 50 -> 34.3 |
| tcp | 1 MiB | 8 | both | 7199 | 7436 | ~ | 33 -> 0.5 |  |
| tls | 64 B | 1 | oneway | 21 | 28 | +32.02% | 2 -> 0 | 3 -> 1 |
| tls | 64 B | 1 | both | 52 | 98 | +90.71% | 4 -> 0 |  |
| tls | 64 B | 8 | oneway | 160 | 295 | +84.19% | 2 -> 0 | 3 -> 0.0308 |
| tls | 64 B | 8 | both | 187 | 238 | +27.23% | 4 -> 0 |  |
| tls | 1 KiB | 1 | oneway | 297 | 420 | +41.31% | 2 -> 0 | 3 -> 1 |
| tls | 1 KiB | 1 | both | 1653 | 2731 | +65.20% | 4 -> 0 |  |
| tls | 1 KiB | 8 | oneway | 694 | 884 | +27.46% | 2 -> 0 | 3.01 -> 0.291 |
| tls | 1 KiB | 8 | both | 1167 | 1495 | +28.05% | 4 -> 0 |  |
| tls | 4 KiB | 1 | oneway | 993 | 1402 | +41.20% | 1 -> 0 | 3 -> 2 |
| tls | 4 KiB | 1 | both | 3594 | 4097 | +14.00% | 2 -> 0 |  |
| tls | 4 KiB | 8 | oneway | 1614 | 2000 | +23.90% | 1 -> 0 | 3.21 -> 2.2 |
| tls | 4 KiB | 8 | both | 2551 | 2795 | +9.56% | 2 -> 0 |  |
| tls | 64 KiB | 1 | oneway | 2517 | 2703 | +7.39% | 1 -> 0 | 7 -> 6 |
| tls | 64 KiB | 1 | both | 4064 | 4463 | ~ | 2 -> 0 |  |
| tls | 64 KiB | 8 | oneway | 2951 | 3003 | ~ | 1 -> 0 | 7 -> 6 |
| tls | 64 KiB | 8 | both | 4584 | 4701 | ~ | 2 -> 0 |  |
| tls | 1 MiB | 1 | oneway | 2940 | 2915 | ~ | 17 -> 0 | 112 -> 96 |
| tls | 1 MiB | 1 | both | 4572 | 4572 | ~ | 38.5 -> 1 |  |
| tls | 1 MiB | 8 | oneway | 3014 | 2997 | ~ | 16 -> 0 | 112 -> 96 |
| tls | 1 MiB | 8 | both | 4752 | 4722 | ~ | 32 -> 0 |  |
| pipe | 64 B | 1 | oneway | 26 | 87 | +232.25% | 2 -> 0 | 3 -> 1 |
| pipe | 1 KiB | 1 | oneway | 384 | 1228 | +219.76% | 2 -> 0 | 3 -> 1 |
| pipe | 4 KiB | 1 | oneway | 1470 | 2678 | +82.11% | 1 -> 0 | 3 -> 2 |
| pipe | 64 KiB | 1 | oneway | 8917 | 10159 | +13.92% | 1 -> 0 | 3 -> 2 |
| pipe | 1 MiB | 1 | oneway | 9889 | 10060 | ~ | 16 -> 0 | 48 -> 32 |

Latency (64-byte echo; p50 and p99 of each run's round trips, medians
over the runs; p99 moves a lot between runs on a shared machine, and no
change in it is significant):

| transport | case | µs/op v0.10.8 | µs/op now | change | p50 µs | p99 µs | allocs/op |
|---|---|---:|---:|---:|---|---|---|
| tcp | held | 20.9 | 20.9 | ~ | 20.5 -> 19.9 | 33.8 -> 44.3 | 4 -> 0 |
| tcp | fresh | 30.9 | 30.7 | ~ | 28.4 -> 27.9 | 66.3 -> 71.6 | 28 -> 22 |
| tcp | fresh-answered | 45.6 | 45.5 | ~ | 44.2 -> 44.3 | 77.0 -> 71.6 | 27 -> 22 |
| tls | held | 21.6 | 21.7 | ~ | 21.2 -> 20.9 | 33.3 -> 40.1 | 4 -> 0 |
| tls | fresh | 29.9 | 29.7 | ~ | 28.9 -> 28.8 | 48.3 -> 47.8 | 28 -> 22 |
| tls | fresh-answered | 46.7 | 47.2 | ~ | 45.7 -> 45.9 | 70.6 -> 70.8 | 28 -> 22 |

Per message, unchanged or better everywhere: one data frame per message
up to 64 KiB (16 for 1 MiB); one write of the connection per message for
one session, fewer when sessions share writes (64 B, 8 sessions: 0.039 ->
0.023 per message); over TLS the socket still takes a write per 16 KiB
record (crypto/tls's; below). Idle sessions: 8 busy sessions move the
same beside 1,000 idle ones (`BenchmarkIdleSessions`, no significant
difference), and link holds 0 goroutines per idle session.

Profiles after (`link@1fd57b5/profile/`): 64-byte messages on 8 sessions
are now limited by the senders' shared write lock (`Conn.sendData`, the
mutex profile's 87 %), no longer by the receive loop's reads; at 64 KiB
the kernel's copies (`read`/`write` syscalls) are 65 % of the CPU and
user-space copies 1 %.

## The optimizations

### P1: data buffers go back to their pool without an allocation

- **Where**: `conn.go`: `dataBufs`, `newData`, `releaseData` (commit
  22a040c).
- **Why it was slow**: `releaseData` put `&b`, a pointer to its own
  parameter, into the `sync.Pool`: `b` escaped, so every call allocated
  its slice header on the heap: one allocation per `Session.Read` of a
  data frame (and per `WriteTo` batch), small frames included (the
  baseline heap profile: 18 % of the bytes allocated under 64-byte
  messages, `Session.Read` -> `releaseData`).
- **Gain**: allocations per message, 64 B: 2 -> 1; 64 KiB: 1 -> 0; a held
  64-byte echo: 4 -> 2 (TCP, 1 and 8 sessions; deterministic). Time within
  the noise of that run (load average 7.5-8.3). Evidence:
  `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@22a040c/ab-P1/`.
- **Guarantee at risk**: none of the protocol's; a buffer must go back
  only once nothing references it, as before (the same call sites).
  `go test -race` (`sync.Pool` drops items at random under the race
  detector, which shakes out a use after release).
- **Guard**: `TestSteadyStateAllocs` (0 allocations per echoed message).
- **Don't undo**: pool `*[N]byte` array pointers, never `*[]byte` of a
  local: converting the slice back (`(*[MaxChunk]byte)(b[:MaxChunk])`) is
  free, taking the address of a parameter is an allocation.

### P2: bytequeue pushes reuse the array head that pops freed

- **Where**: `internal/bytequeue/bytequeue.go`: `Queue.head`,
  `pushLocked`, `trim` (commit ee36fee).
- **Why it was slow**: pops resliced the queue from the front
  (`q.bufs = q.bufs[k:]`), which drops capacity; a queue that never
  drained (a reader one slice behind) grew a new array again and again
  (baseline heap profile: `Queue.Push`, 19 % of the bytes allocated under
  64-byte messages).
- **Gain**: bytes allocated per 64-byte message, 8 sessions: 138 -> 86 (the
  rest: the payload's buffer, gone with P3); time -5 % geomean, within the
  noise. Evidence: `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@ee36fee/ab-P2/`.
- **Guarantee at risk**: FIFO order and the byte limit (`Push` waits at
  `limit`): the bytequeue tests and `-race`.
- **Guard**: `TestQueueReusesItsArray` (0 allocations per push and pop
  with a slice always queued).
- **Don't undo**: a head index plus compaction before growth; never
  reslice the front away.

### P3: frames are read ahead, and short payloads queue packed

- **Where**: `read.go` (`view`, `readFull`, `recvIdle`), `conn.go`
  (`recv`, `next`, `Recv`, `smallBufs`/`newChunk`), `mux.go`
  (`Mux.Serve`, `Mux.data`), `credit.go` (`receivedSID`),
  `internal/bytequeue` (`Append`) (commit 4b68fec).
- **Why it was slow**: the receive loop is one goroutine per connection
  and was the bottleneck for small messages. It read each data frame
  with three reads of the connection (header, sid, payload: on plain TCP
  three syscalls; baseline CPU profile, 64-byte messages on 8 sessions:
  `Conn.Recv` 37 % of all CPU, almost all `read` syscalls), converted the
  sid to a string (an allocation) to find the session, and allocated an
  exact buffer for every payload under 4 KiB. A session whose reader was
  behind then held one buffer per frame (up to 16x its bytes for 4 KiB
  frames in 64 KiB buffers, about 40x for 1-byte frames), and each Read
  returned one frame.
- **What it does now**: the loop reads through a read-ahead buffer. At a
  frame boundary with nothing buffered it reads into `rsmall`, a 2 KiB
  array of the Conn's own (so an idle connection holds no pooled
  buffer: it waits in that read); once a read fills `rsmall` (more is
  waiting) it reads into a 64 KiB buffer from the pool, which goes back
  at the next boundary with nothing buffered. A payload of 4 KiB or more
  is read from the connection straight into its own pooled buffer, never
  through the read-ahead. A Mux takes a data frame's sid and a short
  payload as views of the read buffer (`Conn.next`; `Conn.Recv` still
  hands its caller copies, as documented): the session is found with
  `m.sess[string(sid)]` (no allocation), and the payload is appended to
  the session's newest queued buffer when that has room
  (`bytequeue.Append`), else copied into a pooled 4 KiB chunk.
- **Gain**: reads of the connection per message, TCP: 64 B, 8 sessions
  3 -> 0.012; 64 B, 1 session 3 -> 0.46; 1 KiB, 8 sessions 3 -> 0.063;
  4 KiB and 64 KiB 3 -> 2; TLS 64 B, 8 sessions 3 -> 0.032. Time per
  message, TCP, 8 sessions: 64 B 1.16 us -> 275 ns (-76 %), 1 KiB 1.54 us
  -> 745 ns (-52 %); TLS 64 B, 8 sessions -20 %. Allocations per message:
  0. One session sending small messages got slower in this commit alone
  (64 B +37 %, 1 KiB +25 %): with the receiver no longer busy in reads it
  idles between messages, and the sender's yield before each write (P4)
  became a scheduler round trip; P4 removes it, and the net change from
  v0.10.8 is -15 % (faster) for that case. Evidence:
  `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@4b68fec/ab-P3/` (load average 7.7-8.0).
- **Guarantees at risk, and the tests that held**:
  - the wire: `testdata/v2_session.golden` and the wire vectors are read
    through the new path byte for byte (`TestV2Golden`, `TestChainGolden`, `TestWireVectors`);
    every decoder's fuzz target (`make fuzz-short`) runs `Conn.Recv`;
  - EOF semantics (`io.EOF` only at a frame boundary, else
    `io.ErrUnexpectedEOF`, as `io.ReadFull` per read): `eofIn`, and the
    truncation cases of the fuzz corpora;
  - credit: a payload is charged before it is queued (`receivedSID`), so
    the per-session window still bounds what a session holds
    (`TestCreditBoundsUnacknowledgedData`, `TestMuxV2StuckReaderIsolation`);
    packing lowers the memory per byte held (at most about 2x the bytes,
    and one 4 KiB chunk for a session with a few bytes waiting);
  - isolation: a stalled reader beside busy sessions
    (`TestMuxSlowSessionIsolated`, `TestSessionPumpStuckWriterIsolation`,
    `TestChainStalledFarReader`) and the chain fault tests
    (`TestChainMiddleHopFaultsIntegrity` x30, quiclink's
    `TestChainMiddleHopFaultsMixed` x10), and a 16-minute soak with 0
    integrity failures (SOAK.md);
  - idle cost: `TestIdleConnHoldsNoReadBuffer`, `TestChainIdleCost`,
    `TestSessionPumpIdleHoldsNoGoroutine`.
- **Guards**: `TestFramesAndReadsPerMessage` (reads of the connection
  per message: at most 1.5 for 64 B, 2.5 for 4 KiB and 64 KiB; v0.10.8:
  3), `TestSteadyStateAllocs`, `TestShortFramesQueuePacked` (1,000
  one-byte frames are one Read), `TestIdleConnHoldsNoReadBuffer`.
- **Don't undo**: a view (`rframe.view`) is valid only until the next
  read: copy it before the next `recv` (`Mux.data` appends or copies at
  once). Keep the pooled read buffer out of the idle read (`recvIdle`):
  a relay holds a connection per agent, and 64 KiB each idle adds up.
  Large payloads must keep bypassing the read-ahead, or every 64 KiB
  frame is copied twice. The queue may append only onto a slice it owns
  (pushed by the Mux, popped by nobody yet).

### P4: a connection that one session writes on does not yield

- **Where**: `conn.go`: `flushLocked`, `frameLocked` (`lastData`,
  `mixed`) (commit fb3e5c3).
- **Why it was slow**: before each write the writer yields once
  (`runtime.Gosched`), so that on a busy CPU other senders buffer their
  frames for the same write (it came from the relay's data plane, where
  it cut writes per frame under a 1-CPU budget). A connection with one
  session sending has nobody to wait for: the yield was a scheduler round
  trip per `Write`.
- **What it does now**: a flush yields for 64 flushes after data frames
  of two different sessions followed each other in the write buffer; a
  connection that one session writes on writes at once.
- **Gain**: one session, TCP: 64 B 2.95 us -> 2.27 us per message (-23 %),
  1 KiB 2.83 us -> 2.20 us (-22 %); 8 sessions at GOMAXPROCS 1 and 10
  unchanged (no significant change, as intended). Evidence:
  `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@fb3e5c3/ab-P4/` (`PERF_CPU=1,10`).
- **Guarantee at risk**: coalescing on a busy CPU, and fairness between
  sessions. Never yielding is 5x slower for 8 sessions at GOMAXPROCS 1
  (each sender writes its own frame); with this rule 8 sessions keep
  their coalescing (0.13 writes per message at GOMAXPROCS 1, as before).
  `TestSessionsTakeTurnsOnASlowConn`, `TestConnCombinesWrites`.
- **Guard**: `TestFramesAndReadsPerMessage` (at most one write per
  message for one session); the GOMAXPROCS 1 rows of the P4 A/B.
- **Don't undo**: do not drop the yield altogether (the GOMAXPROCS 1
  case), nor yield unconditionally (the one-session case).

### P5: a Session's Write frames and flushes under one lock

- **Where**: `conn.go`: `sendData` (its `flush` argument); `mux.go`:
  `Session.Write`, `Session.send` (commit 68b35fc).
- **Why it was slow**: a `Write` took the connection's write lock twice,
  to buffer its frames and again to write them (`Flush`); with 8 sessions
  on one connection most of the time went to handing that lock over
  (baseline mutex profile: `Session.Write` -> `sendData`, 90 % of the
  contention).
- **What it does now**: the last chunk of a `Write` flushes inside the
  same critical section. A session with a write deadline still flushes
  from a goroutine (`flushAsync`), so no deadline-bound Write waits on
  the network.
- **Gain**: 64 B, 8 sessions, TCP: 286 ns -> 219 ns per message (-24 %),
  writes of the connection per message 0.028 -> 0.018 (the batches are
  fuller). Evidence: `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@68b35fc/ab-P5/`.
- **Guarantee at risk**: write deadlines (`TestWriteDeadline`,
  `TestWriteDeadlineStuckNetwork`), the
  turn-taking of senders waiting for room (`TestSessionsTakeTurnsOnASlowConn`),
  and what a failed write returns (the bytes buffered, then the error:
  `flushErr` keeps `Write`'s count as before).
- **Don't undo**: framing and flushing a small Write in two lock
  acquisitions doubles the lock traffic between sessions.

### P6: a Session's data frames skip the answer check

- **Where**: `conn.go`: `sendData` (no `acceptLocked`), `SendData`
  (keeps it) (commit b6000b3).
- **Why it was slow**: on a FeatureChain connection every data frame
  took the connection's credit lock (`Credit.answering`), shared by all
  its sessions on both the send and the receive path, to learn whether
  the session still owes its ACCEPT. A Session knows: it answered before
  it sends (`autoAccept`), or it opened the session.
- **Gain**: one acquisition of the shared credit lock fewer per Write
  (by construction); time within the noise of its run (load average 9-14).
  Evidence: `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@b6000b3/ab-P6/`.
- **Guarantee at risk**: answers (an accepter's first frame is preceded
  by its ACCEPT): `TestChain*`, and `TestConnSendsAnswerPeerOpens`, which drives
  `Conn.SendData` (still checked).
- **Don't undo**: `Conn.SendData` must keep `acceptLocked`: its caller
  sends no answers of its own.

### P7: after a long payload, read the next header without its payload

- **Where**: `read.go`: `rheadLen`, `Conn.rlong` (`readFull`, `view`)
  (commit 1fd57b5).
- **Why it was slow**: after P3, the read at a frame boundary went into
  the 2 KiB array; on a stream of 64 KiB frames it took up to 2 KiB of the
  next payload, which was then copied into that payload's buffer: 3 % more
  bytes copied where reads are copies rather than syscalls (net.Pipe 1 MiB
  messages +5 %, TLS +4 % in the v0.10.8 -> P6 A/B).
- **What it does now**: after a payload read straight into its buffer,
  the next read takes at most 512 bytes (header and sid).
- **Gain**: at most 506 bytes copied twice per 64 KiB frame instead of
  2 KiB; reads per message unchanged (2 per 64 KiB frame on TCP). Time
  within the noise of its run (load average 11-16). Evidence:
  `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@1fd57b5/ab-P7/`.
- **Guarantee at risk**: as P3 (the same tests).
- **Don't undo**: keep the boundary read short after a long frame.

## Latency (2026-10-17, after v0.10.9)

Latency is a target of its own; a change for it is accepted only when
the median improves in interleaved A/B runs with throughput, fairness,
isolation and every guarantee intact.

**Where the time goes.** `BenchmarkLatency` (TCP loopback, 64-byte
echo, one held session) against `raw`, the same echo on the bare
connection:

| | p50 | wakeups per round trip |
|---|---:|---:|
| bare TCP connection | 13.3-14.5 µs | 2 |
| link, held session | 19.7-20.7 µs | 4 |
| link, two sessions in turn (`held-mixed`) | ~20.4 µs | 6 |
| link, fresh session (open, echo, close) | ~28 µs | 12 |
| link, fresh session waiting for the answer | ~44 µs | 16 |

Wakeups are goroutines scheduled to run per round trip
(`runtime/metrics` `/sched/latencies`, sampled 1 in 8, so an estimate).
On the bare connection each side's reader is woken by the network
poller. Through link the network poller wakes the receive loop, and the
receive loop then wakes the session's reader (`Mux.data` ->
`bytequeue.Push` -> `sync.Cond.Broadcast`): two handoffs more per round
trip. A scheduler trace of 20,000 held echoes
(`go tool trace -pprof=sched`) puts 85 % of all scheduling delay on that
`Broadcast`, about 2 µs per handoff: the reader waits while the receive
loop finishes its next read of the connection and parks. That is the
~6 µs link adds. Evidence: `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@f2796a7/latency-trace-held/`.
`held-mixed` adds the writer's yield (P4) on each side: +2 wakeups, and
no significant change in p50.

**Tried and rejected** (each measured, interleaved):

- *Yield the receive loop after delivering data when nothing more is
  read ahead* (`runtime.Gosched` in `Mux.Serve`), so the woken reader
  runs on that thread at once: p50 unchanged (held 20.3 -> 19.7 µs, ~),
  +2 wakeups per round trip, 4 KiB on 8 sessions -32 % throughput.
  Evidence: `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@f2796a7/latency-gosched-after-delivery/`.
- *Let a reader spin (up to 50 µs, yielding) before it parks*, so data
  that arrives meanwhile needs no wakeup: p50 20 -> 34-50 µs (the
  spinners take the CPU the receive loops need). Evidence:
  `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@f2796a7/latency-reader-spin/`.
- *Hand the connection's read to the waiting session reader* (a
  leader/follower receive: the reader that waits reads the connection
  itself and is woken by the network poller): the only design that
  removes the handoff. Not built: the receive loop waits in a read of the
  connection whenever it is idle, so taking that role over needs an
  interrupted read (a read deadline in the past, which a caller's
  `net.Conn` need not support), and every receive-loop duty (opens,
  credit, liveness, other sessions' data, isolation of a stalled reader)
  would move into whichever reader holds the role. The risk to the
  guarantees is out of proportion to ~4 µs per round trip.

**Per-open cost (P8, not merged).** Branch `perf/latency-open-allocs`
(commit 7df91b4): the session's queue becomes a field of the Session
(`bytequeue.Init`), the queue's pop hook an interface the Session
satisfies through an unexported type instead of a closure, and the
session id is formatted on the stack: a fresh session's open, echo and
close takes 22 -> 18 allocations. Median latency did not move (8
interleaved runs: TCP fresh p50 28.1 -> 28.5 µs, answered 44.3 -> 44.1
µs, both ~; evidence `~/nfltr-evidence/profiles/2026-10-17-link-perf/link@7df91b4/ab-P8-latency/`), so under
the latency rule it waits for a decision. The rest of an open's cost is
syscalls: the OPEN is written at once (`Open`'s contract: a peer that
speaks first is reached), the accepter's explicit ACCEPT is a write of
its own, and the close is a third.

## What was looked at and left

- **TLS writes**: link writes a coalesced batch with one `Write` on the
  `tls.Conn`; crypto/tls then writes each 16 KiB record to the socket
  with a write of its own (`sock-writes/msg`, 4.3 per 64 KiB message).
  Batching the records needs a conn below `tls.Conn` that collects them
  (the relay's `tlsbatch` does); that is the caller's connection, not
  link's.
- **Copying payloads into the write buffer**: a vectored write
  (`net.Buffers`) of header and payload would skip one copy of a large
  payload, but a Write may return before its frames are written (another
  write is in flight), so the bytes must be copied; the copy is 1 % of
  the CPU at 64 KiB (`memmove`), against the kernel's 65 %.
- **Window updates**: one every quarter window per session (0.25 per
  64 KiB message with the default 1 MiB window), each a write when none
  is in flight. A longer interval would save a syscall per 256 KiB, and
  cost a session on a long path its window in flight (credit.go).
- **The credit lock**: `Credit.acquire` and `Consumed` still take the
  connection's credit mutex per message; it does not show in the mutex
  profiles after P5.
