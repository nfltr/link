# link, yamux and muxado with their defaults

Go benchmarks that run the same code over three stream multiplexers:
this checkout of `github.com/nfltr/link`, `github.com/hashicorp/yamux`
v0.1.2 and `golang.ngrok.com/muxado/v2` v2.0.2 (the muxado ngrok's agent
uses). Each library runs with its defaults: no window, keepalive or
backlog is tuned, because the defaults are what most users get. The
sibling harness in `../` (`compare/README.md`) is the other comparison:
link and yamux configured alike (1 MiB windows, the same liveness), as
two processes, under netem and faults.

It is a package of the `compare` module, so none of the three enters
link's own dependencies. `make check-compare` vets it in CI.

## Defaults that matter

| | link | yamux | muxado |
|---|---|---|---|
| window per stream | 1 MiB (`DefaultWindow`) | 256 KiB (`MaxStreamWindowSize`) | 256 KiB (`MaxWindowSize`) |
| keepalive / dead peer | ping after 5 s idle, dead after 10 s more (`LivenessDefault`) | ping every 30 s, write timeout 10 s | none (heartbeat is an opt-in wrapper) |
| inbound streams | at most 1,024 open (`MaxSessions`); accept runs in a goroutine per stream | accept backlog 256 | accept backlog 128; a stream past it is reset (`AcceptQueueFull`) |
| listener credential | `NoAuth` (link needs one named; the others have none) | none | none |

## Scenarios

All over real TCP loopback, both ends in one process (so heap and
allocation counts are both ends together); the listener reads with a
32 KiB buffer and plain `Read` calls for every library.

| benchmark | measures |
|---|---|
| `Bulk/sessions=1/rtt=0s` | one stream, 64 KiB writes one way: MB/s, B/op, allocs/op per 64 KiB |
| `Bulk/sessions=64/rtt=0s` | 64 streams on one connection writing at once |
| `Bulk/sessions=1/rtt=40ms` | one stream over a path that delays every write by 20 ms each way (bandwidth unlimited): window-bound |
| `Latency/raw` | the floor: a 64-byte echo on the TCP connection itself |
| `Latency/held` | a 64-byte echo on an open stream: p50, p99 |
| `Latency/fresh` | open a stream, 64-byte echo, close |
| `Open` | open, write 1 byte, close, without waiting: the open rate; `lost/op` counts streams the listener never saw |
| `IdleMemory/n=1000` | 1,000 streams, one byte each way, then idle: heap and goroutines per stream (both ends) |
| `SlowReader/stall=1s,5s/awriter=stops,keeps` | streams A and B written as fast as possible; after 0.5 s the listener stops reading A for the stall. B's rate before and during the stall, B's longest gap (5 ms samples), whether B stalled, whether A resumed. `stops`: A's writer stops with A's reader; `keeps`: it keeps writing into the full window. Every wait is bounded (5 s); a stuck writer would show as `stuck`, never a hang |

## Running it

```sh
make compare-defaults                    # 8 passes into compare/out/defaults
compare/defaults/run.sh OUT [COUNT]      # the same, OUT of your choice
cd compare/defaults && go test -run '^$' -bench 'Bulk|Latency' -benchmem
```

`run.sh` builds the test binary once and runs COUNT passes; each pass
runs every benchmark for all three libraries (the library is the
innermost sub-benchmark) and rotates their order (`LIBS`), so load from
other work hits all three alike. Then one profiled pass per library (cpu,
heap, mutex, block) and benchstat (`-col /lib`); `manifest.json` records
the sha, versions, Go, machine and the load average before and after.

## Results (2026-10-29)

Apple M5, 10 CPUs, macOS, Go 1.27.0, link at main 92a1618, 8 passes alternating. The machine was shared with other work:
load average 5.9 at the start, 9.0 at the end, up to 11 during the
run. Pass 8 lost its later benchmarks to an exhausted ephemeral port
range on the machine (`connect: can't assign requested address`), so
those rows have n=7. Medians; `±` is benchstat's spread, `~` no
significant difference (p >= 0.05). benchstat prints the MB/s metrics as
B/s with binary prefixes (1 GiB/s = 1,074 MB/s).

| scenario | link | yamux | muxado |
|---|---:|---:|---:|
| bulk, 1 stream (GiB/s) | **8.34** ± 35% | 3.96 ± 100% | 3.77 ± 98% |
| bulk, 1 stream: B/op, allocs/op per 64 KiB | 12, 0 | 134, 2 | 78, 2 |
| bulk, 64 streams (GiB/s) | 2.45 ± 6% | **6.16** ± 13% | 4.45 ± 16% |
| bulk, 1 stream, 40 ms RTT (MiB/s) | **23.4** | 5.8 | 5.9 |
| held echo p50 / p99 (µs; raw TCP 13.3 / 18.9) | 19.5 / 29.6 | 21.3 / 33.6 (~) | 23.8 / 35.4 (~) |
| fresh open+echo+close p50 / p99 (µs) | **27.6** / 72.6 | 35.5 / 95.3 | 31.1 / 80.4 (p99 ~) |
| open rate (µs per stream) | **9.5** | 17.4 | 14.1 |
| idle stream: heap (both ends), goroutines | **1.98 KiB**, 0 | 3.88 KiB, 0 | 3.06 KiB, 0 |
| slow reader 1 s: B before / during A's stall (GiB/s) | 3.70 / 6.51 | 1.87 / 3.34 | 1.62 / 3.19 |
| slow reader 5 s: B before / during A's stall (GiB/s) | 3.67 / 6.67 | 1.88 / 3.23 | 1.62 / 3.11 |
| slow reader: B stalled, B longest gap, A resumed (keeps), stuck | no, 0 ms, yes, no | no, 0 ms, yes, no | no, 0 ms, yes, no |

## Reading

- **Slow reader: all three keep B going.** Each has a window per stream,
  so a reader that stops holds only its own stream's window; B never
  went 5 ms without bytes and ran about twice as fast during A's stall,
  with the connection to itself. With A's writer still writing, A's
  writer blocked on A's window, and A resumed once its reader did, in
  all three. This is not where these libraries differ; the head-of-line
  stall the first link post measured was gRPC's (one stream's flow
  control shared by many sessions), not a multiplexer with per-stream
  windows.
- **One stream: link moves about twice as many bytes** (8.3 against
  4.0 and 3.8 GiB/s) with no allocation per 64 KiB. The spread is wide
  (the first pass ran while the machine stalled loopback for every
  library); link was ahead in every pass.
- **64 streams at once: link loses.** yamux moved 2.5 times and muxado
  1.8 times link's bytes. link serializes writers on the connection in
  turns (docs/performance.md P4); 64 concurrent 64 KiB writers pay for
  it. Not yet investigated; the profiles are in the evidence directory.
- **40 ms RTT: link's 4x is its default window**, 1 MiB against
  256 KiB: one window per round trip (24 against 6 MiB/s). With the same
  window the three would be alike; `compare/` sets yamux to 1 MiB.
- **Latency on a held stream: no significant difference.** A new
  stream's open, echo and close: link 22 % below yamux and 11 % below
  muxado at the median.
- **Opening streams: link 9.5 µs, muxado 14.1, yamux 17.4.** No stream
  was lost in these passes; muxado did lose 7 of 29,089 in an earlier
  run on this machine, refused past its 128-stream accept backlog after
  the opener had closed.
- **An idle stream costs link about half yamux's heap** and two thirds
  of muxado's; none holds a goroutine per idle stream.

## Not measured

TLS (all over plain TCP), more than one connection, loss and netem (the
`compare/` harness does those, link and yamux only), Linux, CPU per byte
(the profiles have it, unsummarized), smux and QUIC. Both ends share one
process and its scheduler. Run it on a quiet machine before quoting a
number to two digits.

## benchstat

```text
goos: darwin
goarch: arm64
pkg: github.com/nfltr/link/compare/defaults
cpu: Apple M5
                            │      link       │                   yamux                   │                  muxado                  │        tcp         │
                            │     sec/op      │     sec/op       vs base                  │     sec/op      vs base                  │   sec/op     vs base   │
Bulk/sessions=1/rtt=0s-10      7.321µ ±   34%   15.415µ ±    4%  +110.55% (p=0.000 n=8)     16.180µ ±   2%  +121.01% (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10    24.924µ ±    6%    9.906µ ±    4%   -60.25% (p=0.000 n=8)     13.703µ ±   7%   -45.02% (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10    2.676m ±    2%   10.745m ±    3%  +301.51% (p=0.000 n=8)     10.680m ±   1%  +299.08% (p=0.000 n=8)
Latency/raw-10                                                                                                                         13.92µ ± 2%
Latency/held-10                19.62µ ±   83%    21.81µ ±    4%         ~ (p=0.083 n=8)      24.31µ ±  15%         ~ (p=0.105 n=8)
Latency/fresh-10               28.94µ ±   35%    37.99µ ±   43%   +31.28% (p=0.006 n=7+8)    33.09µ ±   2%   +14.33% (p=0.014 n=7+8)
Open-10                        9.542µ ±   54%   17.440µ ±   44%   +82.77% (p=0.001 n=7)     14.063µ ±  26%   +47.38% (p=0.011 n=7)
IdleMemory/n=1000-10           29.40m ± 8199%    38.38m ± 6097%         ~ (p=0.128 n=7)      36.51m ± 107%         ~ (p=0.165 n=7)
geomean                        96.47µ            137.1µ           +42.10%                    138.5µ          +43.57%                   13.92µ       ? ¹ ²
¹ benchmark set differs from baseline; geomeans may not be comparable
² ratios must be >0 to compute geomean

                            │      link      │                 yamux                  │                muxado                │
                            │      B/s       │      B/s        vs base                │      B/s       vs base               │
Bulk/sessions=1/rtt=0s-10      8.338Gi ± 35%   3.960Gi ± 100%   -52.51% (p=0.000 n=8)   3.772Gi ± 98%  -54.76% (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10     2.449Gi ±  6%   6.162Gi ±  13%  +151.61% (p=0.000 n=8)   4.454Gi ± 16%  +81.89% (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10   23.355Mi ±  2%   5.817Mi ±   2%   -75.09% (p=0.000 n=8)   5.851Mi ±  3%  -74.95% (p=0.000 n=8)
geomean                        793.7Mi         529.9Mi          -33.23%                 468.9Mi        -40.93%

                                     │      link      │                   yamux                   │                 muxado                  │         tcp         │
                                     │      B/op      │      B/op       vs base                   │      B/op       vs base                 │    B/op     vs base │
Bulk/sessions=1/rtt=0s-10                12.50 ± 116%     134.00 ±  0%   +972.00% (p=0.000 n=8)        78.00 ±  5%   +524.00% (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10               50.00 ±  72%     232.50 ± 26%   +365.00% (p=0.000 n=8)       363.00 ± 23%   +626.00% (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10            78.96Ki ±   4%    73.65Ki ±  0%     -6.72% (p=0.000 n=8)      73.84Ki ±  3%     -6.48% (p=0.000 n=8)
Latency/raw-10                                                                                                                                0.000 ± 0%
Latency/held-10                          4.500 ±    ?    176.000 ±  0%  +3811.11% (p=0.000 n=8)       64.000 ±  0%  +1322.22% (p=0.000 n=8)
Latency/fresh-10                       36.56Ki ±   1%    37.70Ki ±  0%     +3.11% (p=0.000 n=7+8)    36.02Ki ±  0%     -1.47% (n=7+8)
Open-10                                34.10Ki ±   0%    35.95Ki ±  0%     +5.42% (p=0.001 n=7)      34.53Ki ±  0%     +1.26% (p=0.001 n=7)
IdleMemory/n=1000-10                   4.786Mi ±  16%    5.804Mi ±  0%    +21.26% (p=0.001 n=7)      4.161Mi ±  0%    -13.06% (p=0.026 n=7)
SlowReader/stall=1s/awriter=stops-10   6.923Mi ±  13%   19.418Mi ± 37%   +180.48% (p=0.001 n=7)      7.010Mi ± 11%          ~ (p=0.535 n=7)
SlowReader/stall=1s/awriter=keeps-10   7.103Mi ±  18%   19.129Mi ± 44%   +169.31% (p=0.001 n=7)      6.733Mi ± 28%          ~ (p=0.456 n=7)
SlowReader/stall=5s/awriter=stops-10   6.995Mi ±  10%   62.346Mi ± 47%   +791.24% (p=0.001 n=7)     19.536Mi ± 35%   +179.27% (p=0.001 n=7)
SlowReader/stall=5s/awriter=keeps-10   7.361Mi ±  37%   59.092Mi ± 63%   +702.75% (p=0.001 n=7)     19.909Mi ± 41%   +170.45% (p=0.001 n=7)
geomean                                48.53Ki           174.5Ki         +259.48%                    102.6Ki         +111.50%                             ? ¹ ² ³
¹ benchmark set differs from baseline; geomeans may not be comparable
² summaries must be >0 to compute geomean
³ ratios must be >0 to compute geomean

                                     │      link       │                   yamux                   │                  muxado                  │         tcp         │
                                     │    allocs/op    │   allocs/op     vs base                   │   allocs/op    vs base                   │ allocs/op   vs base │
Bulk/sessions=1/rtt=0s-10                0.000 ±  0%        2.000 ±  0%          ? (p=0.000 n=8)       2.000 ±  0%          ? (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10               0.000 ±  0%        2.000 ±  0%          ? (p=0.000 n=8)       2.000 ±  0%          ? (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10              1.000 ±  0%        5.000 ±  0%   +400.00% (p=0.000 n=8)       6.000 ±  0%   +500.00% (p=0.000 n=8)
Latency/raw-10                                                                                                                                  0.000 ± 0%
Latency/held-10                          0.000 ±  0%        4.000 ±  0%          ? (p=0.000 n=8)       2.000 ±  0%          ? (p=0.000 n=8)
Latency/fresh-10                         23.00 ±  0%        44.00 ±  0%    +91.30% (p=0.000 n=7+8)     10.00 ±  0%    -56.52% (p=0.002 n=7+8)
Open-10                                 21.000 ±  0%       37.000 ±  0%    +76.19% (p=0.001 n=7)       7.000 ±  0%    -66.67% (p=0.001 n=7)
IdleMemory/n=1000-10                   22.071k ±  0%      44.525k ±  0%   +101.74% (p=0.001 n=7)      8.113k ±  0%    -63.24% (p=0.001 n=7)
SlowReader/stall=1s/awriter=stops-10    43.25k ± 50%      388.00k ± 47%   +797.04% (p=0.001 n=7)     156.92k ± 35%   +262.80% (p=0.001 n=7)
SlowReader/stall=1s/awriter=keeps-10    43.04k ± 19%      374.77k ± 51%   +770.76% (p=0.001 n=7)     153.53k ± 42%   +256.72% (p=0.001 n=7)
SlowReader/stall=5s/awriter=stops-10    41.99k ± 46%     1324.71k ± 49%  +3054.89% (p=0.001 n=7)     561.32k ± 39%  +1236.81% (p=0.001 n=7)
SlowReader/stall=5s/awriter=keeps-10    40.05k ± 57%     1285.96k ± 66%  +3110.90% (p=0.001 n=7)     573.13k ± 49%  +1331.04% (p=0.001 n=7)
geomean                                              ¹     1.033k        ?                             463.3        ?                                       ? ² ¹ ³
¹ summaries must be >0 to compute geomean
² benchmark set differs from baseline; geomeans may not be comparable
³ ratios must be >0 to compute geomean

                 │     link     │                 yamux                 │                muxado                │        tcp         │
                 │   p50-sec    │   p50-sec     vs base                 │   p50-sec    vs base                 │   p50-sec    vs base   │
Latency/raw-10                                                                                                   13.33µ ± 1%
Latency/held-10    19.48µ ± 43%   21.31µ ±  2%        ~ (p=0.083 n=8)     23.77µ ± 4%        ~ (p=0.098 n=8)
Latency/fresh-10   27.58µ ± 11%   35.52µ ± 33%  +28.78% (p=0.000 n=7+8)   31.08µ ± 2%  +12.69% (p=0.000 n=7+8)
geomean            23.18µ         27.51µ        +18.70%                   27.18µ       +17.27%                   13.33µ       ? ¹ ²
¹ benchmark set differs from baseline; geomeans may not be comparable
² ratios must be >0 to compute geomean

                 │     link      │                 yamux                 │                muxado                 │        tcp         │
                 │    p99-sec    │   p99-sec     vs base                 │   p99-sec     vs base                 │   p99-sec    vs base   │
Latency/raw-10                                                                                                     18.92µ ± 7%
Latency/held-10    29.56µ ± 141%   33.63µ ± 43%        ~ (p=0.130 n=8)     35.42µ ± 90%        ~ (p=0.105 n=8)
Latency/fresh-10   72.58µ ±  38%   95.33µ ± 35%  +31.34% (p=0.006 n=7+8)   80.35µ ±  5%        ~ (p=0.152 n=7+8)
geomean            46.32µ          56.62µ        +22.23%                   53.35µ        +15.16%                   18.92µ       ? ¹ ²
¹ benchmark set differs from baseline; geomeans may not be comparable
² ratios must be >0 to compute geomean

        │    link    │             yamux             │            muxado             │
        │  lost/op   │  lost/op    vs base           │  lost/op    vs base           │
Open-10   0.000 ± 0%   0.000 ± 0%  ~ (p=1.000 n=7) ¹   0.000 ± 0%  ~ (p=1.000 n=7) ¹
¹ all samples are equal

                     │        link        │                 yamux                 │                muxado                 │
                     │ goroutines/session │ goroutines/session  vs base           │ goroutines/session  vs base           │
IdleMemory/n=1000-10           0.000 ± 0%           0.000 ± 0%  ~ (p=1.000 n=7) ¹           0.000 ± 0%  ~ (p=1.000 n=7) ¹
¹ all samples are equal

                     │      link      │                 yamux                 │                muxado                 │
                     │ heap-B/session │ heap-B/session  vs base               │ heap-B/session  vs base               │
IdleMemory/n=1000-10     1.976Ki ± 1%     3.884Ki ± 0%  +96.59% (p=0.001 n=7)     3.063Ki ± 0%  +55.07% (p=0.001 n=7)

                                     │     link      │                yamux                 │                muxado                │
                                     │ A-B/s-before  │ A-B/s-before   vs base               │ A-B/s-before   vs base               │
SlowReader/stall=1s/awriter=stops-10   3.676Gi ± 33%   1.878Gi ± 39%  -48.90% (p=0.001 n=7)   1.648Gi ± 38%  -55.16% (p=0.001 n=7)
SlowReader/stall=1s/awriter=keeps-10   3.643Gi ± 26%   1.878Gi ± 49%  -48.44% (p=0.001 n=7)   1.602Gi ± 88%  -56.03% (p=0.001 n=7)
SlowReader/stall=5s/awriter=stops-10   3.654Gi ± 37%   1.869Gi ± 56%  -48.84% (p=0.001 n=7)   1.621Gi ± 51%  -55.65% (p=0.001 n=7)
SlowReader/stall=5s/awriter=keeps-10   3.455Gi ± 31%   1.896Gi ± 91%  -45.12% (p=0.001 n=7)   1.620Gi ± 51%  -53.13% (p=0.001 n=7)
geomean                                3.606Gi         1.881Gi        -47.85%                 1.623Gi        -55.00%

                                     │     link     │               yamux                │               muxado               │
                                     │  A-resumed   │ A-resumed   vs base                │ A-resumed   vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=1s/awriter=keeps-10   1.000 ± 0%     1.000 ± 0%       ~ (p=1.000 n=7) ¹   1.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=keeps-10   1.000 ± 0%     1.000 ± 0%       ~ (p=1.000 n=7) ¹   1.000 ± 0%       ~ (p=1.000 n=7) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                                     │     link      │                yamux                 │                muxado                │
                                     │ B-B/s-before  │ B-B/s-before   vs base               │ B-B/s-before   vs base               │
SlowReader/stall=1s/awriter=stops-10   3.702Gi ± 33%   1.873Gi ± 39%  -49.41% (p=0.001 n=7)   1.640Gi ± 36%  -55.70% (p=0.001 n=7)
SlowReader/stall=1s/awriter=keeps-10   3.615Gi ± 25%   1.870Gi ± 49%  -48.27% (p=0.001 n=7)   1.608Gi ± 88%  -55.51% (p=0.001 n=7)
SlowReader/stall=5s/awriter=stops-10   3.671Gi ± 37%   1.878Gi ± 56%  -48.83% (p=0.001 n=7)   1.615Gi ± 51%  -56.01% (p=0.001 n=7)
SlowReader/stall=5s/awriter=keeps-10   3.501Gi ± 31%   1.892Gi ± 93%  -45.97% (p=0.001 n=7)   1.607Gi ± 52%  -54.08% (p=0.001 n=7)
geomean                                3.622Gi         1.878Gi        -48.14%                 1.618Gi        -55.33%

                                     │     link      │                yamux                 │                muxado                │
                                     │  B-B/s-stall  │  B-B/s-stall   vs base               │  B-B/s-stall   vs base               │
SlowReader/stall=1s/awriter=stops-10   6.511Gi ± 54%   3.340Gi ± 51%  -48.71% (p=0.017 n=7)   3.188Gi ± 35%  -51.04% (p=0.017 n=7)
SlowReader/stall=1s/awriter=keeps-10   6.881Gi ± 42%   3.181Gi ± 50%  -53.76% (p=0.001 n=7)   3.050Gi ± 40%  -55.67% (p=0.001 n=7)
SlowReader/stall=5s/awriter=stops-10   6.668Gi ± 58%   3.225Gi ± 48%  -51.63% (p=0.007 n=7)   3.105Gi ± 39%  -53.44% (p=0.011 n=7)
SlowReader/stall=5s/awriter=keeps-10   6.651Gi ± 25%   3.106Gi ± 64%  -53.30% (p=0.001 n=7)   3.161Gi ± 50%  -52.47% (p=0.001 n=7)
geomean                                6.676Gi         3.212Gi        -51.89%                 3.126Gi        -53.18%

                                     │     link     │               yamux                │               muxado               │
                                     │   B-gap-ms   │  B-gap-ms   vs base                │  B-gap-ms   vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=1s/awriter=keeps-10   0.000 ± 0%     0.000 ±  ?       ~ (p=1.000 n=7)     0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ±  ?     0.000 ±  ?       ~ (p=1.000 n=7)     0.000 ± 0%       ~ (p=1.000 n=7)
SlowReader/stall=5s/awriter=keeps-10   0.000 ±  ?     0.000 ±  ?       ~ (p=0.192 n=7)     0.000 ±  ?       ~ (p=0.462 n=7)
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                                     │     link     │               yamux                │               muxado               │
                                     │  B-stalled   │ B-stalled   vs base                │ B-stalled   vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=1s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                                     │     link     │               yamux                │               muxado               │
                                     │    stuck     │   stuck     vs base                │   stuck     vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=1s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
SlowReader/stall=5s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=7) ¹   0.000 ± 0%       ~ (p=1.000 n=7) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean
```
