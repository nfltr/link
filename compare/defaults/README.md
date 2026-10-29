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

Apple M5, 10 CPUs, macOS, Go 1.27.0, link at main 1d4804e (with the
write path change below), 8 passes alternating. The machine was shared
with other work: load average 8.3 at the start, 5.3 at the end, 5.8-8.4
at the start of each pass. Medians; `±` is benchstat's spread, `~` no
significant difference (p >= 0.05). benchstat prints the MB/s metrics as
B/s with binary prefixes (1 GiB/s = 1,074 MB/s).

| scenario | link | yamux | muxado |
|---|---:|---:|---:|
| bulk, 1 stream (GiB/s) | **8.19** ± 25% | 3.75 ± 39% | 3.57 ± 28% |
| bulk, 1 stream: B/op, allocs/op per 64 KiB | 12, 0 | 140, 2 | 83, 2 |
| bulk, 64 streams (GiB/s) | **6.59** ± 29% | 5.32 ± 22% | 3.78 ± 16% |
| bulk, 64 streams: B/op, allocs/op per 64 KiB | 18, 0 | 246, 2 | 543, 2 |
| bulk, 1 stream, 40 ms RTT (MiB/s) | **23.6** | 6.0 | 5.9 |
| held echo p50 / p99 (µs; raw TCP 13.3 / 36.0) | **20.2** / 49.4 | 22.1 / 53.0 (p99 ~) | 24.5 / 58.4 (p99 ~) |
| fresh open+echo+close p50 / p99 (µs) | **28.2** / **83.7** | 40.3 / 124.8 | 31.5 / 90.4 (~) |
| open rate (µs per stream) | **10.1** | 17.9 | 15.0 |
| idle stream: heap (both ends), goroutines | **1.97 KiB**, 0 | 3.88 KiB, 0 | 3.06 KiB, 0 |
| slow reader 1 s: B before / during A's stall (GiB/s) | 3.45 / 6.54 | 1.70 / 3.16 | 1.62 / 2.80 |
| slow reader 5 s: B before / during A's stall (GiB/s) | 3.28 / 6.18 | 1.56 / 2.75 | 1.31 / 2.60 |
| slow reader: B stalled, B longest gap, A resumed (keeps), stuck | no, 0 ms, yes, no | no, 0 ms, yes, no | no, 0 ms, yes, no |

The first run (link at 92a1618, before the change; load average 5.9-11):
64 streams 2.45 GiB/s for link, 6.16 for yamux and 4.45 for muxado; the
other rows within the spread of the run above.

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
- **One stream: link moves about twice as many bytes** (8.2 against
  3.8 and 3.6 GiB/s) with no allocation per 64 KiB. link was ahead in
  every pass.
- **64 streams at once: link 6.6, yamux 5.3, muxado 3.8 GiB/s.** In the
  first run link lost here (2.45 against 6.16 and 4.45): senders waiting
  for room in link's write buffer all waited on one condition variable,
  and each frame woke every one of them (about 66 goroutines woken per
  64 KiB message, most of the CPU in scheduler and lock handoffs). Each
  is now woken alone, when it is its turn (docs/performance.md, P9):
  about 3 wakeups per message. link alone, base and change alternating,
  8 passes: 2.35 -> 8.06 GiB/s, the other rows unchanged. The spread is
  wide on a shared machine; yamux's median moved 6.2 -> 5.3 between the
  two runs with no change of its own.
- **40 ms RTT: link's 4x is its default window**, 1 MiB against
  256 KiB: one window per round trip (24 against 6 MiB/s). With the same
  window the three would be alike; `compare/` sets yamux to 1 MiB.
- **Latency on a held stream: close.** At the median yamux takes 9.7 %
  and muxado 22 % longer than link; p99 no significant difference. A new stream's open,
  echo and close: link 30 % below yamux at the median, muxado not
  significantly different.
- **Opening streams: link 10.1 µs, muxado 15.0, yamux 17.9.** No stream
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
                            │     link     │                 yamux                 │                muxado                 │         tcp         │
                            │    sec/op    │    sec/op      vs base                │    sec/op      vs base                │    sec/op     vs base   │
Bulk/sessions=1/rtt=0s-10     7.456µ ± 29%   16.275µ ± 41%  +118.30% (p=0.000 n=8)   17.087µ ± 38%  +129.19% (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10    9.271µ ± 25%   11.598µ ± 21%   +25.09% (p=0.001 n=8)   16.153µ ± 14%   +74.23% (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10   2.644m ±  1%   10.439m ±  1%  +294.81% (p=0.000 n=8)   10.573m ±  2%  +299.88% (p=0.000 n=8)
Latency/raw-10                                                                                                               14.72µ ± 17%
Latency/held-10               22.16µ ± 13%    24.38µ ± 18%         ~ (p=0.161 n=8)    26.11µ ± 30%   +17.82% (p=0.007 n=8)
Latency/fresh-10              30.45µ ± 47%    48.97µ ± 19%   +60.84% (p=0.007 n=8)    34.03µ ± 23%         ~ (p=0.234 n=8)
Open-10                       10.10µ ± 17%    17.88µ ± 28%   +77.03% (p=0.000 n=8)    14.95µ ±  7%   +47.99% (p=0.000 n=8)
IdleMemory/n=1000-10          31.78m ± 25%    38.62m ±  3%   +21.52% (p=0.050 n=8)    37.09m ±  8%   +16.71% (p=0.050 n=8)
geomean                       87.60µ          148.9µ         +70.01%                  146.3µ         +67.06%                 14.72µ        ? ¹ ²
¹ benchmark set differs from baseline; geomeans may not be comparable
² ratios must be >0 to compute geomean

                            │      link      │                yamux                 │                muxado                │
                            │      B/s       │      B/s       vs base               │      B/s       vs base               │
Bulk/sessions=1/rtt=0s-10      8.188Gi ± 25%   3.753Gi ± 39%  -54.17% (p=0.000 n=8)   3.573Gi ± 28%  -56.36% (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10     6.589Gi ± 29%   5.323Gi ± 22%  -19.21% (p=0.001 n=8)   3.779Gi ± 16%  -42.65% (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10   23.637Mi ±  1%   5.984Mi ±  2%  -74.68% (p=0.000 n=8)   5.908Mi ±  2%  -75.01% (p=0.000 n=8)
geomean                        1.076Gi         500.5Mi        -54.57%                 437.3Mi        -60.30%

                                     │      link      │                  yamux                   │                 muxado                  │         tcp         │
                                     │      B/op      │      B/op        vs base                 │      B/op       vs base                 │    B/op     vs base │
Bulk/sessions=1/rtt=0s-10                12.00 ± 117%     140.00 ±   9%  +1066.67% (p=0.000 n=8)      83.00 ±  7%   +591.67% (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10               18.50 ± 403%     245.50 ± 131%  +1227.03% (p=0.000 n=8)     542.50 ± 67%  +2832.43% (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10            80.12Ki ±   8%    73.65Ki ±   3%     -8.07% (p=0.000 n=8)    73.13Ki ±  1%     -8.71% (p=0.000 n=8)
Latency/raw-10                                                                                                                               0.000 ± 0%
Latency/held-10                          4.500 ±    ?    176.000 ±   1%  +3811.11% (p=0.000 n=8)     64.000 ±  2%  +1322.22% (p=0.000 n=8)
Latency/fresh-10                       36.42Ki ±   1%    37.70Ki ±   0%     +3.50% (p=0.000 n=8)    36.02Ki ±  0%     -1.10% (p=0.000 n=8)
Open-10                                34.13Ki ±   0%    35.95Ki ±   0%     +5.33% (p=0.000 n=8)    34.53Ki ±  0%     +1.18% (p=0.000 n=8)
IdleMemory/n=1000-10                   4.408Mi ±  26%    5.802Mi ±   0%    +31.63% (p=0.000 n=8)    4.162Mi ±  0%          ~ (p=0.105 n=8)
SlowReader/stall=1s/awriter=stops-10   5.878Mi ±  30%   18.616Mi ±  16%   +216.69% (p=0.000 n=8)    7.190Mi ± 24%          ~ (p=0.234 n=8)
SlowReader/stall=1s/awriter=keeps-10   7.745Mi ±  25%   18.867Mi ±  27%   +143.61% (p=0.000 n=8)    6.758Mi ± 22%    -12.74% (p=0.038 n=8)
SlowReader/stall=5s/awriter=stops-10   6.086Mi ±  14%   52.688Mi ±  22%   +765.72% (p=0.000 n=8)   16.877Mi ± 21%   +177.31% (p=0.000 n=8)
SlowReader/stall=5s/awriter=keeps-10   7.725Mi ±  21%   52.213Mi ±  27%   +575.92% (p=0.000 n=8)   17.609Mi ± 23%   +127.96% (p=0.000 n=8)
geomean                                43.23Ki           170.5Ki          +294.54%                  104.7Ki         +142.15%                             ? ¹ ² ³
¹ benchmark set differs from baseline; geomeans may not be comparable
² summaries must be >0 to compute geomean
³ ratios must be >0 to compute geomean

                                     │      link       │                   yamux                   │                 muxado                  │         tcp         │
                                     │    allocs/op    │    allocs/op     vs base                  │   allocs/op     vs base                 │ allocs/op   vs base │
Bulk/sessions=1/rtt=0s-10                0.000 ±  0%         2.000 ±  0%           ? (p=0.000 n=8)      2.000 ±  0%          ? (p=0.000 n=8)
Bulk/sessions=64/rtt=0s-10               0.000 ±  0%         2.000 ±  0%           ? (p=0.000 n=8)      2.000 ±  0%          ? (p=0.000 n=8)
Bulk/sessions=1/rtt=40ms-10              1.000 ±  0%         5.000 ±  0%    +400.00% (p=0.000 n=8)      6.000 ±  0%   +500.00% (p=0.000 n=8)
Latency/raw-10                                                                                                                                 0.000 ± 0%
Latency/held-10                          0.000 ±  0%         4.000 ±  0%           ? (p=0.000 n=8)      2.000 ±  0%          ? (p=0.000 n=8)
Latency/fresh-10                         23.00 ±  0%         44.00 ±  0%     +91.30% (p=0.000 n=8)      10.00 ±  0%    -56.52% (p=0.000 n=8)
Open-10                                 21.000 ±  0%        37.000 ±  0%     +76.19% (p=0.000 n=8)      7.000 ±  0%    -66.67% (p=0.000 n=8)
IdleMemory/n=1000-10                   22.043k ±  0%       44.484k ±  0%    +101.81% (p=0.000 n=8)     8.113k ±  0%    -63.19% (p=0.000 n=8)
SlowReader/stall=1s/awriter=stops-10    5.572k ± 23%      355.093k ± 23%   +6272.81% (p=0.000 n=8)   143.855k ± 24%  +2481.75% (p=0.000 n=8)
SlowReader/stall=1s/awriter=keeps-10    4.263k ± 37%      377.984k ± 37%   +8765.58% (p=0.000 n=8)   130.406k ± 20%  +2958.67% (p=0.000 n=8)
SlowReader/stall=5s/awriter=stops-10    4.814k ± 21%     1123.814k ± 23%  +23242.28% (p=0.000 n=8)   466.365k ± 22%  +9586.67% (p=0.000 n=8)
SlowReader/stall=5s/awriter=keeps-10    5.903k ± 35%     1125.576k ± 29%  +18967.87% (p=0.000 n=8)   496.235k ± 26%  +8306.49% (p=0.000 n=8)
geomean                                              ¹       998.5        ?                             439.6        ?                                     ? ² ¹ ³
¹ summaries must be >0 to compute geomean
² benchmark set differs from baseline; geomeans may not be comparable
³ ratios must be >0 to compute geomean

                 │     link     │                yamux                │               muxado               │        tcp         │
                 │   p50-sec    │   p50-sec     vs base               │   p50-sec    vs base               │   p50-sec    vs base   │
Latency/raw-10                                                                                               13.31µ ± 1%
Latency/held-10    20.15µ ± 10%   22.10µ ± 15%   +9.72% (p=0.013 n=8)   24.54µ ± 4%  +21.82% (p=0.000 n=8)
Latency/fresh-10   28.17µ ± 26%   40.29µ ± 11%  +43.04% (p=0.002 n=8)   31.54µ ± 9%        ~ (p=0.061 n=8)
geomean            23.82µ         29.84µ        +25.28%                 27.82µ       +16.80%                 13.31µ       ? ¹ ²
¹ benchmark set differs from baseline; geomeans may not be comparable
² ratios must be >0 to compute geomean

                 │     link     │                yamux                 │               muxado                │         tcp         │
                 │   p99-sec    │    p99-sec     vs base               │   p99-sec     vs base               │   p99-sec     vs base   │
Latency/raw-10                                                                                                 35.98µ ± 42%
Latency/held-10    49.42µ ± 38%    53.00µ ± 35%        ~ (p=0.505 n=8)   58.35µ ± 43%        ~ (p=0.195 n=8)
Latency/fresh-10   83.71µ ± 52%   124.83µ ± 18%  +49.13% (p=0.028 n=8)   90.40µ ± 22%        ~ (p=0.442 n=8)
geomean            64.32µ          81.34µ        +26.47%                 72.63µ        +12.93%                 35.98µ        ? ¹ ²
¹ benchmark set differs from baseline; geomeans may not be comparable
² ratios must be >0 to compute geomean

        │    link    │             yamux             │            muxado             │
        │  lost/op   │  lost/op    vs base           │  lost/op    vs base           │
Open-10   0.000 ± 0%   0.000 ± 0%  ~ (p=1.000 n=8) ¹   0.000 ± 0%  ~ (p=1.000 n=8) ¹
¹ all samples are equal

                     │        link        │                 yamux                 │                muxado                 │
                     │ goroutines/session │ goroutines/session  vs base           │ goroutines/session  vs base           │
IdleMemory/n=1000-10           0.000 ± 0%           0.000 ± 0%  ~ (p=1.000 n=8) ¹           0.000 ± 0%  ~ (p=1.000 n=8) ¹
¹ all samples are equal

                     │      link      │                 yamux                 │                muxado                 │
                     │ heap-B/session │ heap-B/session  vs base               │ heap-B/session  vs base               │
IdleMemory/n=1000-10     1.968Ki ± 1%     3.876Ki ± 0%  +97.00% (p=0.000 n=8)     3.063Ki ± 0%  +55.68% (p=0.000 n=8)

                                     │     link      │                yamux                 │                muxado                │
                                     │ A-B/s-before  │ A-B/s-before   vs base               │ A-B/s-before   vs base               │
SlowReader/stall=1s/awriter=stops-10   3.447Gi ± 23%   1.697Gi ± 17%  -50.77% (p=0.000 n=8)   1.612Gi ± 26%  -53.23% (p=0.000 n=8)
SlowReader/stall=1s/awriter=keeps-10   2.862Gi ± 28%   1.850Gi ± 37%  -35.37% (p=0.000 n=8)   1.321Gi ± 25%  -53.85% (p=0.000 n=8)
SlowReader/stall=5s/awriter=stops-10   3.224Gi ± 18%   1.547Gi ± 36%  -52.02% (p=0.000 n=8)   1.304Gi ± 24%  -59.56% (p=0.000 n=8)
SlowReader/stall=5s/awriter=keeps-10   3.721Gi ± 34%   1.646Gi ± 62%  -55.76% (p=0.000 n=8)   1.467Gi ± 17%  -60.58% (p=0.000 n=8)
geomean                                3.298Gi         1.681Gi        -49.02%                 1.421Gi        -56.93%

                                     │     link     │               yamux                │               muxado               │
                                     │  A-resumed   │ A-resumed   vs base                │ A-resumed   vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=1s/awriter=keeps-10   1.000 ± 0%     1.000 ± 0%       ~ (p=1.000 n=8) ¹   1.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=keeps-10   1.000 ± 0%     1.000 ± 0%       ~ (p=1.000 n=8) ¹   1.000 ± 0%       ~ (p=1.000 n=8) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                                     │     link      │                yamux                 │                muxado                │
                                     │ B-B/s-before  │ B-B/s-before   vs base               │ B-B/s-before   vs base               │
SlowReader/stall=1s/awriter=stops-10   3.445Gi ± 24%   1.699Gi ± 16%  -50.69% (p=0.000 n=8)   1.616Gi ± 26%  -53.10% (p=0.000 n=8)
SlowReader/stall=1s/awriter=keeps-10   2.845Gi ± 29%   1.837Gi ± 36%  -35.42% (p=0.000 n=8)   1.321Gi ± 25%  -53.58% (p=0.000 n=8)
SlowReader/stall=5s/awriter=stops-10   3.275Gi ± 18%   1.556Gi ± 35%  -52.49% (p=0.000 n=8)   1.306Gi ± 24%  -60.12% (p=0.000 n=8)
SlowReader/stall=5s/awriter=keeps-10   3.722Gi ± 35%   1.653Gi ± 62%  -55.59% (p=0.000 n=8)   1.466Gi ± 20%  -60.60% (p=0.000 n=8)
geomean                                3.306Gi         1.683Gi        -49.09%                 1.422Gi        -56.99%

                                     │     link      │                yamux                 │                muxado                │
                                     │  B-B/s-stall  │  B-B/s-stall   vs base               │  B-B/s-stall   vs base               │
SlowReader/stall=1s/awriter=stops-10   6.539Gi ± 18%   3.155Gi ± 28%  -51.74% (p=0.000 n=8)   2.797Gi ± 27%  -57.23% (p=0.000 n=8)
SlowReader/stall=1s/awriter=keeps-10   5.825Gi ± 14%   3.256Gi ± 39%  -44.10% (p=0.000 n=8)   2.587Gi ± 28%  -55.59% (p=0.000 n=8)
SlowReader/stall=5s/awriter=stops-10   6.177Gi ± 22%   2.754Gi ± 23%  -55.42% (p=0.000 n=8)   2.599Gi ± 21%  -57.92% (p=0.000 n=8)
SlowReader/stall=5s/awriter=keeps-10   6.648Gi ± 20%   2.719Gi ± 28%  -59.09% (p=0.000 n=8)   2.746Gi ± 27%  -58.69% (p=0.000 n=8)
geomean                                6.289Gi         2.962Gi        -52.90%                 2.681Gi        -57.37%

                                     │     link     │               yamux                │               muxado               │
                                     │   B-gap-ms   │  B-gap-ms   vs base                │  B-gap-ms   vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=1s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=keeps-10   0.000 ± 0%     0.000 ±  ?       ~ (p=0.467 n=8)     0.000 ± 0%       ~ (p=1.000 n=8) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                                     │     link     │               yamux                │               muxado               │
                                     │  B-stalled   │ B-stalled   vs base                │ B-stalled   vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=1s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                                     │     link     │               yamux                │               muxado               │
                                     │    stuck     │   stuck     vs base                │   stuck     vs base                │
SlowReader/stall=1s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=1s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=stops-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
SlowReader/stall=5s/awriter=keeps-10   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=8) ¹   0.000 ± 0%       ~ (p=1.000 n=8) ¹
geomean                                           ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean
```
