# Soak

`make soak SOAKTIME=24h` runs link for as long as asked, under faults, and
writes a JSON summary (`SOAKOUT`, default `soak/out/soak.json`). The soak
is a module of its own (`soak/`), so quic-go stays out of link's
dependencies; GitHub Actions runs it for an hour weekly and keeps the
summary as an artifact.

## What it runs

Four pairs at once, each in epochs (`SOAKEPOCH`, default 5 min) on a
fresh pair:

| pair | transport | direction |
|---|---|---|
| link-forward | `peerlink.Node` over TCP through `peerlinktest.Proxy` | A dials B and opens |
| link-reverse | the same | B dials A (`Config.Connect`); A opens on B's connection |
| quic-forward | `quiclink` over UDP through `peerlinktest.UDPProxy`, mutual TLS | A dials B and opens |
| quic-reverse | the same | B dials A; A opens on B's connection |

Each pair's path runs at `SOAKRATE` (default 8 MiB/s each way) with
`link.LivenessFast` (dead-peer bound 4.5 s). In every epoch:

- `SOAKWORKERS` (default 6) workers open session after session and echo
  random bytes, 40 % under 1 KiB, 40 % up to 64 KiB, 20 % up to 4 MiB;
  each echo's SHA-256 and length must come back unchanged.
- Two stuck sessions: the accepter writes, the opener never reads, for
  the whole epoch; the echoes beside them must keep completing.
- At 25 % of the epoch the path is cut (TCP: every connection reset;
  UDP: the flows go silent), from 45 % to 55 % it is throttled to 1 MiB/s,
  at 65 % the UDP flows are rebound to new source ports (QUIC connection
  migration).
- At 85 % the path is black-holed with three idle probe sessions open:
  each end of each probe must read an error within the dead-peer bound
  (+2 s for a loaded host); the time is recorded.
- At the end both Links are drained (10 s deadline), the proxies closed,
  and the process's goroutines, file descriptors and heap sampled.

## What fails it

- an echo error outside a fault window (the fault plus its bound and
  10 s), or any echo that came back different;
- a probe that outlived the dead-peer bound + 2 s;
- a minute outside the fault windows in which no echo completed;
- a drain whose `Serve` did not return;
- leaks: at the end, more than 10 goroutines or 8 file descriptors over
  the sample after the first epoch, or a heap over twice it plus 16 MiB.

## Results

| date | where | hours | pairs | echoes | GiB echoed | unexpected errors | integrity | dead-peer max (bound 4.5 s) | leaks | result |
|---|---|---:|---|---:|---:|---:|---:|---|---|---|
| 2026-10-14 | macOS arm64, 10 CPUs, shared (load average 7-35 from other work) | 1.08 | all four, 12 epochs | 191882 | 77.7 | 0 | 0 failures | link 4.51 s, QUIC 5.22 s | none: 2 goroutines, 5 FDs after every epoch; heap 4-13 MiB in use | pass |
| 2026-10-14 | GitHub Actions ubuntu, linux/amd64, 4 CPUs | 1.08 | all four, 12 epochs | 172626 | 69.9 | 0 | 0 failures | link 4.50 s, QUIC 4.99 s | none: 2 goroutines, 9 FDs after every epoch; heap 5-10 MiB in use | pass |

Cumulative: 2.17 h. The summaries are in [`soak/results/`](soak/results/).

Per pair (that run): link-forward 52547 echoes (21.3 GiB), link-reverse
52377 (21.3 GiB), quic-forward 43482 (17.4 GiB), quic-reverse 43476
(17.6 GiB); 287-320 echoes per pair failed inside the fault windows, as
cuts and black holes make them; 12 cuts, throttles, black holes and
drains each, 12 rebinds for QUIC.

Finding: QUIC's dead peer was detected at up to 5.22 s (macOS, loaded)
and 4.99 s (Linux CI, otherwise idle) against `quiclink`'s stated bound
of 4.5 s (link: 4.50-4.51 s), within the soak's 2 s allowance; the
conformance suite's `DeadPeer` measured 3.0-4.5 s with fewer sessions.
Since it shows on an idle runner too, the stated bound likely misses a
term of quic-go's idle timer (with sessions busy until the black hole);
open, for quiclink.
