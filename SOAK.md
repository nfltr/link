# Soak

`make soak SOAKTIME=24h` runs link for as long as asked, under faults, and
writes a JSON summary (`SOAKOUT`, default `soak/out/soak.json`). The soak
is a module of its own (`soak/`), so quic-go stays out of link's
dependencies; GitHub Actions runs it for an hour weekly and keeps the
summary as an artifact.

## What it runs

Four pairs at once (and the chain-3hop path, "Chains" below), each in epochs (`SOAKEPOCH`, default 5 min) on a
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

## Chains

A fifth config, `chain-3hop` (since link v0.10.0), runs a path
`origin -> hop 1 -> hop 2 -> echo` of `peerlink.Node`s over TCP, each
link through a `peerlinktest.Proxy`, each hop a `peerlink.Forward`
(`soak/chainsoak`). Per epoch, beside `SOAKWORKERS` echo workers (each
waiting for the far end's answer, then checked by SHA-256):

- at 25 % the middle link is reset: hop 1 aborts what it joined
  (`CodeConnLost`, read by the origin as from its peer) and redials;
- at 50 % a stalled far reader's bytes in flight are measured: at most
  3 windows + 2 x 256 KiB (3.5 MiB with the default 1 MiB windows), or
  the run fails;
- at 70 %, in rotation: hop 2 killed (the origin reads `CodeConnLost`
  from hop 1), hop 2 drained (what it joined finishes, new opens are
  refused `CodeDraining`), or the last link black-holed (hop 2 sees it
  dead within the liveness bound and aborts toward the origin); then
  every Node drains.

Errors inside these windows are counted (`errors_in_fault_windows`, the
first typed aborts in `first_aborts`); any other error, an integrity
failure or an in-flight measure over its bound fails the run.
`TestChainSoakShort` is one 20 s epoch (`go test ./soak -run
TestChainSoakShort`).

### Across machines: `cmd/linkhop`

The same roles as separate processes, for a path whose hops are pods or
hosts (plaintext TCP with `link.NameAuth`: a trusted network such as a
cluster's pod network):

    go build -o linkhop ./soak/cmd/linkhop        # in soak/
    linkhop -role echo   -listen :7000
    linkhop -role hop    -listen :7000 -next echo:7000
    linkhop -role hop    -listen :7000 -next hop-2:7000
    linkhop -role origin -next hop-1:7000 -duration 1h -bound 3670016 -out chain.json

The origin runs `-workers` echo loops of mixed sizes and measures a
stalled far reader's in-flight bytes every `-inflight`; its JSON summary
counts echoes, bytes, integrity failures, errors by `*link.Abort` code,
hop and kind, and the largest in-flight measure. It exits 1 on an
integrity failure, on no echo completed, or on an in-flight measure over
`-bound` (links x window + hops x 256 KiB). Faults are yours (delete or
drain a hop's pod, a NetworkPolicy): errors during them are counted,
never fatal. `hop` and `echo` drain on SIGTERM (`-drain`, 30 s).

## What fails it

- a chain's stalled far reader holding more than its in-flight bound;
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
| 2026-10-14 | macOS arm64, 10 CPUs, shared; quiclink with the dead-peer fix | 0.18 | all four, 2 epochs | 33681 | 13.5 | 0 | 0 failures | link 4.50 s, QUIC 3.86 s | none: 2 goroutines, 5 FDs after every epoch | pass |
| 2026-10-16 | macOS arm64, 10 CPUs, shared (load average 14-26); link v0.10.0 with chain-3hop | 0.27 | all five, 3 epochs | 661867 (chain-3hop 612387) | 273.3 (chain-3hop 250.0) | 0 | 0 failures | link 4.50 s, QUIC 3.86 s | none: 2 goroutines, 5 FDs after every epoch; heap 6-8 MiB in use | pass |

Cumulative: 2.62 h. chain-3hop (2026-10-16): 3 middle-link cuts, one
hop 2 drained, one black-holed and one killed; 85 echoes failed inside
those windows, each with a typed `*link.Abort` (`CodeConnLost` from the
hop beside the fault); a stalled far reader held at most 3.31 MiB of its
3.5 MiB bound. The summaries are in [`soak/results/`](soak/results/).

Per pair (that run): link-forward 52547 echoes (21.3 GiB), link-reverse
52377 (21.3 GiB), quic-forward 43482 (17.4 GiB), quic-reverse 43476
(17.6 GiB); 287-320 echoes per pair failed inside the fault windows, as
cuts and black holes make them; 12 cuts, throttles, black holes and
drains each, 12 rebinds for QUIC.

Finding: QUIC's dead peer was detected at up to 5.22 s (macOS, loaded)
and 4.99 s (Linux CI, otherwise idle) against `quiclink`'s stated bound
of 4.5 s (link: 4.50-4.51 s), within the soak's 2 s allowance. The
missing term: QUIC's idle timer starts at the first packet sent after the
last one received, so a side that could not send when the path went
silent (writes stalled, congestion window full) timed out late by as long
as it could not send. Fixed in `peerlink/quiclink` v0.2.0, which closes a
connection silent for Interval + Timeout/2 itself
(`TestDeadPeerSendStalled`: 5.4 s before, within 4.5 s after).
