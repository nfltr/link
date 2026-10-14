#!/usr/bin/env python3
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
"""link vs yamux: runs the cases of ./lc (this directory) and writes one
JSON line per run to OUT/<mode>.jsonl; `summarize` prints the medians.

  python3 rig.py host   [OUT]   two processes on this machine (no netem)
  python3 rig.py docker [OUT]   each side in a container of its own
                                (cpusets, NET_ADMIN: tc netem, iptables)
  python3 rig.py summarize OUT

Env: REPS (default 5), CASES (comma list, default all of the mode's),
DUR (seconds per throughput run, default 5; 8 under loss), NETEMS (comma
list of netem labels), TRANSPORTS (comma list: mtls,plain; quic is mtls
only), CELLS (tput sessions/pool, default 1/1,8/1,8/4), LOAD_GATE (wait before each run until the 1-min
load average is below it, at most an hour per pass), FD_DUR (falsedead phase seconds, default 60), LIBS
(comma list, default link,yamux), KEEP=1 (leave the containers up).
"""
import json, os, statistics, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPS = int(os.environ.get("REPS", "5"))
LIBS = os.environ.get("LIBS", "link,yamux").split(",")
TRANSPORTS = {"mtls": ["-tls"], "plain": []}
if os.environ.get("TRANSPORTS"):  # quic is mtls only
    TRANSPORTS = {t: TRANSPORTS[t] for t in os.environ["TRANSPORTS"].split(",")}
NETEM = [("none", None), ("2ms-0.1%", "delay 2ms loss 0.1%"), ("20ms-1%", "delay 20ms loss 1%"), ("50ms-2%", "delay 50ms loss 2%"),
         # the internet hop (link vs quic): one-way delay, so RTT is twice it
         ("50ms-1%", "delay 50ms loss 1%"), ("100ms-3%", "delay 100ms loss 3%"), ("150ms-5%", "delay 150ms loss 5%")]
if os.environ.get("NETEMS"):
    NETEM = [n for n in NETEM if n[0] in os.environ["NETEMS"].split(",")]
FD_DUR = os.environ.get("FD_DUR", "60") + "s"  # falsedead: busy, then idle, this long each
OUT = None
GATE = float(os.environ.get("LOAD_GATE", "0"))  # wait before each run until the 1-min load is below this
GATE_BUDGET = [3600.0]  # seconds of waiting per pass, then runs go ahead (the load is recorded)
LOAD_START = [0.0]


def gate():
    while GATE and os.getloadavg()[0] >= GATE and GATE_BUDGET[0] > 0:
        time.sleep(5)
        GATE_BUDGET[0] -= 5
    LOAD_START[0] = os.getloadavg()[0]


def sh(*args, check=True, inp=None):
    p = subprocess.run(args, input=inp, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and p.returncode != 0:
        sys.exit("FAIL: %s\n%s" % (" ".join(args), p.stdout[-3000:]))
    return p.stdout


def record(mode, row):
    row["load"] = os.getloadavg()[0]
    row["load_start"] = LOAD_START[0]
    row["at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with open("%s/%s.jsonl" % (OUT, mode), "a") as f:
        f.write(json.dumps(row) + "\n")
    print(json.dumps({k: v for k, v in row.items() if not isinstance(v, (list, dict))}), flush=True)


def build():
    os.makedirs(OUT + "/bin", exist_ok=True)
    env = dict(os.environ, GOWORK="off", CGO_ENABLED="0")
    subprocess.run(["go", "build", "-o", OUT + "/bin/lc-host", "."], cwd=HERE, env=env, check=True)
    subprocess.run(["go", "build", "-o", OUT + "/bin/lc", "."], cwd=HERE, env=dict(env, GOOS="linux"), check=True)
    if not os.path.exists(OUT + "/certs/cert.pem"):
        sh(OUT + "/bin/lc-host", "gencert", "-certs", OUT + "/certs")


def interleave(rep):
    return LIBS if rep % 2 == 0 else LIBS[::-1]


def cases(default):
    return os.environ.get("CASES", default).split(",")


# ---- host: two processes per library and transport on loopback

def host():
    build()
    lc = OUT + "/bin/lc-host"
    procs, port = {}, 17000
    addrs = {}
    for lib in LIBS:
        for t, flags in TRANSPORTS.items():
            a = ["-addr", "127.0.0.1:%d" % port, "-raw", "127.0.0.1:%d" % (port + 1), "-ctl", "127.0.0.1:%d" % (port + 2)]
            addrs[(lib, t)] = a
            log = open("%s/host-b-%s-%s.log" % (OUT, lib, t), "a")
            procs[(lib, t)] = subprocess.Popen([lc, "serve", "-lib", lib, "-certs", OUT + "/certs"] + flags + a, stdout=log, stderr=log)
            port += 10
    with open(OUT + "/host-pids.txt", "w") as f:
        for k, p in procs.items():
            f.write("%s %s %d\n" % (k[0], k[1], p.pid))
    time.sleep(1)
    try:
        def run(lib, t, *args):
            gate()
            out = sh(lc, "run", "-lib", lib, "-certs", OUT + "/certs", *(TRANSPORTS[t] + addrs[(lib, t)] + list(args)))
            return json.loads(out.strip().splitlines()[-1])
        todo = cases("tput,lat,mem,busy,iso")
        dur = os.environ.get("DUR", "5")
        for rep in range(REPS):
            for t in TRANSPORTS:
                for c in todo:
                    for lib in interleave(rep):
                        if c == "tput":
                            for d in ("down", "up"):
                                for s in (1, 8):
                                    r = run(lib, t, "-test", "tput", "-sessions", str(s), "-dir", d, "-dur", dur + "s")
                                    record("host", dict(r, transport=t, rep=rep))
                        elif c == "lat":
                            record("host", dict(run(lib, t, "-test", "lat", "-n", "2000"), transport=t, rep=rep))
                        elif c == "mem":
                            for n, pre in ((1000, 0), (200, 1 << 20)):
                                record("host", dict(run(lib, t, "-test", "mem", "-n", str(n), "-preload", str(pre)), transport=t, rep=rep))
                        elif c == "busy":
                            record("host", dict(run(lib, t, "-test", "busy", "-n", "64"), transport=t, rep=rep))
                        elif c == "iso":
                            for m in ("a", "b"):
                                record("host", dict(run(lib, t, "-test", "iso", "-mode", m), transport=t, rep=rep))
                        time.sleep(1)
    finally:
        for p in procs.values():
            p.kill()
            p.wait()


# ---- docker: per library a dialer container (A) and a listener container (B)

IMAGE = "nfltr-link-faults-rig:1"  # the replica fault rig's image (scripts/replica-link-faults.py in nfltr)
NET, SUBNET = "lcmp", "172.30.88"
IP = {("link", "a"): SUBNET + ".11", ("link", "b"): SUBNET + ".12", ("yamux", "a"): SUBNET + ".21", ("yamux", "b"): SUBNET + ".22",
      ("quic", "a"): SUBNET + ".31", ("quic", "b"): SUBNET + ".32"}
NEWIP = {"link": SUBNET + ".111", "yamux": SUBNET + ".121", "quic": SUBNET + ".131"}  # netchange
CPUS = {"a": "0-3", "b": "4-7"}


def name(lib, side):
    return "lcmp-%s-%s" % (lib, side)


def dx(lib, side, *cmd, check=True):
    return sh("docker", "exec", name(lib, side), *cmd, check=check)


def rig_up():
    if not sh("docker", "images", "-q", IMAGE).strip():
        sh("docker", "build", "-q", "-t", IMAGE, "-", inp="FROM python:3.12-slim\nRUN apt-get update && apt-get install -y "
           "--no-install-recommends iptables iproute2 procps && rm -rf /var/lib/apt/lists/*\n")
    rig_down()
    sh("docker", "network", "create", "--subnet", SUBNET + ".0/24", NET)
    for (lib, side), ip in IP.items():
        if lib not in LIBS:
            continue
        cmd = ["sleep", "infinity"]
        if side == "b":
            cmd = ["sh", "-c", "while true; do /lc/bin/lc serve $(cat /lc/args-%s) >>/lc/b-%s.log 2>&1; sleep 1; done" % (lib, lib)]
        sh("docker", "run", "-d", "--name", name(lib, side), "--network", NET, "--ip", ip, "--cap-add", "NET_ADMIN",
           "--cpuset-cpus", CPUS[side], "-v", OUT + ":/lc", IMAGE, *cmd)


def rig_down():
    sh("docker", "rm", "-f", *[name(l, s) for l, s in IP], check=False)
    sh("docker", "network", "rm", NET, check=False)


def b_args(lib, t, live="default", gcp=False):
    a = "-lib %s -certs /lc/certs -live %s -addr 0.0.0.0:7000 -raw 0.0.0.0:7001 -ctl 0.0.0.0:7002 %s %s" % (
        lib, live, " ".join(TRANSPORTS[t]), "-gcpressure" if gcp else "")
    return a


def set_b(t, live="default", gcp=False):
    for lib in LIBS:
        want = b_args(lib, t, live, gcp)
        with open("%s/args-%s" % (OUT, lib), "w") as f:
            f.write(want)
        dx(lib, "b", "pkill", "-x", "lc", check=False)
    time.sleep(1.5)
    for lib in LIBS:
        for _ in range(100):
            out = sh("docker", "exec", name(lib, "a"), "/lc/bin/lc", "run", "-lib", lib, "-test", "ping", "-ctl", IP[(lib, "b")] + ":7002", check=False)
            if '"ok":true' in out:
                break
            time.sleep(0.2)
        else:
            sys.exit("FAIL: listener %s not up" % lib)


def drun(lib, t, live, *args):
    gate()
    b = IP[(lib, "b")]
    out = dx(lib, "a", "/lc/bin/lc", "run", "-lib", lib, "-certs", "/lc/certs", "-live", live, *TRANSPORTS[t],
             "-addr", b + ":7000", "-raw", b + ":7001", "-ctl", b + ":7002", *args)
    return json.loads(out.strip().splitlines()[-1])


def netem(spec):
    for lib in LIBS:
        for side, peer in (("a", "b"), ("b", "a")):
            dx(lib, side, "sh", "-c", "tc qdisc del dev eth0 root 2>/dev/null; true", check=False)
            if spec:
                dx(lib, side, "sh", "-c", "tc qdisc add dev eth0 root handle 1: prio && tc qdisc add dev eth0 parent 1:3 handle 30: "
                   "netem %s && tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst %s/32 flowid 1:3" % (spec, IP[(lib, peer)]))


def cpus(v):
    for lib in LIBS:
        for side in "ab":
            sh("docker", "update", "--cpus", str(v), name(lib, side))


def both(t, live, *args):
    """Runs a case for both libraries at the same time (separate container pairs)."""
    gate()
    procs = {}
    for lib in LIBS:
        b = IP[(lib, "b")]
        procs[lib] = subprocess.Popen(["docker", "exec", name(lib, "a"), "/lc/bin/lc", "run", "-lib", lib, "-certs", "/lc/certs",
                                       "-live", live, *TRANSPORTS[t], "-addr", b + ":7000", "-raw", b + ":7001", "-ctl", b + ":7002", *args],
                                      text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    res = {}
    for lib, p in procs.items():
        out, _ = p.communicate()
        try:
            res[lib] = json.loads(out.strip().splitlines()[-1])
        except (ValueError, IndexError):
            res[lib] = {"lib": lib, "error": out[-2000:]}
    return res


def docker():
    build()
    rig_up()
    try:
        todo = cases("tput,lat,iso,fair,part,kill,drain,falsedead,hs,hol,netchange")
        if "tput" in todo:
            for t in TRANSPORTS:
                set_b(t)
                for label, spec in NETEM:
                    netem(spec)
                    dur = os.environ.get("DUR", "5" if spec is None else "8") + "s"
                    cells = [tuple(c.split("/")) for c in os.environ.get("CELLS", "1/1,8/1,8/4").split(",")]
                    for rep in range(REPS):
                        for s, p in cells:
                            args = ("-test", "tput", "-sessions", s, "-pool", p, "-dur", dur)
                            if spec and "loss" in spec and label != "2ms-0.1%":  # network-bound: both at once
                                for lib, r in both(t, "default", *args).items():
                                    record("docker", dict(r, transport=t, netem=label, rep=rep, concurrent=True))
                            else:
                                for lib in interleave(rep):
                                    record("docker", dict(drun(lib, t, "default", *args), transport=t, netem=label, rep=rep))
                netem(None)
        if "lat" in todo:
            for t in TRANSPORTS:
                set_b(t)
                for rep in range(REPS):
                    for lib in interleave(rep):
                        record("docker", dict(drun(lib, t, "default", "-test", "lat", "-n", "2000"), transport=t, rep=rep))
        if "iso" in todo:
            set_b("mtls")
            for rep in range(min(REPS, 3)):
                for m in ("a", "b"):
                    for lib in interleave(rep):
                        record("docker", dict(drun(lib, "mtls", "default", "-test", "iso", "-mode", m), transport="mtls", rep=rep))
        if "fair" in todo:
            set_b("mtls")
            netem("delay 50ms loss 2%")
            for rep in range(min(REPS, 3)):
                for p in ("1", "4"):
                    for lib, r in both("mtls", "default", "-test", "fair", "-pool", p, "-dur", "30s").items():
                        record("docker", dict(r, transport="mtls", netem="50ms-2%", rep=rep, concurrent=True))
            netem(None)
        if "part" in todo:
            for live in ("default", "fast"):
                set_b("mtls", live)
                for rep in range(min(REPS, 3)):
                    for busy in ("0", "4", "16"):  # busy sessions beside 4 idle ones; 16: more credit outstanding than the socket buffers hold
                        for lib, r in both("mtls", live, "-test", "part", "-sessions", busy, "-dur", "30s").items():
                            record("docker", dict(r, transport="mtls", rep=rep, concurrent=True))
        if "kill" in todo:
            for t in TRANSPORTS:
                set_b(t)
                for rep in range(min(REPS, 3)):
                    for lib in interleave(rep):
                        record("docker", dict(drun(lib, t, "default", "-test", "kill"), transport=t, rep=rep))
                        set_b(t)
        if "drain" in todo:
            set_b("mtls")
            for rep in range(min(REPS, 3)):
                for olds in ("2", "8"):  # 2: one connection, room for more; 8: the pool at its 4
                    for lib in interleave(rep):
                        record("docker", dict(drun(lib, "mtls", "default", "-test", "drain", "-pool", "4", "-sessions", olds), transport="mtls", rep=rep))
                        set_b("mtls")
        if "falsedead" in todo:
            netem("delay 50ms loss 2%")
            for c in ("0.25", "0.1"):
                for live in ("fast", "default"):
                    set_b("mtls", live, gcp=True)
                    cpus(c)
                    for lib, r in both("mtls", live, "-test", "falsedead", "-dur", FD_DUR, "-gcpressure").items():
                        record("docker", dict(r, transport="mtls", cpus=c, netem="50ms-2%", rep=0, concurrent=True))
                    cpus(4)
            netem(None)
        if "hs" in todo:  # cold connect to first byte
            set_b("mtls")
            for label, spec in NETEM:
                netem(spec)
                for rep in range(REPS):
                    for lib in interleave(rep):
                        record("docker", dict(drun(lib, "mtls", "default", "-test", "hs", "-n", "20"), transport="mtls", netem=label, rep=rep))
            netem(None)
        if "hol" in todo:  # 8 downloads, a 5 % loss burst: do all stall together?
            set_b("mtls")
            for label, spec in (("20ms", "delay 20ms"), ("50ms", "delay 50ms")):
                netem(spec)
                for rep in range(REPS):
                    for lib in interleave(rep):
                        record("docker", dict(drun(lib, "mtls", "default", "-test", "hol", "-dur", "3s"), transport="mtls", netem=label, rep=rep))
            netem(None)
        if "netchange" in todo:  # this side's address changes mid-download
            for live in ("fast", "default"):
                set_b("mtls", live)
                for rep in range(min(REPS, 3)):
                    for lib in interleave(rep):
                        old = IP[(lib, "a")]
                        record("docker", dict(drun(lib, "mtls", live, "-test", "netchange", "-oldip", old, "-newip", NEWIP[lib]),
                                              transport="mtls", rep=rep))
                        dx(lib, "a", "sh", "-c", "ip addr add %s/24 dev eth0; ip addr del %s/32 dev eth0; true" % (old, NEWIP[lib]), check=False)
                        set_b("mtls", live)
    finally:
        if not os.environ.get("KEEP"):
            rig_down()


# ---- summary: medians per case and setting, link and yamux side by side

KEYS = ("test", "transport", "netem", "sessions", "pool", "dir", "n", "preload", "mode", "live", "cpus")


def flat(r):
    """Nested results (hol's before/during) as dotted fields."""
    out = {}
    for k, v in r.items():
        if isinstance(v, dict):
            out.update({"%s.%s" % (k, kk): vv for kk, vv in v.items()})
        else:
            out[k] = v
    return out


def summarize(path):
    rows = {}
    for mode in ("host", "docker"):
        p = "%s/%s.jsonl" % (path, mode)
        if not os.path.exists(p):
            continue
        for line in open(p):
            r = flat(json.loads(line))
            k = (mode,) + tuple(str(r.get(x, "")) for x in KEYS)
            rows.setdefault(k, {}).setdefault(r["lib"], []).append(r)
    for k in sorted(rows):
        libs = rows[k]
        print("## " + " ".join("%s=%s" % (n, v) for n, v in zip(("mode",) + KEYS, k) if v))
        fields = sorted({f for rs in libs.values() for r in rs for f, v in r.items()
                         if isinstance(v, (int, float)) and not isinstance(v, bool) and f not in KEYS + ("rep",)})
        for f in fields:
            vals = []
            for lib in LIBS:
                xs = [r[f] for r in libs.get(lib, []) if isinstance(r.get(f), (int, float))]
                vals.append("%s %.4g (n=%d)" % (lib, statistics.median(xs), len(xs)) if xs else "%s -" % lib)
            print("  %-26s %s" % (f, " | ".join(vals)))


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("host", "docker", "summarize"):
        sys.exit(__doc__)
    OUT = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else "out")
    os.makedirs(OUT, exist_ok=True)
    {"host": host, "docker": docker, "summarize": lambda: summarize(OUT)}[sys.argv[1]]()
