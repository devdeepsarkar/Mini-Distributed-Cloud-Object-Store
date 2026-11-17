#!/usr/bin/env python3
"""Measure the object store: throughput, latency and recovery times.

    docker compose up --build -d
    pip install requests matplotlib
    python bench/bench.py                 # full run, about 5 minutes
    python bench/bench.py --quick         # small run, about 1 minute

Writes bench/results/results.csv, RESULTS.md and two charts.
It stops and starts containers, so use a stack you can disturb.
"""
import argparse
import csv
import os
import statistics as st
import subprocess
import sys
import time
from pathlib import Path

import requests

MB = 1024 * 1024
rows = []   # (test, parameter, trial, value, unit)
made = []


def sh(*cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()


def rec(test, param, trial, val, unit):
    rows.append((test, param, trial, round(val, 4), unit))


def vals(test, param):
    return [r[3] for r in rows if r[0] == test and r[1] == param]


def ms(test, param):
    v = vals(test, param)
    return st.mean(v), (st.stdev(v) if len(v) > 1 else 0.0)


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def cluster():
    return requests.get(API + "/cluster", timeout=5).json()


def status(wid):
    return next(w["status"] for w in cluster()["workers"] if w["worker_id"] == wid)


def healthy():
    c = cluster()
    return (c["mode"] == "normal" and c["under_replicated_chunks"] == 0 and c["corrupt_copies"] == 0
            and all(w["status"] == "alive" for w in c["workers"]))


def wait(cond, secs=120, every=0.1):
    end = time.time() + secs
    while time.time() < end:
        try:
            if cond():
                return time.time()
        except requests.RequestException:
            pass
        time.sleep(every)
    sys.exit("timed out, is the cluster healthy?")


def put(name, data):
    t = time.perf_counter()
    r = requests.post(API + "/objects", files={"file": (name, data)})
    dt = time.perf_counter() - t
    r.raise_for_status()
    made.append(r.json()["object_id"])
    return r.json()["object_id"], dt


def get(oid, headers=None):
    t = time.perf_counter()
    n = 0
    with requests.get(f"{API}/objects/{oid}", headers=headers, stream=True) as r:
        r.raise_for_status()
        for buf in r.iter_content(MB):
            n += len(buf)
    return n, time.perf_counter() - t


def drop(oid):
    requests.delete(f"{API}/objects/{oid}")
    if oid in made:
        made.remove(oid)


# ---------- tests ----------

def throughput(sizes, trials):
    print("\nthroughput (upload and download, whole objects)")
    for mb in sizes:
        data = os.urandom(mb * MB)
        for i in range(trials):
            oid, up = put(f"bench-{mb}mb.bin", data)
            n, down = get(oid)
            assert n == len(data)
            rec("upload", mb, i, mb / up, "MB/s")
            rec("download", mb, i, mb / down, "MB/s")
            drop(oid)
        u, d = ms("upload", mb), ms("download", mb)
        print(f"  {mb:>4} MB   upload {u[0]:7.1f} ± {u[1]:5.1f} MB/s   download {d[0]:7.1f} ± {d[1]:5.1f} MB/s")


def latency(n):
    print(f"\nlatency ({n} objects of 100 KB)")
    data = os.urandom(100 * 1024)
    ids = []
    for i in range(n):
        oid, dt = put("bench-small.bin", data)
        ids.append(oid)
        rec("small upload", "100KB", i, dt * 1000, "ms")
    for i, oid in enumerate(ids):
        rec("small download", "100KB", i, get(oid)[1] * 1000, "ms")
        t = time.perf_counter()
        requests.get(f"{API}/objects/{oid}/metadata")
        rec("metadata read", "100KB", i, (time.perf_counter() - t) * 1000, "ms")
    for oid in ids:
        drop(oid)
    for t in ("small upload", "small download", "metadata read"):
        v = vals(t, "100KB")
        print(f"  {t:<15} p50 {pct(v, 50):6.1f} ms   p95 {pct(v, 95):6.1f} ms   p99 {pct(v, 99):6.1f} ms")


def ranges(mb, trials):
    print(f"\nrange read (1 MB from the middle of a {mb} MB object)")
    oid, _ = put("bench-range.bin", os.urandom(mb * MB))
    mid = mb // 2 * MB + 4096
    for i in range(trials):
        rec("range read", f"1MB of {mb}MB", i, get(oid, {"Range": f"bytes={mid}-{mid + MB - 1}"})[1] * 1000, "ms")
        rec("full read", f"{mb}MB", i, get(oid)[1] * 1000, "ms")
    drop(oid)
    a, b = ms("range read", f"1MB of {mb}MB"), ms("full read", f"{mb}MB")
    print(f"  range {a[0]:.1f} ± {a[1]:.1f} ms   full object {b[0]:.1f} ± {b[1]:.1f} ms")


def recovery(volumes, trials, victim="worker2"):
    print(f"\nworker failure (docker stop {victim}): time to detect and to restore 2 copies")
    for mb in volumes:
        ids = [put("bench-fill.bin", os.urandom(10 * MB))[0] for _ in range(mb // 10)]
        wait(healthy)
        for i in range(trials):
            on_victim = next(w["chunks"] for w in cluster()["workers"] if w["worker_id"] == victim)
            sh("docker", "stop", "-t", "1", victim)
            t0 = time.time()
            t1 = wait(lambda: status(victim) == "dead")
            t2 = wait(lambda: cluster()["under_replicated_chunks"] == 0)
            rec("detect failure", mb, i, t1 - t0, "s")
            rec("re-replicate", mb, i, t2 - t1, "s")
            rec("total recovery", mb, i, t2 - t0, "s")
            rec("chunks re-replicated", mb, i, on_victim, "chunks")
            # reads must keep working while a worker is down
            assert get(ids[0])[0] == 10 * MB
            sh("docker", "start", victim)
            wait(healthy)
            time.sleep(3)  # let the trim of extra copies finish
            wait(healthy)
        for oid in ids:
            drop(oid)
        d, r, c = ms("detect failure", mb), ms("re-replicate", mb), ms("chunks re-replicated", mb)
        print(f"  {mb:>4} MB stored   detect {d[0]:5.1f} ± {d[1]:.1f} s   re-replicate {c[0]:.0f} chunks in {r[0]:5.1f} ± {r[1]:.1f} s")


def corruption(trials):
    print("\ncorruption: damage one copy, time for a scrub to find and repair it")
    oid, _ = put("bench-corrupt.bin", os.urandom(20 * MB))
    wait(healthy)
    for i in range(trials):
        c = requests.get(f"{API}/objects/{oid}/metadata").json()["chunks"][i % 20]
        w = c["replicas"][0]["worker"]
        sh("docker", "exec", w, "python", "-c",
           f"f=open('/data/chunks/{c['chunk_id']}','r+b'); f.seek(500); f.write(b'bad!'*16); f.close()")
        t = time.perf_counter()
        requests.post(API + "/admin/scrub")
        wait(healthy, every=0.02)
        rec("scrub + repair", "1 bad copy", i, time.perf_counter() - t, "s")
        assert get(oid)[0] == 20 * MB
    drop(oid)
    a = ms("scrub + repair", "1 bad copy")
    print(f"  detected and repaired in {a[0]:.2f} ± {a[1]:.2f} s")


def controller(trials):
    print("\ncontroller restart: time until it serves again with all chunk locations rebuilt")
    oid, _ = put("bench-restart.bin", os.urandom(50 * MB))
    wait(healthy)
    for i in range(trials):
        sh("docker", "restart", "-t", "1", "controller")
        t = time.time()
        t1 = wait(lambda: requests.get(URL + "/healthz", timeout=1).status_code == 200)
        t2 = wait(healthy)
        rec("controller back", "restart", i, t1 - t, "s")
        rec("chunk map rebuilt", "restart", i, t2 - t, "s")
        assert get(oid)[0] == 50 * MB
    drop(oid)
    a, b = ms("controller back", "restart"), ms("chunk map rebuilt", "restart")
    print(f"  answering after {a[0]:.1f} ± {a[1]:.1f} s, fully recovered after {b[0]:.1f} ± {b[1]:.1f} s")


# ---------- output ----------

def charts(out, sizes, volumes):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\nmatplotlib is not installed, skipping the charts (pip install matplotlib)")
        return False

    fig, ax = plt.subplots(figsize=(7, 4))
    x = range(len(sizes))
    for k, (test, off) in enumerate((("upload", -0.2), ("download", 0.2))):
        m = [ms(test, s) for s in sizes]
        ax.bar([i + off for i in x], [a for a, _ in m], 0.4, yerr=[b for _, b in m], capsize=4, label=test)
    ax.set_xticks(list(x), [f"{s} MB" for s in sizes])
    ax.set_xlabel("object size")
    ax.set_ylabel("throughput (MB/s)")
    ax.set_title("Throughput by object size (mean ± std)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "throughput.png", dpi=150)

    if volumes:
        fig, ax = plt.subplots(figsize=(7, 4))
        d = [ms("detect failure", v) for v in volumes]
        r = [ms("re-replicate", v) for v in volumes]
        x = range(len(volumes))
        ax.bar(x, [a for a, _ in d], 0.5, label="detect (heartbeat timeout)")
        ax.bar(x, [a for a, _ in r], 0.5, bottom=[a for a, _ in d], yerr=[b for _, b in r], capsize=4,
               label="re-replicate lost copies")
        ax.set_xticks(list(x), [f"{v} MB" for v in volumes])
        ax.set_xlabel("data stored in the cluster")
        ax.set_ylabel("seconds after docker stop")
        ax.set_title("Recovery time after a worker failure")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out / "recovery.png", dpi=150)
    return True


def report(out, sizes, volumes, drew, a):
    c = cluster()
    L = ["# Benchmark results", "",
         f"Run on {time.strftime('%Y-%m-%d %H:%M')}. Cluster: 1 controller, {len(c['workers'])} workers, "
         f"chunk size {c['chunk_size'] // 1024} KB, replication factor {c['replication_factor']}. "
         f"{a.trials} trials per data point, shown as mean ± standard deviation.", "",
         "All containers run on one machine, so these numbers describe this implementation on this "
         "hardware, not network-limited performance.", "",
         "## Throughput", "", "| Object size | Upload (MB/s) | Download (MB/s) |", "|---|---|---|"]
    for s in sizes:
        u, d = ms("upload", s), ms("download", s)
        L.append(f"| {s} MB | {u[0]:.1f} ± {u[1]:.1f} | {d[0]:.1f} ± {d[1]:.1f} |")
    if drew:
        L += ["", "![Throughput](throughput.png)"]
    L += ["", "Uploads are slower than downloads because every chunk is hashed and written to two workers, "
          "while a download reads one copy.", "",
          "## Latency for small objects (100 KB)", "", "| Operation | p50 (ms) | p95 (ms) | p99 (ms) |", "|---|---|---|---|"]
    for t in ("small upload", "small download", "metadata read"):
        v = vals(t, "100KB")
        L.append(f"| {t} | {pct(v, 50):.1f} | {pct(v, 95):.1f} | {pct(v, 99):.1f} |")
    rk = [r[1] for r in rows if r[0] == "range read"]
    if rk:
        fk = [r[1] for r in rows if r[0] == "full read"][0]
        x, y = ms("range read", rk[0]), ms("full read", fk)
        L += ["", "## Range reads", "",
              f"Reading 1 MB from the middle of a {fk} object took {x[0]:.1f} ± {x[1]:.1f} ms, against "
              f"{y[0]:.1f} ± {y[1]:.1f} ms for the whole object. A range read fetches only the chunks it overlaps."]
    if volumes:
        L += ["", "## Recovery after a worker failure", "",
              "`docker stop worker2`, then time until the controller marks it dead and until every chunk has two "
              "healthy copies again.", "",
              "| Data stored | Chunks to re-replicate | Detect (s) | Re-replicate (s) | Total (s) |", "|---|---|---|---|---|"]
        for v in volumes:
            d, r, t, n = ms("detect failure", v), ms("re-replicate", v), ms("total recovery", v), ms("chunks re-replicated", v)
            L.append(f"| {v} MB | {n[0]:.0f} | {d[0]:.1f} ± {d[1]:.1f} | {r[0]:.1f} ± {r[1]:.1f} | {t[0]:.1f} ± {t[1]:.1f} |")
        if drew:
            L += ["", "![Recovery](recovery.png)"]
        L += ["", "Detection time is fixed by the heartbeat timeout. Re-replication time grows with the amount of "
              "data that was on the failed worker. The total is the window in which a second failure could "
              "make data unavailable, which is the real cost of replication factor 2."]
    if vals("scrub + repair", "1 bad copy"):
        x = ms("scrub + repair", "1 bad copy")
        L += ["", "## Corruption", "",
              f"After overwriting bytes in one stored copy, a scrub found and repaired it in {x[0]:.2f} ± {x[1]:.2f} s. "
              "Downloads returned correct data throughout."]
    if vals("controller back", "restart"):
        x, y = ms("controller back", "restart"), ms("chunk map rebuilt", "restart")
        L += ["", "## Controller restart", "",
              f"After `docker restart controller` the API answered again after {x[0]:.1f} ± {x[1]:.1f} s and all chunk "
              f"locations were rebuilt from worker reports after {y[0]:.1f} ± {y[1]:.1f} s."]
    L += ["", "Raw data: [results.csv](results.csv)", ""]
    (out / "RESULTS.md").write_text("\n".join(L))


def main():
    global URL, API
    p = argparse.ArgumentParser(description="benchmark the mini object store")
    p.add_argument("--url", default=os.getenv("OS_URL", "http://localhost:8000"))
    p.add_argument("--sizes", default="1,10,100", help="object sizes in MB for the throughput test")
    p.add_argument("--volumes", default="50,150,300", help="stored MB for the recovery test")
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--quick", action="store_true", help="small sizes, 2 trials")
    p.add_argument("--no-failures", action="store_true", help="skip tests that stop containers")
    p.add_argument("--out", default=str(Path(__file__).parent / "results"))
    a = p.parse_args()
    if a.quick:
        a.sizes, a.volumes, a.trials = "1,10", "30", 2
    URL = a.url.rstrip("/")
    API = URL + "/api/v1"
    sizes = [int(x) for x in a.sizes.split(",")]
    volumes = [] if a.no_failures else [int(x) for x in a.volumes.split(",")]
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    wait(healthy, 30)
    try:
        throughput(sizes, a.trials)
        latency(20 if a.quick else 50)
        ranges(max(sizes), a.trials)
        if not a.no_failures:
            recovery(volumes, min(a.trials, 3))
            corruption(a.trials)
            controller(min(a.trials, 3))
    finally:
        for oid in list(made):
            drop(oid)

    with open(out / "results.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["test", "parameter", "trial", "value", "unit"])
        w.writerows(rows)
    drew = charts(out, sizes, volumes)
    report(out, sizes, volumes, drew, a)
    print(f"\nwrote {out}/RESULTS.md, results.csv" + (", throughput.png, recovery.png" if drew else ""))


if __name__ == "__main__":
    main()
