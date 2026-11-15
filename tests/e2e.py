#!/usr/bin/env python3
"""End-to-end test against the running Docker stack.

    docker compose up --build -d
    pip install requests
    python tests/e2e.py

It stops and starts worker containers, so run it on a stack you can disturb.
Takes about two minutes, most of it waiting for heartbeat timeouts.
"""
import hashlib
import os
import subprocess
import sys
import time

import requests

URL = os.getenv("OS_URL", "http://localhost:8000")
API = URL + "/api/v1"
MB = 1024 * 1024
made = []


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sh(*cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=ROOT).stdout.strip()


def step(msg):
    print(f"\n== {msg}")


def ok(cond, msg):
    print(("  pass  " if cond else "  FAIL  ") + msg)
    if not cond:
        sys.exit(1)


def put(name, data, meta=None, ctype=None):
    files = {"file": (name, data, ctype) if ctype else (name, data)}
    r = requests.post(API + "/objects", files=files, data={"metadata": meta} if meta else None)
    if r.status_code == 201:
        made.append(r.json()["object_id"])
    return r


def meta(oid):
    return requests.get(f"{API}/objects/{oid}/metadata").json()


def cluster():
    return requests.get(API + "/cluster").json()


def status(wid):
    return next(w["status"] for w in cluster()["workers"] if w["worker_id"] == wid)


def wait(cond, secs, what):
    end = time.time() + secs
    while time.time() < end:
        if cond():
            return
        time.sleep(0.5)
    ok(False, f"timed out waiting for {what}")


def healthy():
    c = cluster()
    return c["under_replicated_chunks"] == 0 and c["corrupt_copies"] == 0


def placed_right(oid):
    """Every chunk has exactly RF healthy copies, all on different workers."""
    for c in meta(oid)["chunks"]:
        ws = [r["worker"] for r in c["replicas"] if r["alive"] and r["state"] == "ok"]
        if len(ws) != 2 or len(set(ws)) != 2:
            return False
    return True


def main():
    step("cluster is up")
    wait(lambda: sum(w["status"] == "alive" for w in cluster()["workers"]) == 4, 30, "4 live workers")
    ok(True, "4 workers alive")
    ok(requests.get(URL + "/healthz").json()["status"] == "ok", "healthz")

    step("upload / download round trip, at the chunk boundaries")
    sizes = {"empty.bin": 0, "tiny.txt": 11, "exact.bin": MB, "plus1.bin": MB + 1, "big.bin": 3 * MB + 500_000}
    want = {"empty.bin": 0, "tiny.txt": 1, "exact.bin": 1, "plus1.bin": 2, "big.bin": 4}
    blobs, ids = {}, {}
    for name, n in sizes.items():
        blobs[name] = os.urandom(n)
        r = put(name, blobs[name])
        ok(r.status_code == 201, f"{name}: 201 Created")
        m = r.json()
        ids[name] = m["object_id"]
        ok(m["chunk_count"] == want[name] and m["size"] == n, f"{name}: {n} bytes -> {m['chunk_count']} chunk(s)")
        ok(m["checksum"] == hashlib.sha256(blobs[name]).hexdigest(), f"{name}: checksum recorded")
        d = requests.get(f"{API}/objects/{ids[name]}")
        ok(d.status_code == 200 and d.content == blobs[name], f"{name}: download matches the original")
    big = ids["big.bin"]
    ok(requests.get(f"{API}/objects/{ids['tiny.txt']}").headers["content-type"].startswith("text/plain"), "content type kept")
    ok(all(c["size"] == MB for c in meta(big)["chunks"][:-1]) and meta(big)["chunks"][-1]["size"] == 500_000,
       "chunks are 1 MB, last one smaller")

    step("replication")
    ok(all(placed_right(i) for i in ids.values()), "every chunk has 2 copies on 2 different workers")
    used = {r["worker"] for c in meta(big)["chunks"] for r in c["replicas"]}
    ok(len(used) >= 3, f"big object spread over {len(used)} workers")

    step("metadata")
    r = put("doc.pdf", b"%PDF-fake", meta='{"owner": "dev", "year": 2026}', ctype="application/pdf")
    doc = r.json()["object_id"]
    m = meta(doc)
    ok(m["custom_metadata"] == {"owner": "dev", "year": 2026}, "custom metadata stored at upload")
    ok(m["content_type"] == "application/pdf" and m["name"] == "doc.pdf", "system metadata")
    time.sleep(1.1)
    r = requests.patch(f"{API}/objects/{doc}/metadata", json={"owner": None, "status": "final"})
    ok(r.status_code == 200 and r.json()["custom_metadata"] == {"year": 2026, "status": "final"},
       "PATCH adds, updates and removes keys")
    m2 = meta(doc)
    ok(m2["modified_at"] > m["modified_at"] and m2["created_at"] == m["created_at"], "modified_at moved, created_at did not")
    ok(m2["checksum"] == m["checksum"] and m2["chunks"] == m["chunks"], "object data untouched by the metadata update")

    step("listing")
    d = requests.get(API + "/objects").json()
    ok({o["object_id"] for o in d["objects"]} >= set(made), "all uploaded objects listed")
    ok(all(k in d["objects"][0] for k in ("object_id", "name", "size", "content_type")), "entries have id, name, size, type")
    p = requests.get(API + "/objects", params={"limit": 2, "offset": 1}).json()
    ok(len(p["objects"]) == 2 and p["total"] == d["total"], "pagination")

    step("error handling")
    def code(r):
        return r.status_code, r.json().get("error", {}).get("code")
    ok(code(requests.get(API + "/objects/nope")) == (404, "not_found"), "GET missing object -> 404")
    ok(code(requests.get(API + "/objects/nope/metadata")) == (404, "not_found"), "GET missing metadata -> 404")
    ok(code(requests.patch(API + "/objects/nope/metadata", json={})) == (404, "not_found"), "PATCH missing -> 404")
    ok(code(requests.delete(API + "/objects/nope")) == (404, "not_found"), "DELETE missing -> 404")
    ok(code(requests.post(API + "/objects")) == (400, "bad_request"), "upload without file -> 400")
    ok(code(put("x", b"x", meta="[1]")) == (400, "bad_request"), "metadata that is not an object -> 400")
    ok(code(put("x", b"x", meta="{oops")) == (400, "bad_request"), "metadata that is not JSON -> 400")
    ok(code(requests.patch(f"{API}/objects/{doc}/metadata", data="nope")) == (400, "bad_request"), "PATCH bad body -> 400")

    step("delete")
    before = cluster()["chunks"]
    r = requests.delete(f"{API}/objects/{ids['plus1.bin']}")
    ok(r.status_code == 204 and r.content == b"", "DELETE -> 204 No Content")
    ok(requests.get(f"{API}/objects/{ids['plus1.bin']}").status_code == 404, "deleted object is gone")
    made.remove(ids["plus1.bin"])
    ok(cluster()["chunks"] == before - 2, "its chunks were removed")
    left = sh("docker", "exec", "worker1", "sh", "-c", "ls /data/chunks; ls /data/chunks | wc -l")
    ok(ids["plus1.bin"] not in left, "no chunk files left on worker1")

    step("worker failure: docker stop worker2")
    sh("docker", "stop", "worker2")
    d = requests.get(f"{API}/objects/{big}")
    ok(d.status_code == 200 and d.content == blobs["big.bin"], "download works right after the failure")
    wait(lambda: status("worker2") == "dead", 30, "worker2 to be marked dead")
    ok(True, "controller detected the dead worker")
    wait(healthy, 60, "re-replication")
    ok(all(placed_right(i) for i in made), "replication factor 2 restored on the 3 live workers")
    r = put("during.bin", os.urandom(2 * MB + 5))
    ok(r.status_code == 201 and placed_right(r.json()["object_id"]), "uploads still work with 3 workers")
    gone = ids["exact.bin"]
    ok(requests.delete(f"{API}/objects/{gone}").status_code == 204, "delete while worker2 is down")
    made.remove(gone)

    step("worker recovery: docker start worker2")
    sh("docker", "start", "worker2")
    wait(lambda: status("worker2") == "alive", 30, "worker2 to come back")

    def trimmed():
        return all(len(c["replicas"]) == 2 for c in meta(big)["chunks"])
    wait(trimmed, 30, "extra copies to be trimmed")
    ok(placed_right(big), "back to exactly 2 copies per chunk")
    time.sleep(2)
    files = sh("docker", "exec", "worker2", "ls", "/data/chunks")
    ok(gone not in files, "chunks of the object deleted meanwhile were cleaned off worker2")
    d = requests.get(f"{API}/objects/{big}")
    ok(d.content == blobs["big.bin"], "download still matches")

    step("data integrity: corrupt one copy on disk")
    c = meta(big)["chunks"][1]
    w = c["replicas"][0]["worker"]
    sh("docker", "exec", w, "python", "-c",
       f"f=open('/data/chunks/{c['chunk_id']}','r+b'); f.seek(1000); f.write(b'garbage!'*8); f.close()")
    for _ in range(6):
        d = requests.get(f"{API}/objects/{big}")
        ok(d.status_code == 200 and d.content == blobs["big.bin"], "download returns correct data")
    requests.post(API + "/admin/scrub")
    kinds = [e["kind"] for e in requests.get(API + "/events", params={"limit": 100}).json()["events"]]
    ok("corrupt" in kinds, "corruption was detected")
    wait(healthy, 30, "corrupt copy to be repaired")
    fixed = sh("docker", "exec", w, "sha256sum", f"/data/chunks/{c['chunk_id']}").split()[0]
    ok(fixed == c["checksum"], f"the bad copy on {w} was rewritten with good data")

    step("data integrity: delete a chunk file behind the store's back")
    c = meta(big)["chunks"][0]
    w = c["replicas"][0]["worker"]
    sh("docker", "exec", w, "rm", f"/data/chunks/{c['chunk_id']}")
    d = requests.get(f"{API}/objects/{big}")
    ok(d.content == blobs["big.bin"], "download still correct")
    requests.post(API + "/admin/scrub")
    wait(healthy, 30, "missing copy to be restored")
    ok(placed_right(big), "missing copy replaced")

    step("not enough workers: stop 3 of 4")
    for w in ("worker1", "worker2", "worker3"):
        sh("docker", "stop", w)
    wait(lambda: sum(w["status"] == "alive" for w in cluster()["workers"]) == 1, 30, "3 workers to be marked dead")
    r = put("nope.bin", b"x" * 100)
    ok(code(r) == (503, "storage_unavailable"), "upload -> 503 when 2 copies cannot be stored")
    for w in ("worker1", "worker2", "worker3"):
        sh("docker", "start", w)
    wait(lambda: sum(w["status"] == "alive" for w in cluster()["workers"]) == 4, 30, "workers to return")
    wait(healthy, 60, "cluster to settle")
    d = requests.get(f"{API}/objects/{big}")
    ok(d.status_code == 200 and d.content == blobs["big.bin"], "all data intact after the outage")

    step("controller restart: metadata is persistent")
    n = cluster()["objects"]
    sh("docker", "restart", "controller")

    def up():
        try:
            return requests.get(URL + "/healthz", timeout=2).json()["live_workers"] == 4
        except requests.RequestException:
            return False
    wait(up, 40, "controller to come back")
    ok(cluster()["objects"] == n, "same objects after restart")
    d = requests.get(f"{API}/objects/{big}")
    ok(d.content == blobs["big.bin"], "download matches after restart")
    ok(meta(doc)["custom_metadata"] == {"year": 2026, "status": "final"}, "custom metadata survived")

    step("409 Conflict: delete during a download")
    slow = put("slow.bin", os.urandom(40 * MB)).json()["object_id"]
    r = requests.get(f"{API}/objects/{slow}", stream=True)
    it = r.iter_content(MB)
    got = len(next(it))                      # download started, most of it still to come
    ok(code(requests.delete(f"{API}/objects/{slow}")) == (409, "conflict"), "DELETE while a download is running -> 409")
    got += sum(len(b) for b in it)
    ok(got == 40 * MB, "the running download finished complete")
    ok(requests.delete(f"{API}/objects/{slow}").status_code == 204, "DELETE after the download -> 204")
    made.remove(slow)

    step("500 Internal Server Error: a damaged metadata record")
    bad = put("broken.txt", b"some data", meta='{"a": 1}').json()["object_id"]
    sh("./scripts/break_metadata.sh", bad)
    r = requests.get(f"{API}/objects/{bad}/metadata")
    ok(code(r) == (500, "internal_error"), "unexpected internal failure -> 500 with a JSON error body")
    ok("Traceback" not in r.text and "JSONDecodeError" not in r.text, "no internal details leak to the client")
    ok(requests.get(f"{API}/objects/{doc}/metadata").status_code == 200, "other objects are unaffected")
    ok(requests.get(f"{API}/objects/{bad}").content == b"some data", "the damaged object's data still downloads")
    ok(requests.get(URL + "/healthz").json()["status"] == "ok", "the controller keeps running")
    sh("./scripts/break_metadata.sh", bad, "--fix")
    ok(requests.get(f"{API}/objects/{bad}/metadata").status_code == 200, "200 again once the record is repaired")

    step("ETag and conditional requests")
    r = requests.get(f"{API}/objects/{big}")
    tag = r.headers["etag"]
    ok(tag == f'"{hashlib.sha256(blobs["big.bin"]).hexdigest()}"', "object ETag is its SHA-256")
    r = requests.get(f"{API}/objects/{big}", headers={"If-None-Match": tag})
    ok(r.status_code == 304 and r.content == b"", "GET with a matching If-None-Match -> 304, no body")
    r = requests.get(f"{API}/objects/{doc}/metadata")
    v1 = r.headers["etag"]
    r2 = requests.get(f"{API}/objects/{doc}/metadata", headers={"If-None-Match": v1})
    ok(r2.status_code == 200 and r2.headers["cache-control"] == "no-store",
       "metadata is never served as 304 or cached (chunk locations change during repairs)")
    a = requests.patch(f"{API}/objects/{doc}/metadata", json={"editor": "alice"}, headers={"If-Match": v1})
    b = requests.patch(f"{API}/objects/{doc}/metadata", json={"editor": "bob"}, headers={"If-Match": v1})
    ok(a.status_code == 200 and a.headers["etag"] != v1, "first writer with If-Match wins and gets a new ETag")
    ok(code(b) == (412, "precondition_failed"), "second writer with the stale ETag -> 412")
    ok(meta(doc)["custom_metadata"]["editor"] == "alice", "the first update was not overwritten")
    ok(requests.patch(f"{API}/objects/{doc}/metadata", json={"x": 1}).status_code == 200, "PATCH without If-Match still works")
    ok(code(requests.delete(f"{API}/objects/{doc}", headers={"If-Match": v1})) == (412, "precondition_failed"),
       "DELETE with a stale ETag -> 412")
    ok(requests.get(f"{API}/objects/{doc}/metadata").status_code == 200, "object survived the refused delete")

    step("Range requests")
    data, n = blobs["big.bin"], len(blobs["big.bin"])
    r = requests.head(f"{API}/objects/{big}")
    ok(r.status_code == 200 and r.headers["accept-ranges"] == "bytes" and int(r.headers["content-length"]) == n,
       "HEAD gives size and Accept-Ranges")
    cases = {
        "inside one chunk": (f"bytes=10-99", data[10:100]),
        "across a chunk boundary": (f"bytes={MB - 5}-{MB + 4}", data[MB - 5:MB + 5]),
        "open ended": (f"bytes={n - 1000}-", data[-1000:]),
        "last 500 bytes": ("bytes=-500", data[-500:]),
        "end beyond the object is clamped": (f"bytes={n - 10}-{n + 999}", data[-10:]),
        "single byte": ("bytes=0-0", data[:1]),
        "three chunks": (f"bytes={MB // 2}-{2 * MB + MB // 2}", data[MB // 2:2 * MB + MB // 2 + 1]),
    }
    for name, (h, want) in cases.items():
        r = requests.get(f"{API}/objects/{big}", headers={"Range": h})
        ok(r.status_code == 206 and r.content == want and int(r.headers["content-length"]) == len(want), f"206: {name}")
    r = requests.get(f"{API}/objects/{big}", headers={"Range": f"bytes={MB - 5}-{MB + 4}"})
    ok(r.headers["content-range"] == f"bytes {MB - 5}-{MB + 4}/{n}", "Content-Range header")
    r = requests.get(f"{API}/objects/{big}", headers={"Range": f"bytes={n}-"})
    ok(code(r) == (416, "range_not_satisfiable") and r.headers["content-range"] == f"bytes */{n}", "start past the end -> 416")
    ok(requests.get(f"{API}/objects/{big}", headers={"Range": "lines=1-2"}).status_code == 200, "unknown range unit ignored -> 200")
    ev = requests.get(API + "/events", params={"limit": 20}).json()["events"]
    ok(any(e["kind"] == "range" and "chunk(s) 0-0 of 4" in e["message"] for e in ev), "a small range read touched only one chunk")

    step("metrics")
    m = requests.get(URL + "/metrics").text
    for name in ("objstore_under_replicated_chunks 0.0", 'objstore_workers{status="alive"} 4.0', "objstore_events_total",
                 "objstore_request_seconds_bucket", "objstore_uploaded_bytes_total", 'objstore_worker_chunks{worker="worker1"}'):
        ok(name in m, f"/metrics has {name.split('{')[0].split(' ')[0]}")

    step("controller loses its metadata database")
    before = {o["object_id"] for o in requests.get(API + "/objects").json()["objects"]}
    ok(requests.post(API + "/admin/backup").status_code == 200, "backup taken")
    sh("docker", "exec", "controller", "sh", "-c", "rm -f /data/meta.db /data/meta.db-wal /data/meta.db-shm")
    sh("docker", "kill", "controller")
    sh("docker", "start", "controller")

    def recovered():
        try:
            h = requests.get(URL + "/healthz", timeout=2).json()
            return h["mode"] == "normal" and h["live_workers"] == 4
        except requests.RequestException:
            return False
    wait(recovered, 60, "controller to recover")
    msgs = [e["message"] for e in requests.get(API + "/events", params={"limit": 100}).json()["events"] if e["kind"] == "recovery"]
    ok(any("restored from the backup" in x for x in msgs), "metadata restored from the backup volume")
    ok(any("chunk locations rebuilt" in x for x in msgs), "chunk locations rebuilt from worker reports")
    ok({o["object_id"] for o in requests.get(API + "/objects").json()["objects"]} == before, "same objects as before the crash")
    d = requests.get(f"{API}/objects/{big}")
    ok(d.status_code == 200 and d.content == blobs["big.bin"], "download matches after recovery")
    ok(meta(doc)["custom_metadata"]["editor"] == "alice", "custom metadata recovered")
    wait(healthy, 30, "cluster to settle")
    ok(all(placed_right(i) for i in made), "every chunk has 2 healthy copies again")

    step("cleanup")
    for i in made:
        requests.delete(f"{API}/objects/{i}")
    ok(True, "test objects deleted")
    print("\nALL TESTS PASSED")


if __name__ == "__main__":
    main()
