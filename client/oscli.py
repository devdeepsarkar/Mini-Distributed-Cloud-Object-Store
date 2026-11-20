#!/usr/bin/env python3
"""Command-line client for the mini object store.

    python oscli.py upload notes.pdf -m course=cloud -m owner=dev
    python oscli.py ls
    python oscli.py download <object_id> -o copy.pdf
    python oscli.py chunks <object_id>

The controller address comes from OS_URL (default http://localhost:8000).
"""
import argparse
import hashlib
import json
import mimetypes
import os
import sys

import requests

URL = os.getenv("OS_URL", "http://localhost:8000").rstrip("/")
API = URL + "/api/v1"


def die(r):
    try:
        e = r.json()["error"]
        sys.exit(f"error {r.status_code} {e['code']}: {e['message']}")
    except (ValueError, KeyError):
        sys.exit(f"error {r.status_code}: {r.text[:200]}")


def call(method, path, **kw):
    try:
        r = requests.request(method, API + path, **kw)
    except requests.ConnectionError:
        sys.exit(f"cannot reach the controller at {URL}")
    if r.status_code >= 400:
        die(r)
    return r


def pairs(items):
    """['a=1', 'b=x'] -> {'a': '1', 'b': 'x'}; 'a=' removes key a on update."""
    kv = {}
    for it in items or []:
        if "=" not in it:
            sys.exit(f"metadata must look like key=value, got '{it}'")
        k, v = it.split("=", 1)
        kv[k] = v
    return kv


def human(n):
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024 or u == "GB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024


def show_map(m):
    print(f"{m['name']}  {human(m['size'])}  {m['chunk_count']} chunk(s)")
    for c in m["chunks"]:
        where = []
        for r in c["replicas"]:
            tag = "" if r["alive"] and r["state"] == "ok" else f" ({'down' if not r['alive'] else r['state']})"
            where.append(r["worker"] + tag)
        print(f"  chunk {c['index']:>3}  {human(c['size']):>9}  copies={c['healthy_copies']}  {', '.join(where)}")


def upload(a):
    ct = mimetypes.guess_type(a.file)[0] or "application/octet-stream"
    data = {"metadata": json.dumps(pairs(a.meta))} if a.meta else {}
    with open(a.file, "rb") as f:
        m = call("POST", "/objects", files={"file": (os.path.basename(a.file), f, ct)}, data=data).json()
    print("object_id:", m["object_id"])
    show_map(m)


def download(a):
    m = call("GET", f"/objects/{a.id}/metadata").json()
    out = a.out or m["name"]
    h = hashlib.sha256()
    with call("GET", f"/objects/{a.id}", stream=True) as r, open(out, "wb") as f:
        for buf in r.iter_content(1 << 20):
            f.write(buf)
            h.update(buf)
    ok = h.hexdigest() == m["checksum"]
    print(f"saved {out} ({human(m['size'])}), checksum {'matches' if ok else 'DOES NOT MATCH'}")
    if not ok:
        sys.exit(1)


def ls(a):
    d = call("GET", "/objects", params={"limit": a.limit, "offset": a.offset}).json()
    for o in d["objects"]:
        print(f"{o['object_id']}  {human(o['size']):>9}  {o['content_type']:<28}  {o['name']}")
    print(f"{len(d['objects'])} of {d['total']} object(s)")


def meta(a):
    print(json.dumps(call("GET", f"/objects/{a.id}/metadata").json(), indent=2))


def setmeta(a):
    kv = {k: (v if v != "" else None) for k, v in pairs(a.meta).items()}
    # with --if-match the update is refused (412) if someone changed the object first
    hd = {"If-Match": f'"{a.if_match.strip(chr(34))}"'} if a.if_match else {}
    m = call("PATCH", f"/objects/{a.id}/metadata", json=kv, headers=hd).json()
    print("custom metadata:", json.dumps(m["custom_metadata"]), " version:", m["metadata_etag"])


def rm(a):
    call("DELETE", f"/objects/{a.id}")
    print("deleted", a.id)


def chunks(a):
    show_map(call("GET", f"/objects/{a.id}/metadata").json())


def status(a):
    c = call("GET", "/cluster").json()
    if c["mode"] != "normal":
        print("controller is in safe mode, rebuilding chunk locations from the workers")
    for w in c["workers"]:
        print(f"{w['worker_id']:<10} {w['status']:<6} {w['chunks']:>5} chunks   last heartbeat {w['last_heartbeat_seconds_ago']}s ago")
    print(f"{c['objects']} object(s), {c['chunks']} chunk(s), replication factor {c['replication_factor']}")
    print(f"under-replicated chunks: {c['under_replicated_chunks']}   corrupt copies: {c['corrupt_copies']}")


def events(a):
    from datetime import datetime
    for e in reversed(call("GET", "/events", params={"limit": a.limit}).json()["events"]):
        t = datetime.fromtimestamp(e["time"]).strftime("%H:%M:%S")
        print(f"{t}  {e['kind']:<12} {e['message']}")


def scrub(a):
    call("POST", "/admin/scrub")
    status(a)


def main():
    p = argparse.ArgumentParser(description="mini object store client")
    s = p.add_subparsers(dest="cmd", required=True)

    c = s.add_parser("upload", help="store a file")
    c.add_argument("file")
    c.add_argument("-m", "--meta", action="append", metavar="key=value", help="custom metadata, repeatable")
    c.set_defaults(fn=upload)

    c = s.add_parser("download", help="fetch an object and verify its checksum")
    c.add_argument("id")
    c.add_argument("-o", "--out", help="where to save it (default: the object's name)")
    c.set_defaults(fn=download)

    c = s.add_parser("ls", help="list objects")
    c.add_argument("--limit", type=int, default=100)
    c.add_argument("--offset", type=int, default=0)
    c.set_defaults(fn=ls)

    c = s.add_parser("meta", help="show all metadata of an object")
    c.add_argument("id")
    c.set_defaults(fn=meta)

    c = s.add_parser("set", help="add/update custom metadata (key= removes a key)")
    c.add_argument("id")
    c.add_argument("meta", nargs="+", metavar="key=value")
    c.add_argument("--if-match", metavar="ETAG", help="only update if the metadata version is still this, e.g. v3")
    c.set_defaults(fn=setmeta)

    c = s.add_parser("rm", help="delete an object")
    c.add_argument("id")
    c.set_defaults(fn=rm)

    c = s.add_parser("chunks", help="show where each chunk of an object is stored")
    c.add_argument("id")
    c.set_defaults(fn=chunks)

    c = s.add_parser("status", help="workers and replication health")
    c.set_defaults(fn=status)

    c = s.add_parser("events", help="recent cluster events")
    c.add_argument("--limit", type=int, default=30)
    c.set_defaults(fn=events)

    c = s.add_parser("scrub", help="verify every stored chunk now and repair")
    c.set_defaults(fn=scrub)

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
