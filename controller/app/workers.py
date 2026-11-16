"""Worker registry, placement and the HTTP calls to workers."""
import random
import time

import httpx

from . import db, events

http: httpx.AsyncClient = None


class Gone(Exception):
    """The worker answered, but it does not have the chunk."""


def start():
    global http
    http = httpx.AsyncClient(timeout=httpx.Timeout(5, connect=2))


async def stop():
    await http.aclose()


# A worker that just failed or timed out on a chunk call. It may be hung rather
# than dead, so heartbeats have not caught it yet. Until they do, try it last.
suspect = {}


def flag(wid):
    suspect[wid] = time.time() + 15


def shaky(wid):
    return suspect.get(wid, 0) > time.time()


def seen(wid, url, free):
    """Record a register/heartbeat. Returns True if the worker just came (back) up."""
    w = db.one("select status from workers where id=?", wid)
    db.x(
        "insert into workers(id, url, last_seen, status, free) values(?,?,?,'alive',?) "
        "on conflict(id) do update set url=excluded.url, last_seen=excluded.last_seen, "
        "status='alive', free=excluded.free",
        wid, url, time.time(), free,
    )
    back = w is None or w["status"] != "alive"
    if back:
        events.add("worker-up", f"{wid} is alive")
    return back


def alive():
    return db.q("select * from workers where status='alive' order by id")


def pick(n, skip=()):
    """Choose n live workers holding the fewest chunks, never one in `skip`."""
    load = {r["wid"]: r["n"] for r in db.q("select wid, count(*) n from replicas group by wid")}
    ws = [w for w in alive() if w["id"] not in skip]
    random.shuffle(ws)  # break ties differently each time
    ws.sort(key=lambda w: (shaky(w["id"]), load.get(w["id"], 0)))
    return ws[:n]


async def put(url, cid, data, cs):
    try:
        r = await http.put(f"{url}/internal/chunks/{cid}", content=data, headers={"x-checksum": cs})
        return r.status_code == 200 and r.json()["checksum"] == cs
    except httpx.HTTPError:
        return False


async def fetch(url, cid):
    r = await http.get(f"{url}/internal/chunks/{cid}")
    if r.status_code == 404:
        raise Gone(cid)
    r.raise_for_status()
    return r.content


async def drop(url, cid):
    try:
        await http.delete(f"{url}/internal/chunks/{cid}")
    except httpx.HTTPError:
        pass  # the audit removes leftovers once the worker is reachable again


async def inventory(w):
    """{chunk_id: checksum} for everything on the worker's disk, hashed fresh."""
    r = await http.get(
        f"{w['url']}/internal/workers/{w['id']}/chunks",
        params={"checksums": "true"}, timeout=120,
    )
    r.raise_for_status()
    return {c["chunk_id"]: c["checksum"] for c in r.json()["chunks"]}
