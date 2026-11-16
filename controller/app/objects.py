"""Object operations: chunked upload, verified download, metadata, delete."""
import asyncio
import hashlib
import json
import random
import uuid
from datetime import datetime, timezone

import httpx

from . import config, db, events, metrics, state, workers


class Unavailable(Exception):
    """Not enough storage is reachable right now (maps to 503)."""


class Conflict(Exception):
    """The object is in a state that does not allow this (maps to 409)."""


class BadRange(Exception):
    """The requested byte range lies outside the object (maps to 416)."""

    def __init__(self, size):
        self.size = size


def writable():
    if state.safe["on"]:
        raise Unavailable("controller just started and is still collecting chunk locations, retry shortly")


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha(b):
    return hashlib.sha256(b).hexdigest()


# ---------- upload ----------

async def place(cid, buf, cs):
    """Store one chunk on RF different workers, trying others if a write fails."""
    done, tried = 0, set()
    while done < config.RF:
        ws = workers.pick(config.RF - done, skip=tried)
        if not ws:
            raise Unavailable(f"could not store {config.RF} copies of a chunk")
        tried |= {w["id"] for w in ws}
        res = await asyncio.gather(*(workers.put(w["url"], cid, buf, cs) for w in ws))
        for w, ok in zip(ws, res):
            if ok:
                db.x("insert or replace into replicas values(?,?,'ok')", cid, w["id"])
                done += 1
            else:
                workers.flag(w["id"])


async def put_object(f, name, ctype, custom):
    writable()
    if len(workers.alive()) < config.RF:
        raise Unavailable(f"need {config.RF} live workers, have {len(workers.alive())}")

    oid = uuid.uuid4().hex
    t = now()
    db.x(
        "insert into objects(id, name, size, ctype, created, modified, checksum, nchunks, custom, state) "
        "values(?,?,?,?,?,?,?,?,?,'uploading')",
        oid, name, 0, ctype, t, t, "", 0, json.dumps(custom),
    )
    h, size, i = hashlib.sha256(), 0, 0
    try:
        while buf := await f.read(config.CHUNK):
            cid = f"{oid}-{i:05d}"
            cs = sha(buf)
            db.x("insert into chunks values(?,?,?,?,?)", cid, oid, i, len(buf), cs)
            await place(cid, buf, cs)
            h.update(buf)
            size += len(buf)
            i += 1
    except BaseException:
        await purge(oid)  # no half-stored objects
        raise

    db.x(
        "update objects set size=?, checksum=?, nchunks=?, state='ready' where id=?",
        size, h.hexdigest(), i, oid,
    )
    state.changed()
    metrics.BYTES_IN.inc(size)
    events.add("upload", f"{name} stored as {oid} ({i} chunks, {size} bytes)")
    return oid


# ---------- download ----------

async def good_copy(c):
    """Return (bytes, worker id) of a copy that passes its checksum.

    Bad copies found on the way are flagged so the repair loop fixes them.
    """
    reps = db.q(
        "select r.wid, w.url from replicas r join workers w on w.id=r.wid "
        "where r.cid=? and r.state='ok' and w.status='alive'", c["id"],
    )
    random.shuffle(reps)
    reps.sort(key=lambda r: workers.shaky(r["wid"]))  # workers that just failed go last
    for r in reps:
        try:
            data = await workers.fetch(r["url"], c["id"])
        except workers.Gone:
            db.x("delete from replicas where cid=? and wid=?", c["id"], r["wid"])
            events.add("missing", f"chunk {c['id']} is missing on {r['wid']}")
            continue
        except httpx.HTTPError:
            workers.flag(r["wid"])  # down or hung; the heartbeat timeout will settle which
            continue
        if sha(data) != c["checksum"]:
            db.x("update replicas set state='corrupt' where cid=? and wid=?", c["id"], r["wid"])
            events.add("corrupt", f"chunk {c['id']} on {r['wid']} failed its checksum, using another copy")
            continue
        return data, r["wid"]
    return None, None


def span(o, lo, hi):
    """The chunks that hold bytes lo..hi, each with the offset of its first byte."""
    out, pos = [], 0
    for c in db.q("select * from chunks where oid=? order by idx", o["id"]):
        if pos + c["size"] > lo and pos <= hi:
            out.append((c, pos))
        pos += c["size"]
    return out


def check_readable(o, lo, hi):
    if o["state"] != "ready":
        raise Conflict(f"object is {o['state']}")
    need = [c["id"] for c, _ in span(o, lo, hi)]
    lost = [r["id"] for r in db.q(
        "select c.id from chunks c where c.oid=? and not exists("
        "select 1 from replicas r join workers w on w.id=r.wid "
        "where r.cid=c.id and r.state='ok' and w.status='alive')", o["id"],
    ) if r["id"] in need]
    if lost:
        why = " (controller is still collecting chunk locations)" if state.safe["on"] else ""
        raise Unavailable(f"{len(lost)} chunk(s) have no reachable healthy copy{why}")


readers = {}  # object id -> downloads in progress, so a delete cannot cut one off


async def stream(o, lo, hi):
    """Yield bytes lo..hi of the object. Only the chunks in that range are read."""
    oid = o["id"]
    readers[oid] = readers.get(oid, 0) + 1
    try:
        for c, pos in span(o, lo, hi):
            data, _ = await good_copy(c)
            if data is None:
                raise Unavailable(f"chunk {c['idx']} has no reachable healthy copy")
            part = data[max(lo - pos, 0): hi - pos + 1]  # whole chunk verified, slice sent
            metrics.BYTES_OUT.inc(len(part))
            yield part
    finally:
        readers[oid] -= 1
        if not readers[oid]:
            del readers[oid]


# ---------- delete ----------

async def purge(oid):
    """Remove an object's chunks from every reachable worker, then its records."""
    db.x("update objects set state='deleting' where id=?", oid)
    reps = db.q(
        "select r.cid, w.url from replicas r join chunks c on c.id=r.cid "
        "join workers w on w.id=r.wid where c.oid=? and w.status='alive'", oid,
    )
    await asyncio.gather(*(workers.drop(r["url"], r["cid"]) for r in reps))
    # copies on dead workers are cleaned up as orphans when those workers return
    with db.tx():
        db.x("delete from replicas where cid in (select id from chunks where oid=?)", oid)
        db.x("delete from chunks where oid=?", oid)
        db.x("delete from objects where id=?", oid)
    state.changed()


async def delete_object(o):
    writable()
    if o["state"] != "ready":
        raise Conflict(f"object is {o['state']}")
    if readers.get(o["id"]):
        raise Conflict("object is being downloaded right now, try again when the download has finished")
    await purge(o["id"])
    events.add("delete", f"{o['name']} ({o['id']}) deleted")


# ---------- views ----------

def etag(o):
    """Changes only when the object's bytes change (they never do after upload)."""
    return f'"{o["checksum"]}"'


def meta_etag(o):
    """Changes on every metadata update; used for If-Match on PATCH and DELETE."""
    return f'"v{o["version"]}"'


def brief(o):
    return {
        "object_id": o["id"],
        "name": o["name"],
        "size": o["size"],
        "content_type": o["ctype"],
        "created_at": o["created"],
        "chunk_count": o["nchunks"],
    }


def meta(o):
    reps = {}
    for r in db.q(
        "select r.cid, r.wid, r.state, w.status from replicas r "
        "join chunks c on c.id=r.cid join workers w on w.id=r.wid "
        "where c.oid=? order by r.wid", o["id"],
    ):
        reps.setdefault(r["cid"], []).append(
            {"worker": r["wid"], "state": r["state"], "alive": r["status"] == "alive"}
        )
    chunks = []
    for c in db.q("select * from chunks where oid=? order by idx", o["id"]):
        rs = reps.get(c["id"], [])
        chunks.append({
            "chunk_id": c["id"],
            "index": c["idx"],
            "size": c["size"],
            "checksum": c["checksum"],
            "healthy_copies": sum(r["alive"] and r["state"] == "ok" for r in rs),
            "replicas": rs,
        })
    return {
        "object_id": o["id"],
        "name": o["name"],
        "size": o["size"],
        "content_type": o["ctype"],
        "created_at": o["created"],
        "modified_at": o["modified"],
        "checksum": o["checksum"],
        "checksum_algorithm": "sha256",
        "chunk_count": o["nchunks"],
        "chunk_size": config.CHUNK,
        "state": o["state"],
        "etag": etag(o),
        "metadata_version": o["version"],
        "metadata_etag": meta_etag(o),
        "custom_metadata": json.loads(o["custom"]),
        "chunks": chunks,
    }


def patch_custom(o, kv):
    """Merge keys into the custom metadata. A null value removes the key."""
    cur = json.loads(o["custom"])
    for k, v in kv.items():
        if v is None:
            cur.pop(k, None)
        else:
            cur[k] = v
    db.x(
        "update objects set custom=?, modified=?, version=version+1 where id=?",
        json.dumps(cur), now(), o["id"],
    )
    state.changed()
