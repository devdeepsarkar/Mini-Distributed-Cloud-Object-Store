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


