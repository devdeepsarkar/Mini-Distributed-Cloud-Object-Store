"""Background jobs: failure detection, scrubbing and re-replication."""
import asyncio
import logging
import time

import httpx

from . import config, db, events, state, workers
from .objects import good_copy

log = logging.getLogger("store")
busy = asyncio.Lock()  # one audit/repair pass at a time
lost = set()           # chunks already reported as unrecoverable


def check_heartbeats():
    cut = time.time() - config.DEAD_AFTER
    for w in db.q("select id from workers where status='alive' and last_seen<?", cut):
        db.x("update workers set status='dead' where id=?", w["id"])
        events.add("worker-down", f"{w['id']} missed heartbeats for {config.DEAD_AFTER:.0f}s, marked dead")


async def audit(w):
    """Compare what a worker really has on disk with what the metadata says.

    Returns the number of chunks the worker reported, or None if it did not answer.
    """
    known = {
        r["cid"]: r for r in db.q(
            "select r.cid, r.state, c.checksum from replicas r "
            "join chunks c on c.id=r.cid where r.wid=?", w["id"],
        )
    }
    try:
        inv = await workers.inventory(w)
    except httpx.HTTPError:
        return None

    for cid, r in known.items():
        if cid not in inv:
            db.x("delete from replicas where cid=? and wid=?", cid, w["id"])
            events.add("missing", f"chunk {cid} is missing on {w['id']}")
        elif inv[cid] != r["checksum"] and r["state"] == "ok":
            db.x("update replicas set state='corrupt' where cid=? and wid=?", cid, w["id"])
            events.add("corrupt", f"scrub: chunk {cid} on {w['id']} failed its checksum")

    for cid in inv.keys() - known.keys():
        c = db.one(
            "select c.checksum, o.state from chunks c join objects o on o.id=c.oid where c.id=?", cid
        )
        if c is None:
            # belongs to an object deleted while this worker was away
            await workers.drop(w["url"], cid)
            events.add("orphan", f"removed leftover chunk {cid} from {w['id']}")
        elif c["state"] == "ready":
            st = "ok" if inv[cid] == c["checksum"] else "corrupt"
            db.x("insert or ignore into replicas values(?,?,?)", cid, w["id"], st)
            if st == "corrupt":
                events.add("corrupt", f"scrub: chunk {cid} on {w['id']} failed its checksum")
    return len(inv)


def counts(having):
    """Chunks of ready objects with their number of live, healthy copies."""
    return db.q(
        "select c.*, count(g.cid) n from chunks c "
        "join objects o on o.id=c.oid and o.state='ready' "
        "left join (select r.cid from replicas r join workers w on w.id=r.wid "
        "           where r.state='ok' and w.status='alive') g on g.cid=c.id "
        f"group by c.id having {having}", config.RF,
    )


async def repair():
    # 1. overwrite corrupt copies in place with a verified good one
    for b in db.q(
        "select r.cid, r.wid, w.url from replicas r join workers w on w.id=r.wid "
        "where r.state='corrupt' and w.status='alive'"
    ):
        c = db.one("select * from chunks where id=?", b["cid"])
        if c is None:
            continue
        data, src = await good_copy(c)
        if data is None:
            give_up(c)
        elif await workers.put(b["url"], c["id"], data, c["checksum"]):
            db.x("update replicas set state='ok' where cid=? and wid=?", c["id"], b["wid"])
            events.add("repair", f"chunk {c['id']} on {b['wid']} rewritten from {src}")

    # 2. bring under-replicated chunks back to RF copies
    for c in counts("n < ?"):
        data, src = await good_copy(c)
        if data is None:
            give_up(c)
            continue
        lost.discard(c["id"])
        have = {r["wid"] for r in db.q("select wid from replicas where cid=?", c["id"])}
        for w in workers.pick(config.RF - c["n"], skip=have):
            if not await workers.put(w["url"], c["id"], data, c["checksum"]):
                continue
            if db.one("select 1 from chunks where id=?", c["id"]) is None:
                await workers.drop(w["url"], c["id"])  # object deleted meanwhile
                break
            db.x("insert or replace into replicas values(?,?,'ok')", c["id"], w["id"])
            events.add("repair", f"chunk {c['id']} re-replicated {src} -> {w['id']}")

    # 3. a worker came back and now there are too many copies: trim the extras
    for c in counts("n > ?"):
        reps = db.q(
            "select r.wid, w.url, (select count(*) from replicas x where x.wid=r.wid) load "
            "from replicas r join workers w on w.id=r.wid "
            "where r.cid=? and r.state='ok' and w.status='alive' order by load desc", c["id"],
        )
        for r in reps[: c["n"] - config.RF]:
            await workers.drop(r["url"], c["id"])
            db.x("delete from replicas where cid=? and wid=?", c["id"], r["wid"])
            events.add("trim", f"extra copy of chunk {c['id']} removed from {r['wid']}")


def give_up(c):
    if c["id"] not in lost:
        lost.add(c["id"])
        events.add("lost", f"chunk {c['id']} has no healthy copy reachable, cannot repair yet")


async def scrub():
    """Re-hash every chunk on every live worker, then fix what was found."""
    async with busy:
        for w in workers.alive():
            await audit(w)
        if not state.safe["on"]:
            await repair()


# ---------- safe mode: rebuilding chunk locations after a controller start ----------

def enter_safe(how):
    ids = {w["id"] for w in db.q("select id from workers")}
    if how == "restored":
        events.add("recovery", "live metadata was missing or damaged, restored from the backup volume")
    if not ids:
        return  # first start ever, nothing to rebuild
    state.safe.update(on=True, wait=ids, until=time.time() + config.SAFE_TIMEOUT)
    events.add("recovery", f"controller started in safe mode, waiting for {len(ids)} workers to report their chunks")


def leave_safe(why):
    state.safe["on"] = False
    n = db.one("select count(*) n from replicas")["n"]
    events.add("recovery", f"safe mode off ({why}), {n} chunk locations rebuilt from worker reports")


def check_safe():
    if state.safe["on"] and time.time() > state.safe["until"]:
        leave_safe("gave up waiting for " + ", ".join(sorted(state.safe["wait"])))


async def welcome(w):
    """A worker (re)joined: reconcile its disk with the metadata."""
    async with busy:
        n = await audit(w)
        if state.safe["on"]:
            if n is not None:
                events.add("recovery", f"{w['id']} reported {n} chunks")
                state.safe["wait"].discard(w["id"])
            if not state.safe["wait"]:
                leave_safe("all workers reported")
        if not state.safe["on"]:
            await repair()


def backup():
    if state.dirty:
        state.dirty = False
        db.backup()


async def every(secs, fn):
    while True:
        await asyncio.sleep(secs)
        try:
            r = fn()
            if asyncio.iscoroutine(r):
                await r
        except Exception:
            log.exception("background job failed")


async def repair_pass():
    if state.safe["on"]:
        return  # locations are incomplete, repairing now would copy data for nothing
    async with busy:
        await repair()


def start():
    return [
        asyncio.create_task(every(1, check_heartbeats)),
        asyncio.create_task(every(1, check_safe)),
        asyncio.create_task(every(config.BACKUP_EVERY, backup)),
        asyncio.create_task(every(config.REPAIR_EVERY, repair_pass)),
        asyncio.create_task(every(config.SCRUB_EVERY, scrub)),
    ]
