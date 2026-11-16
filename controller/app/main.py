"""Controller: client REST API, metadata and cluster management."""
import asyncio
import json
import logging
import mimetypes
import time
from datetime import datetime
from email.utils import formatdate
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, File, Form, Query, Request, Response, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException

from . import config, db, events, healer, metrics, objects, state, workers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("store")
HERE = Path(__file__).parent
jobs = set()  # keeps references to fire-and-forget tasks


class Quiet(logging.Filter):
    """Keep heartbeats and dashboard polling out of the access log."""

    def filter(self, rec):
        m = rec.getMessage()
        return "/internal/workers/heartbeat" not in m and "poll=1" not in m and "/metrics" not in m


logging.getLogger("uvicorn.access").addFilter(Quiet())
logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per chunk transfer is too much


@asynccontextmanager
async def life(app):
    how = db.init()
    workers.start()
    healer.enter_safe(how)
    state.changed()  # take a first backup soon after start
    # an upload or delete cut short by a restart leaves a half-object: drop it
    for o in db.q("select id from objects where state!='ready'"):
        await objects.purge(o["id"])
    tasks = healer.start()
    yield
    for t in tasks:
        t.cancel()
    await workers.stop()


app = FastAPI(
    title="Mini Distributed Cloud Object Store",
    description="Controller API. Clients store and read objects here; workers register and send heartbeats here.",
    version="1.0",
    lifespan=life,
)

A = "Admin and monitoring"
I = "Internal API (workers to controller)"

# ---------- cluster view (dashboard, CLI, demo) ----------

@app.get("/api/v1/cluster", tags=[A], summary="Workers and replication health")
def cluster():
    now = time.time()
    load = {r["wid"]: r["n"] for r in db.q("select wid, count(*) n from replicas group by wid")}
    ws = [{
        "worker_id": w["id"],
        "url": w["url"],
        "status": w["status"],
        "chunks": load.get(w["id"], 0),
        "free_bytes": w["free"],
        "last_heartbeat_seconds_ago": round(now - w["last_seen"], 1),
    } for w in db.q("select * from workers order by id")]
    return {
        "mode": "recovering" if state.safe["on"] else "normal",
        "chunk_size": config.CHUNK,
        "replication_factor": config.RF,
        "workers": ws,
        "objects": db.one("select count(*) n from objects where state='ready'")["n"],
        "chunks": db.one("select count(*) n from chunks")["n"],
        "under_replicated_chunks": metrics.under_replicated(),
        "corrupt_copies": db.one("select count(*) n from replicas where state='corrupt'")["n"],
    }


@app.get("/api/v1/events", tags=[A], summary="Recent cluster events")
def get_events(limit: int = Query(50, ge=1, le=300)):
    return {"events": list(events.feed)[:limit]}


@app.post("/api/v1/admin/scrub", tags=[A], summary="Verify every chunk now and repair")
async def scrub_now():
    """Check every stored chunk against its checksum right now and repair."""
    await healer.scrub()
    return cluster()


@app.post("/api/v1/admin/backup", tags=[A], summary="Back up the metadata database now")
def backup_now():
    """Copy the metadata database to the backup volume right now."""
    db.backup()
    state.dirty = False
    return {"status": "ok", "backup": config.BACKUP}


@app.get("/metrics", include_in_schema=False)
def prom():
    body, ctype = metrics.render()
    return Response(body, media_type=ctype)


@app.get("/healthz", tags=[A], summary="Controller health")
def healthz():
    db.one("select 1")
    return {
        "status": "ok",
        "mode": "recovering" if state.safe["on"] else "normal",
        "live_workers": len(workers.alive()),
    }


# ---------- internal API (workers -> controller) ----------

class Beat(BaseModel):
    worker_id: str
    url: str
    chunks: int = 0
    free_bytes: int = 0


def hello(b):
    if workers.seen(b.worker_id, b.url, b.free_bytes):
        w = db.one("select * from workers where id=?", b.worker_id)
        t = asyncio.create_task(healer.welcome(w))
        jobs.add(t)
        t.add_done_callback(jobs.discard)
    return {"status": "ok", "heartbeat_timeout": config.DEAD_AFTER}


@app.post("/internal/workers/register", tags=[I], summary="Register a worker")
async def register(b: Beat):
    return hello(b)


@app.post("/internal/workers/heartbeat", tags=[I], summary="Worker heartbeat")
async def heartbeat(b: Beat):
    if db.one("select 1 from workers where id=?", b.worker_id) is None:
        raise HTTPException(404, "unknown worker, register first")
    return hello(b)
