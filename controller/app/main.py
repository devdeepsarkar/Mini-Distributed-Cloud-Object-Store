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

C = "Client API"
A = "Admin and monitoring"
I = "Internal API (workers to controller)"

# Documentation only: lets the docs page offer a JSON editor for PATCH metadata.
PATCH_BODY = {"requestBody": {"required": True, "content": {"application/json": {
    "schema": {"type": "object", "additionalProperties": True},
    "example": {"owner": "devdeep", "reviewed": "yes", "old_key": None},
}}}}


def tag_matches(header, *tags):
    """Does an If-Match / If-None-Match header name one of these ETags?"""
    if header is None:
        return False
    given = [t.strip().removeprefix("W/") for t in header.split(",")]
    return "*" in given or any(t in given for t in tags)


def must_match(req, o, *tags):
    """Optimistic locking: with If-Match, the caller's copy must be the current one."""
    h = req.headers.get("if-match")
    if h is not None and not tag_matches(h, *tags):
        raise HTTPException(412, f"object changed since you read it, current ETag is {objects.meta_etag(o)}")


def find(oid):
    o = db.one("select * from objects where id=?", oid)
    if o is None:
        raise HTTPException(404, f"object {oid} not found")
    return o


# ---------- client API ----------

@app.post("/api/v1/objects", status_code=201, tags=[C], summary="Upload an object")
async def upload(resp: Response, file: UploadFile = File(...), metadata: str | None = Form(None)):
    custom = {}
    if metadata:
        try:
            custom = json.loads(metadata)
        except ValueError:
            raise HTTPException(400, "metadata must be valid JSON")
        if not isinstance(custom, dict):
            raise HTTPException(400, "metadata must be a JSON object of key-value pairs")

    name = file.filename or "unnamed"
    ctype = file.content_type
    if not ctype or ctype == "application/octet-stream":
        ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"

    oid = await objects.put_object(file, name, ctype, custom)
    resp.headers["Location"] = f"/api/v1/objects/{oid}"
    return objects.meta(find(oid))


@app.get("/api/v1/objects", tags=[C], summary="List stored objects")
def list_objects(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    rows = db.q(
        "select * from objects where state='ready' order by created desc, id limit ? offset ?",
        limit, offset,
    )
    total = db.one("select count(*) n from objects where state='ready'")["n"]
    return {"objects": [objects.brief(o) for o in rows], "total": total, "limit": limit, "offset": offset}


def object_headers(o, inline):
    kind = "inline" if inline else "attachment"
    return {
        "Content-Disposition": f"{kind}; filename*=UTF-8''{quote(o['name'])}",
        "ETag": objects.etag(o),
        "Accept-Ranges": "bytes",
        "Last-Modified": formatdate(datetime.fromisoformat(o["created"]).timestamp(), usegmt=True),
    }


@app.head("/api/v1/objects/{oid}", tags=[C], summary="Object size and headers, no body")
def head(oid: str, inline: bool = False):
    o = find(oid)
    return Response(media_type=o["ctype"], headers={**object_headers(o, inline), "Content-Length": str(o["size"])})


@app.get("/api/v1/objects/{oid}", tags=[C], summary="Download an object")
async def download(oid: str, req: Request, inline: bool = False):
    o = find(oid)
    hd = object_headers(o, inline)

    # the caller already has these exact bytes
    if tag_matches(req.headers.get("if-none-match"), objects.etag(o)):
        return Response(status_code=304, headers=hd)

    # Range: bytes=a-b -> 206 with only that part, read from only the chunks it touches
    rng = objects.parse_range(req.headers.get("range"), o["size"])
    lo, hi = rng or (0, o["size"] - 1)
    objects.check_readable(o, lo, hi)
    status = 200
    if rng:
        status = 206
        hd["Content-Range"] = f"bytes {lo}-{hi}/{o['size']}"
        cs = objects.span(o, lo, hi)
        events.add("range", f"{o['name']}: bytes {lo}-{hi} read from chunk(s) "
                            f"{cs[0][0]['idx']}-{cs[-1][0]['idx']} of {o['nchunks']}")
    hd["Content-Length"] = str(hi - lo + 1)

    # pull the first chunk before answering, so a dead object gives a clean 503
    gen = objects.stream(o, lo, hi)
    try:
        first = await anext(gen)
    except StopAsyncIteration:
        first = b""

    async def body():
        yield first
        async for buf in gen:
            yield buf

    return StreamingResponse(body(), status_code=status, media_type=o["ctype"], headers=hd)


@app.get("/api/v1/objects/{oid}/metadata", tags=[C], summary="Retrieve metadata")
def get_meta(oid: str, resp: Response):
    o = find(oid)
    # The ETag is the metadata version, for If-Match on PATCH and DELETE. It is
    # not a cache validator: the body also lists chunk locations, which change
    # during repairs without a version bump, so this must never be cached.
    resp.headers["ETag"] = objects.meta_etag(o)
    resp.headers["Cache-Control"] = "no-store"
    return objects.meta(o)


@app.patch("/api/v1/objects/{oid}/metadata", tags=[C], summary="Update custom metadata", openapi_extra=PATCH_BODY)
async def patch_meta(oid: str, req: Request, resp: Response):
    find(oid)
    try:
        kv = await req.json()
    except ValueError:
        raise HTTPException(400, "body must be valid JSON")
    if not isinstance(kv, dict):
        raise HTTPException(400, "body must be a JSON object of key-value pairs")
    # from here to the update nothing awaits, so check-then-write is atomic
    o = find(oid)
    if o["state"] != "ready":
        raise objects.Conflict(f"object is {o['state']}")
    must_match(req, o, objects.meta_etag(o))
    objects.patch_custom(o, kv)
    o = find(oid)
    resp.headers["ETag"] = objects.meta_etag(o)
    return objects.meta(o)


@app.delete("/api/v1/objects/{oid}", status_code=204, tags=[C], summary="Delete an object")
async def delete(oid: str, req: Request):
    o = find(oid)
    must_match(req, o, objects.meta_etag(o), objects.etag(o))
    await objects.delete_object(o)
    return Response(status_code=204)


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


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(HERE / "static" / "index.html")


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
