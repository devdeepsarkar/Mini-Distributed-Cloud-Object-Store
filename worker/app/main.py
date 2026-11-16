"""Storage worker: keeps chunks on local disk and reports to the controller."""
import asyncio
import hashlib
import os
import re
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

WID = os.getenv("WORKER_ID", "worker1")
PORT = int(os.getenv("PORT", "8001"))
URL = os.getenv("WORKER_URL", f"http://{WID}:{PORT}")
CTRL = os.getenv("CONTROLLER_URL", "http://controller:8000")
BEAT = float(os.getenv("HEARTBEAT_INTERVAL", "3"))
DIR = Path(os.getenv("DATA_DIR", "/data")) / "chunks"

OK_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def path(cid):
    # chunk ids become file names, so refuse anything that could leave DIR
    if not OK_ID.match(cid):
        raise HTTPException(400, "bad chunk id")
    return DIR / cid


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while buf := f.read(1 << 20):
            h.update(buf)
    return h.hexdigest()


def stored():
    return [p for p in DIR.iterdir() if not p.name.endswith(".tmp")]


def info():
    return {
        "worker_id": WID,
        "url": URL,
        "chunks": len(stored()),
        "free_bytes": shutil.disk_usage(DIR).free,
    }


async def beat():
    """Register once, then heartbeat. Re-register if the controller forgot us."""
    known = False
    async with httpx.AsyncClient(timeout=3) as c:
        while True:
            try:
                ep = "heartbeat" if known else "register"
                r = await c.post(f"{CTRL}/internal/workers/{ep}", json=info())
                known = r.status_code == 200
            except httpx.HTTPError:
                pass
            await asyncio.sleep(BEAT if known else 1)


@asynccontextmanager
async def life(app):
    DIR.mkdir(parents=True, exist_ok=True)
    for p in DIR.glob("*.tmp"):  # half-written chunks from a crash
        p.unlink()
    t = asyncio.create_task(beat())
    yield
    t.cancel()


app = FastAPI(
    title=f"Object store worker: {WID}",
    description="Internal chunk storage API, called by the controller.",
    version="1.0",
    lifespan=life,
)

W = "Internal API (controller to worker)"

# Documentation only: lets the docs page offer a file picker for PUT chunk.
CHUNK_BODY = {"requestBody": {"required": True, "content": {"application/octet-stream": {
    "schema": {"type": "string", "format": "binary"}}}}}


@app.put("/internal/chunks/{cid}", tags=[W], summary="Store a chunk", openapi_extra=CHUNK_BODY)
async def put_chunk(cid: str, req: Request):
    p = path(cid)
    data = await req.body()
    got = hashlib.sha256(data).hexdigest()
    want = req.headers.get("x-checksum")
    if want and want != got:
        raise HTTPException(400, "checksum mismatch, chunk damaged in transit")

    # write to a temp file and rename, so a crash never leaves half a chunk
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)
    return {"chunk_id": cid, "size": len(data), "checksum": got}


@app.get("/internal/chunks/{cid}", tags=[W], summary="Retrieve a chunk")
def get_chunk(cid: str):
    p = path(cid)
    if not p.exists():
        raise HTTPException(404, "chunk not found")
    return FileResponse(p, media_type="application/octet-stream")


@app.delete("/internal/chunks/{cid}", status_code=204, tags=[W], summary="Delete a chunk")
def del_chunk(cid: str):
    p = path(cid)
    if not p.exists():
        raise HTTPException(404, "chunk not found")
    p.unlink()
    return Response(status_code=204)


@app.get("/internal/workers/{wid}/chunks", tags=[W], summary="List the chunks this worker stores")
def list_chunks(wid: str, checksums: bool = False):
    """What this worker holds. With checksums=true each chunk is re-hashed from disk."""
    if wid != WID:
        raise HTTPException(404, f"this is {WID}, not {wid}")
    out = []
    for p in sorted(stored()):
        c = {"chunk_id": p.name, "size": p.stat().st_size}
        if checksums:
            c["checksum"] = sha(p)
        out.append(c)
    return {"worker_id": WID, "chunks": out}


@app.get("/internal/health", tags=[W], summary="Worker health")
def health():
    return info()
