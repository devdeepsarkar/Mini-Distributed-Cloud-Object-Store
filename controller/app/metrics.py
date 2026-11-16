"""Prometheus metrics, served at /metrics."""
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from . import config, db, state

EVENTS = Counter("objstore_events_total", "Cluster events by kind (repair, corrupt, worker-down, ...)", ["kind"])
BYTES_IN = Counter("objstore_uploaded_bytes_total", "Object bytes accepted from clients")
BYTES_OUT = Counter("objstore_downloaded_bytes_total", "Object bytes served to clients")
LATENCY = Histogram(
    "objstore_request_seconds", "Time to answer a client API request", ["method", "route", "status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

WORKERS = Gauge("objstore_workers", "Workers by status", ["status"])
WORKER_UP = Gauge("objstore_worker_up", "1 if the worker is alive", ["worker"])
WORKER_CHUNKS = Gauge("objstore_worker_chunks", "Chunk copies recorded on the worker", ["worker"])
OBJECTS = Gauge("objstore_objects", "Stored objects")
CHUNKS = Gauge("objstore_chunks", "Chunks across all objects")
STORED = Gauge("objstore_stored_bytes", "Logical bytes stored (before replication)")
UNDER = Gauge("objstore_under_replicated_chunks", "Chunks with fewer healthy live copies than the replication factor")
CORRUPT = Gauge("objstore_corrupt_copies", "Copies that failed their checksum and wait for repair")
SAFE = Gauge("objstore_safe_mode", "1 while the controller is rebuilding chunk locations after a start")


def under_replicated():
    return db.one(
        "select count(*) n from (select c.id from chunks c "
        "join objects o on o.id=c.oid and o.state='ready' "
        "left join (select r.cid from replicas r join workers w on w.id=r.wid "
        "           where r.state='ok' and w.status='alive') g on g.cid=c.id "
        "group by c.id having count(g.cid) < ?)", config.RF,
    )["n"]


def render():
    """Refresh the gauges from the database, then return the exposition text."""
    load = {r["wid"]: r["n"] for r in db.q("select wid, count(*) n from replicas group by wid")}
    n = {"alive": 0, "dead": 0}
    for w in db.q("select id, status from workers"):
        n[w["status"]] += 1
        WORKER_UP.labels(w["id"]).set(w["status"] == "alive")
        WORKER_CHUNKS.labels(w["id"]).set(load.get(w["id"], 0))
    for s, v in n.items():
        WORKERS.labels(s).set(v)
    o = db.one("select count(*) n, coalesce(sum(size), 0) b from objects where state='ready'")
    OBJECTS.set(o["n"])
    STORED.set(o["b"])
    CHUNKS.set(db.one("select count(*) n from chunks")["n"])
    UNDER.set(under_replicated())
    CORRUPT.set(db.one("select count(*) n from replicas where state='corrupt'")["n"])
    SAFE.set(state.safe["on"])
    return generate_latest(), CONTENT_TYPE_LATEST
