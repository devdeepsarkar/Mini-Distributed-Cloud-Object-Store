"""Small in-memory event feed, shown on the dashboard and printed to the logs."""
import logging
import time
from collections import deque

from . import metrics

log = logging.getLogger("store")
feed = deque(maxlen=300)


def add(kind, msg):
    feed.appendleft({"time": time.time(), "kind": kind, "message": msg})
    metrics.EVENTS.labels(kind).inc()
    log.info("[%s] %s", kind, msg)
