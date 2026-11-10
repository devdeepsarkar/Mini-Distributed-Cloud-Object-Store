"""Metadata lives in one SQLite file so it survives a controller restart.

Objects and chunks are the durable truth. Chunk locations (replicas) are
rebuilt from the workers' own inventories every time the controller starts.
"""
import os
import shutil
import sqlite3
from contextlib import contextmanager

from . import config

SCHEMA = """
create table if not exists objects(
    id text primary key,
    name text, size integer, ctype text,
    created text, modified text,
    checksum text, nchunks integer,
    custom text,          -- user key-value metadata, as JSON
    state text,           -- uploading | ready | deleting
    version integer default 1   -- bumped on every metadata change (ETag)
);
create table if not exists chunks(
    id text primary key,
    oid text, idx integer, size integer, checksum text
);
create index if not exists chunks_oid on chunks(oid, idx);
create table if not exists replicas(
    cid text, wid text,
    state text,           -- ok | corrupt
    primary key(cid, wid)
);
create index if not exists replicas_wid on replicas(wid);
create table if not exists workers(
    id text primary key,
    url text, last_seen real,
    status text,          -- alive | dead
    free integer
);
"""

con = None


def usable(path):
    """True if the file is a healthy database that already has our tables."""
    if not os.path.exists(path):
        return False
    try:
        c = sqlite3.connect(path)
        ok = c.execute("pragma quick_check").fetchone()[0] == "ok"
        ok = ok and c.execute("select 1 from sqlite_master where name='objects'").fetchone()
        c.close()
        return bool(ok)
    except sqlite3.Error:
        return False


def init():
    """Open the database. Returns 'existing', 'restored' (from backup) or 'fresh'."""
    global con
    how = "existing"
    d = os.path.dirname(config.DB)
    if d:
        os.makedirs(d, exist_ok=True)

    if not usable(config.DB):
        for ext in ("", "-wal", "-shm"):
            if os.path.exists(config.DB + ext):
                os.remove(config.DB + ext)
        if usable(config.BACKUP):
            shutil.copy(config.BACKUP, config.DB)
            how = "restored"
        else:
            how = "fresh"

    con = sqlite3.connect(config.DB, isolation_level=None, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("pragma journal_mode=wal")
    con.executescript(SCHEMA)
    if "version" not in [c["name"] for c in con.execute("pragma table_info(objects)")]:
        con.execute("alter table objects add column version integer default 1")

    # locations are soft state: forget them and let the workers tell us again
    con.execute("update workers set status='dead'")
    con.execute("delete from replicas")
    return how


def backup():
    """Consistent online copy of the live database to the backup volume."""
    d = os.path.dirname(config.BACKUP)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = config.BACKUP + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    dst = sqlite3.connect(tmp)
    con.backup(dst)
    dst.close()
    os.replace(tmp, config.BACKUP)


def q(sql, *a):
    return con.execute(sql, a).fetchall()


def one(sql, *a):
    return con.execute(sql, a).fetchone()


def x(sql, *a):
    return con.execute(sql, a)


@contextmanager
def tx():
    con.execute("begin")
    try:
        yield
        con.execute("commit")
    except BaseException:
        con.execute("rollback")
        raise
