import os

CHUNK = int(os.getenv("CHUNK_SIZE", 1024 * 1024))    # 1 MB
RF = int(os.getenv("REPLICATION_FACTOR", 2))          # copies per chunk
DEAD_AFTER = float(os.getenv("HEARTBEAT_TIMEOUT", 10))  # seconds without a heartbeat
REPAIR_EVERY = float(os.getenv("REPAIR_INTERVAL", 5))
SCRUB_EVERY = float(os.getenv("SCRUB_INTERVAL", 30))
DB = os.getenv("DB_PATH", "/data/meta.db")

# metadata backup, kept on a different volume than the live database
BACKUP = os.getenv("BACKUP_PATH", "/backup/meta.db")
BACKUP_EVERY = float(os.getenv("BACKUP_INTERVAL", 5))
# how long a starting controller waits for known workers to report in
SAFE_TIMEOUT = float(os.getenv("SAFE_MODE_TIMEOUT", 15))
