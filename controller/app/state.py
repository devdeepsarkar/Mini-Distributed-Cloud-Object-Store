"""Small bits of shared controller state."""

# Safe mode: right after a start the controller does not know where chunks are.
# It waits for workers to report their inventories before it repairs anything
# or accepts writes (the same idea as HDFS safe mode).
safe = {"on": False, "wait": set(), "until": 0.0}

# Set whenever object metadata changes, cleared by the backup job.
dirty = False


def changed():
    global dirty
    dirty = True
