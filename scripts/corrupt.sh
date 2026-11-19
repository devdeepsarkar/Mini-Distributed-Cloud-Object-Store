#!/bin/bash
# Deliberately damage one stored copy of a chunk, for the data-integrity demo.
#
#   ./scripts/corrupt.sh <object_id> [chunk_index]
#
# It looks up which workers hold the chunk, then overwrites a few bytes of the
# chunk file inside the first worker's volume.
set -e

OID=$1
IDX=${2:-0}
URL=${OS_URL:-http://localhost:8000}

if [ -z "$OID" ]; then
  echo "usage: $0 <object_id> [chunk_index]" >&2
  exit 1
fi

# "worker chunk_id" of the first healthy copy
read -r W CID < <(curl -sf "$URL/api/v1/objects/$OID/metadata" | docker exec -i controller python -c "
import json, sys
c = json.load(sys.stdin)['chunks'][$IDX]
r = next(r for r in c['replicas'] if r['alive'] and r['state'] == 'ok')
print(r['worker'], c['chunk_id'])
")

if [ -z "$W" ]; then
  echo "could not find chunk $IDX of object $OID" >&2
  exit 1
fi

docker exec "$W" python -c "
with open('/data/chunks/$CID', 'r+b') as f:
    f.seek(64)
    f.write(b'### CORRUPTED ON PURPOSE ###')
"
echo "corrupted chunk $IDX ($CID) on $W"
