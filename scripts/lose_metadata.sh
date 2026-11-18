#!/bin/bash
# Simulate a controller crash that destroys its metadata database.
#
#   ./scripts/lose_metadata.sh
#
# The live database is deleted and the controller is killed. On restart it
# restores the metadata from the backup volume, then rebuilds every chunk
# location from what the workers report (watch the Events panel).
set -e
URL=${OS_URL:-http://localhost:8000}

curl -sf -X POST "$URL/api/v1/admin/backup" >/dev/null && echo "backup is up to date"
docker exec controller sh -c 'rm -f /data/meta.db /data/meta.db-wal /data/meta.db-shm'
echo "deleted the live metadata database"
docker kill controller >/dev/null
echo "controller killed"
docker start controller >/dev/null
echo "controller restarted, waiting for it to recover..."

for _ in $(seq 60); do
  mode=$(curl -s "$URL/healthz" | sed -n 's/.*"mode":"\([a-z]*\)".*/\1/p')
  [ "$mode" = "normal" ] && break
  sleep 1
done
echo "mode: ${mode:-unreachable}"
curl -s "$URL/api/v1/events?limit=20" | docker exec -i controller python -c "
import json, sys
for e in reversed(json.load(sys.stdin)['events']):
    if e['kind'] == 'recovery': print('  ', e['message'])"
