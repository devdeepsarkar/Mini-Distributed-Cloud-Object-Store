#!/bin/bash
# Walks through every item of the required demonstration, pausing between steps.
# Start the stack first:  docker compose up --build -d
set -e
cd "$(dirname "$0")/.."

URL=${OS_URL:-http://localhost:8000}
API=$URL/api/v1
T=$(mktemp -d)

sum() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
json() { docker exec -i controller python -c "import json,sys; d=json.load(sys.stdin); $1"; }
step() { echo; echo "=================================================================="; echo "  $1"; echo "=================================================================="; }
pause() { echo; read -r -p "press Enter to continue... " _; }

chunkmap() {
  curl -s "$API/objects/$1/metadata" | json "
for c in d['chunks']:
    where = ', '.join(r['worker'] + ('' if r['alive'] and r['state']=='ok' else ' (down)' if not r['alive'] else ' (corrupt)') for r in c['replicas'])
    print(f\"  chunk {c['index']}: {c['size']:>8} bytes  healthy copies={c['healthy_copies']}  on {where}\")"
}

workers() {
  curl -s "$API/cluster" | json "
for w in d['workers']: print(f\"  {w['worker_id']}: {w['status']:<5} {w['chunks']} chunks\")
print('  under-replicated chunks:', d['under_replicated_chunks'], ' corrupt copies:', d['corrupt_copies'])"
}

wait_healthy() {
  echo -n "  waiting for replication to be restored"
  for _ in $(seq 60); do
    n=$(curl -s "$API/cluster" | json "print(d['under_replicated_chunks'] + d['corrupt_copies'] + (d['mode'] != 'normal'))")
    [ "$n" = "0" ] && { echo " done"; return; }
    echo -n "."; sleep 1
  done
  echo " (still not healthy)"
}

step "0. Cluster: 1 controller + 4 workers"
workers
pause

step "1. Object storage: upload a file, download it, compare"
echo "hello from the mini object store" > "$T/hello.txt"
SMALL=$(curl -s -X POST "$API/objects" -F "file=@$T/hello.txt" | json "print(d['object_id'])")
echo "  uploaded hello.txt -> object $SMALL"
curl -s "$API/objects/$SMALL" -o "$T/hello.out"
echo "  original : $(sum "$T/hello.txt")"
echo "  retrieved: $(sum "$T/hello.out")"
cmp -s "$T/hello.txt" "$T/hello.out" && echo "  MATCH"
pause

step "2. Chunking and distribution: upload a 5.5 MB object"
head -c 5767168 /dev/urandom > "$T/big.bin"
BIG=$(curl -s -X POST "$API/objects" -F "file=@$T/big.bin" -F 'metadata={"owner":"devdeep","course":"cloud-computing"}' | json "print(d['object_id'])")
echo "  uploaded big.bin -> object $BIG"
chunkmap "$BIG"
pause

step "3. Replication: every chunk has 2 copies on different workers"
chunkmap "$BIG"
workers
pause

step "4. Metadata: read it, then change it without re-uploading"
curl -s "$API/objects/$BIG/metadata" | json "print('  custom metadata:', d['custom_metadata']); print('  modified_at    :', d['modified_at'])"
sleep 1
echo "  PATCH {\"reviewed\": \"yes\", \"owner\": \"team-7\"}"
curl -s -X PATCH "$API/objects/$BIG/metadata" -H 'Content-Type: application/json' -d '{"reviewed":"yes","owner":"team-7"}' \
  | json "print('  custom metadata:', d['custom_metadata']); print('  modified_at    :', d['modified_at'])"
pause

step "5a. Worker failure: docker stop worker2"
docker stop worker2 >/dev/null
echo "  worker2 stopped. Downloading immediately, before the failure is even detected:"
curl -s "$API/objects/$BIG" -o "$T/big.out"
cmp -s "$T/big.bin" "$T/big.out" && echo "  download OK, content MATCHES (served from the other copies)"
echo "  waiting for the heartbeat timeout..."
for _ in $(seq 30); do
  s=$(curl -s "$API/cluster" | json "print(next(w['status'] for w in d['workers'] if w['worker_id']=='worker2'))")
  [ "$s" = "dead" ] && break; sleep 1
done
workers
wait_healthy
echo "  chunks that lived on worker2 were copied to other workers:"
chunkmap "$BIG"
pause

step "5b. Worker recovery: docker start worker2"
docker start worker2 >/dev/null
for _ in $(seq 30); do
  s=$(curl -s "$API/cluster" | json "print(next(w['status'] for w in d['workers'] if w['worker_id']=='worker2'))")
  [ "$s" = "alive" ] && break; sleep 1
done
sleep 6
echo "  worker2 is back. Its old copies made 3 copies of some chunks, extras were trimmed:"
workers
chunkmap "$BIG"
pause

step "6. Data integrity: corrupt a stored chunk on purpose"
./scripts/corrupt.sh "$BIG" 1 | sed 's/^/  /'
echo "  downloading the object..."
curl -s "$API/objects/$BIG" -o "$T/big.out2"
cmp -s "$T/big.bin" "$T/big.out2" && echo "  download OK, content MATCHES (a copy that fails its checksum is never served)"
echo "  forcing a full scrub of all workers..."
curl -s -X POST "$API/admin/scrub" >/dev/null
wait_healthy
chunkmap "$BIG"
pause

step "7. Event log (what the controller did)"
curl -s "$API/events?limit=40" | json "
import time
for e in reversed(d['events']): print(' ', time.strftime('%H:%M:%S', time.localtime(e['time'])), e['kind'].ljust(12), e['message'])"

step "8. Extras: partial read, conditional update, controller recovery"
echo "  Range: bytes=1048000-1049999 (crosses the border between chunk 0 and chunk 1)"
curl -s -o "$T/part.bin" -D - -H "Range: bytes=1048000-1049999" "$API/objects/$BIG" | grep -iE "^HTTP|content-range|content-length" | sed 's/^/    /'
dd if="$T/big.bin" bs=1 skip=1048000 count=2000 2>/dev/null | cmp -s - "$T/part.bin" && echo "  the 2000 bytes MATCH the original"
echo
TAG=$(curl -s -D - -o /dev/null "$API/objects/$BIG/metadata" | grep -i '^etag' | tr -d '\r' | cut -d' ' -f2)
echo "  two clients read metadata version $TAG, then both try to update it with If-Match:"
curl -s -o /dev/null -w "    client A: HTTP %{http_code}\n" -X PATCH "$API/objects/$BIG/metadata" -H "If-Match: $TAG" -H 'Content-Type: application/json' -d '{"editor":"A"}'
curl -s -o /dev/null -w "    client B: HTTP %{http_code}  (refused, A's update is not overwritten)\n" -X PATCH "$API/objects/$BIG/metadata" -H "If-Match: $TAG" -H 'Content-Type: application/json' -d '{"editor":"B"}'
echo
echo "  an unexpected internal failure: the object's metadata record is damaged in the database"
./scripts/break_metadata.sh "$BIG" | sed 's/^/    /'
curl -s -w "\n    -> HTTP %{http_code}\n" "$API/objects/$BIG/metadata" | sed 's/^{/    {/'
curl -s -o /dev/null -w "    other requests still work: GET /api/v1/objects -> HTTP %{http_code}\n" "$API/objects"
./scripts/break_metadata.sh "$BIG" --fix | sed 's/^/    /'
curl -s -X PATCH "$API/objects/$BIG/metadata" -H 'Content-Type: application/json' -d '{"owner":"team-7","reviewed":"yes","editor":"A"}' -o /dev/null
curl -s -o /dev/null -w "    after the repair: GET metadata -> HTTP %{http_code}\n" "$API/objects/$BIG/metadata"
echo
echo "  now the controller loses its metadata database:"
./scripts/lose_metadata.sh | sed 's/^/  /'
wait_healthy
curl -s "$API/objects/$BIG" -o "$T/big.out3"
cmp -s "$T/big.bin" "$T/big.out3" && echo "  download after recovery MATCHES the original"
pause

step "9. Delete"
curl -s -o /dev/null -w "  DELETE small object -> HTTP %{http_code}\n" -X DELETE "$API/objects/$SMALL"
curl -s -o /dev/null -w "  GET it again        -> HTTP %{http_code}\n" "$API/objects/$SMALL"
echo
echo "demo finished. The 5.5 MB object $BIG is still stored; see $URL"
rm -rf "$T"
