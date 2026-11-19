#!/bin/bash
# Cause a real internal error for one object, for the 500 demo.
#
#   ./scripts/break_metadata.sh <object_id>          damage the object's metadata record
#   ./scripts/break_metadata.sh <object_id> --fix    put it back
#
# The object's custom-metadata column in the controller's database is overwritten
# with text that is not valid JSON. The controller does not expect that, so
# reading the object's metadata raises an exception, and the API answers
# 500 with a JSON error body. Other objects, and this object's data, are unaffected.
set -e

OID=$1
if [ -z "$OID" ]; then
  echo "usage: $0 <object_id> [--fix]" >&2
  exit 1
fi
VAL='{broken'
[ "$2" = "--fix" ] && VAL='{}'

docker exec -e OID="$OID" -e VAL="$VAL" controller python -c "
import os, sqlite3
c = sqlite3.connect('/data/meta.db', timeout=5)
n = c.execute('update objects set custom=? where id=?', (os.environ['VAL'], os.environ['OID'])).rowcount
c.commit()
print('record updated' if n else 'no such object')
"
if [ "$2" = "--fix" ]; then
  echo "metadata record repaired (custom metadata reset to empty)"
else
  echo "metadata record damaged; GET /api/v1/objects/$OID/metadata now returns 500"
fi
