#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PAYLOAD_PATH="$ROOT_DIR/notifications/latest_notification.json"
SHOW_SCRIPT="$ROOT_DIR/scripts/show_latest_notification.sh"
SHOW_EXISTING="${SHOW_EXISTING:-0}"

echo "Watching $PAYLOAD_PATH"
echo "Leave this running while Airflow triggers the DAG."
echo "Press Ctrl+C to stop."

payload_fingerprint() {
  if [[ ! -f "$PAYLOAD_PATH" ]]; then
    return 1
  fi
  python3 - "$PAYLOAD_PATH" <<'PY'
import hashlib
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as payload_file:
    payload = json.load(payload_file)
fingerprint_payload = {
    "quote": payload.get("quote"),
    "movie": payload.get("movie"),
    "character": payload.get("character"),
    "actor": payload.get("actor"),
    "poster_url": payload.get("poster_url"),
}
encoded = json.dumps(fingerprint_payload, sort_keys=True).encode("utf-8")
print(hashlib.sha256(encoded).hexdigest())
PY
}

last_seen=""
if [[ "$SHOW_EXISTING" != "1" ]] && [[ -f "$PAYLOAD_PATH" ]]; then
  last_seen="$(payload_fingerprint || true)"
  echo "Existing payload detected; waiting for the next DAG update."
fi

while true; do
  if [[ -f "$PAYLOAD_PATH" ]]; then
    current="$(payload_fingerprint || true)"
    if [[ -n "$current" && "$current" != "$last_seen" ]]; then
      last_seen="$current"
      echo "New notification payload detected at $(date '+%Y-%m-%d %H:%M:%S')"
      "$SHOW_SCRIPT" "$PAYLOAD_PATH"
    fi
  fi
  sleep 3
done
