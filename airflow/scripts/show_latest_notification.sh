#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PAYLOAD_PATH="${1:-$ROOT_DIR/notifications/latest_notification.json}"

if [[ ! -f "$PAYLOAD_PATH" ]]; then
  echo "Notification payload not found: $PAYLOAD_PATH" >&2
  echo "Run the Airflow DAG first, then try again." >&2
  exit 1
fi

read_value() {
  local key="$1"
  python3 - "$PAYLOAD_PATH" "$key" <<'PY'
import json
import sys

path, key = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as payload_file:
    data = json.load(payload_file)
value = data
for part in key.split("."):
    value = value.get(part) if isinstance(value, dict) else None
print("" if value is None else value)
PY
}

movie="$(read_value movie)"
year="$(read_value year)"
rating="$(read_value rating)"
quote="$(read_value quote)"
character="$(read_value character)"
actor="$(read_value actor)"
poster_file="$(read_value poster_file)"

if [[ "$poster_file" == /opt/airflow/notifications/* ]]; then
  poster_file="$ROOT_DIR/notifications/${poster_file##*/}"
fi

poster_url=""
if [[ -n "$poster_file" && -f "$poster_file" ]]; then
  poster_url="$(python3 - "$poster_file" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).resolve().as_uri())
PY
)"
fi

title="Quote of the hour"
subtitle="$movie"
[[ -n "$year" ]] && subtitle="$subtitle ($year)"
message="\"$quote\""
[[ -n "$character" ]] && message="$message - $character"
[[ -n "$actor" ]] && message="$message ($actor)"
[[ -n "$rating" ]] && message="$message · Rating: $rating"

show_with_osascript() {
  osascript - "$message" "$title" "$subtitle" <<'APPLESCRIPT'
on run argv
  display notification (item 1 of argv) with title (item 2 of argv) subtitle (item 3 of argv)
end run
APPLESCRIPT
  if [[ -n "$poster_file" && -f "$poster_file" ]]; then
    open "$poster_file"
  fi
}

if command -v terminal-notifier >/dev/null 2>&1; then
  args=(-title "$title" -subtitle "$subtitle" -group "movie-quote-notifier")
  if [[ -n "$poster_file" && -f "$poster_file" ]]; then
    args+=(-contentImage "$poster_file")
  fi
  if [[ -n "$poster_url" ]]; then
    args+=(-open "$poster_url")
  fi
  if ! printf "%s" "$message" | terminal-notifier "${args[@]}"; then
    echo "terminal-notifier failed; falling back to osascript." >&2
    show_with_osascript
  fi
else
  show_with_osascript
  echo "Tip: install terminal-notifier to show the poster inside the notification:"
  echo "  brew install terminal-notifier"
fi
