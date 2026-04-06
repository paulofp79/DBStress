#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 4 ]]; then
  echo "Usage: $0 <user/password@host:port/service> [interval_seconds] [event_filter] [min_avg_wait_ms]" >&2
  exit 1
fi

CONNECT_STRING="$1"
INTERVAL_SECONDS="${2:-5}"
EVENT_FILTER="${3:-}"
MIN_AVG_WAIT_MS="${4:-0}"

if ! command -v sqlplus >/dev/null 2>&1; then
  echo "sqlplus is required but was not found in PATH." >&2
  exit 1
fi

BASELINE_FILE="$(mktemp)"
CURRENT_FILE="$(mktemp)"
trap 'rm -f "$BASELINE_FILE" "$CURRENT_FILE"' EXIT

SNAPSHOT_SQL=$(cat <<'EOF'
set pagesize 0 feedback off verify off heading off echo off trimspool on linesize 400
SELECT inst_id || '|' || event || '|' || total_waits || '|' || time_waited_micro
FROM gv$system_event
WHERE event IN (
  'gc current block congested',
  'gc current block 3-way',
  'gc cr grant congested',
  'gc current block 2-way',
  'gc cr block congested',
  'gc cr grant 2-way'
)
ORDER BY inst_id, event;
exit;
EOF
)

take_snapshot() {
  local outfile="$1"
  sqlplus -s "$CONNECT_STRING" <<<"$SNAPSHOT_SQL" > "$outfile"
}

render_delta() {
  local baseline="$1"
  local current="$2"
  awk -F'|' -v filter="$EVENT_FILTER" -v min_avg="$MIN_AVG_WAIT_MS" '
  function trim(s) {
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", s);
    return s;
  }
  BEGIN {
    filter = trim(filter);
    min_avg += 0;
    printed = 0;
  }
  NR == FNR {
    inst_id = trim($1);
    event = trim($2);
    key = inst_id "|" event;
    baseline_waits[key] = trim($3) + 0;
    baseline_time[key] = trim($4) + 0;
    next;
  }
  {
    inst_id = trim($1);
    event = trim($2);
    if (filter != "" && event != filter) {
      next;
    }
    key = inst_id "|" event;
    cur_waits = trim($3) + 0;
    cur_time = trim($4) + 0;
    delta_waits = cur_waits - baseline_waits[key];
    delta_time = cur_time - baseline_time[key];
    avg_ms = 0;
    if (delta_waits > 0 && delta_time >= 0) {
      avg_ms = delta_time / delta_waits / 1000;
    }
    if (avg_ms <= min_avg) {
      next;
    }
    printf "inst_id=%-3s | %-28s | delta_waits=%-12d | avg_wait_ms=%.3f\n", inst_id, event, delta_waits, avg_ms;
    printed = 1;
  }
  END {
    if (!printed) {
      print "No rows matched current filter.";
    }
  }
  ' "$baseline" "$current"
}

take_snapshot "$BASELINE_FILE"

while true; do
  sleep "$INTERVAL_SECONDS"
  take_snapshot "$CURRENT_FILE"
  printf "\n[%s] mode=since-start | sample_interval=%ss" "$(date '+%Y-%m-%d %H:%M:%S')" "$INTERVAL_SECONDS"
  if [[ -n "$EVENT_FILTER" ]]; then
    printf " | filter=%s" "$EVENT_FILTER"
  fi
  if [[ "$MIN_AVG_WAIT_MS" != "0" ]]; then
    printf " | min_avg_wait_ms>%s" "$MIN_AVG_WAIT_MS"
  fi
  printf "\n"
  render_delta "$BASELINE_FILE" "$CURRENT_FILE"
done
