#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"

show_status() {
  local label="$1"
  local url="$2"
  local pidfile="$3"

  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      echo "$label: running at $url (PID $pid)"
      return
    fi
  fi

  echo "$label: not running"
}

show_status "DBStress" "http://localhost:3001" "$RUN_DIR/dbstress.pid"
show_status "GC Benchmark" "http://localhost:8000" "$RUN_DIR/gc-benchmark.pid"
