#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"

stop_from_pidfile() {
  local label="$1"
  local pidfile="$2"

  if [[ ! -f "$pidfile" ]]; then
    echo "$label not running"
    return
  fi

  local pid
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    echo "$label stopped"
  else
    echo "$label already stopped"
  fi

  rm -f "$pidfile"
}

stop_from_pidfile "DBStress" "$RUN_DIR/dbstress.pid"
stop_from_pidfile "GC Benchmark" "$RUN_DIR/gc-benchmark.pid"
