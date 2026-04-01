#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$RUN_DIR/logs"
GC_DIR="$ROOT_DIR/gc_benchmark/gc_benchmark"
GC_VENV="$GC_DIR/.venv"

mkdir -p "$RUN_DIR" "$LOG_DIR"

find_python() {
  if command -v python3.12 >/dev/null 2>&1; then
    echo "python3.12"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return
  fi
  echo ""
}

is_pid_running() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  kill -0 "$pid" >/dev/null 2>&1
}

stop_stale_pidfile() {
  local pidfile="$1"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    if ! is_pid_running "$pid"; then
      rm -f "$pidfile"
    fi
  fi
}

start_dbstress() {
  local pidfile="$RUN_DIR/dbstress.pid"
  stop_stale_pidfile "$pidfile"
  if [[ -f "$pidfile" ]]; then
    echo "DBStress already running with PID $(cat "$pidfile")"
    return
  fi

  (
    cd "$ROOT_DIR"
    nohup npm run serve >"$LOG_DIR/dbstress.log" 2>&1 &
    echo $! >"$pidfile"
  )
  sleep 2
}

prepare_gc_benchmark_env() {
  local pybin
  pybin="$(find_python)"
  if [[ -z "$pybin" ]]; then
    echo "No Python interpreter found. Need python3.12, python3, or python."
    exit 1
  fi

  if [[ ! -x "$GC_VENV/bin/python" ]]; then
    rm -rf "$GC_VENV"
    (
      cd "$GC_DIR"
      "$pybin" -m venv .venv
      . "$GC_VENV/bin/activate"
      python -m pip install --upgrade pip >/dev/null
      pip install -r requirements.txt >/dev/null
    )
    return
  fi

  if [[ ! -x "$GC_VENV/bin/uvicorn" ]]; then
    (
      cd "$GC_DIR"
      . "$GC_VENV/bin/activate"
      python -m pip install --upgrade pip >/dev/null
      pip install -r requirements.txt >/dev/null
    )
  fi
}

start_gc_benchmark() {
  local pidfile="$RUN_DIR/gc-benchmark.pid"
  stop_stale_pidfile "$pidfile"
  if [[ -f "$pidfile" ]]; then
    echo "GC Benchmark already running with PID $(cat "$pidfile")"
    return
  fi

  prepare_gc_benchmark_env

  (
    cd "$GC_DIR"
    nohup "$GC_VENV/bin/python" -m uvicorn main:app --host 0.0.0.0 --port 8000 >"$LOG_DIR/gc-benchmark.log" 2>&1 &
    echo $! >"$pidfile"
  )
  sleep 2
}

print_status() {
  echo ""
  echo "Services:"
  if [[ -f "$RUN_DIR/dbstress.pid" ]] && is_pid_running "$(cat "$RUN_DIR/dbstress.pid")"; then
    echo "  DBStress      : http://localhost:3001"
  else
    echo "  DBStress      : not running"
  fi

  if [[ -f "$RUN_DIR/gc-benchmark.pid" ]] && is_pid_running "$(cat "$RUN_DIR/gc-benchmark.pid")"; then
    echo "  GC Benchmark  : http://localhost:8000"
  else
    echo "  GC Benchmark  : not running"
  fi

  echo ""
  echo "Logs:"
  echo "  $LOG_DIR/dbstress.log"
  echo "  $LOG_DIR/gc-benchmark.log"
}

(
  cd "$ROOT_DIR"
  npm run build >/dev/null
)

start_dbstress
start_gc_benchmark
print_status
