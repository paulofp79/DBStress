"""Subprocess entrypoint for heavy GC Benchmark jobs.

The parent FastAPI process launches this script for workload, login-workload,
and schema-create jobs so the web server does not accumulate thousands of
threads or Oracle sockets.
"""

from __future__ import annotations

import json
import signal
import sys
import threading
import time
import traceback
from pathlib import Path

from login_workload import LoginWorkloadConfig, LoginWorkloadRunner
from oracle_session import (
    DEFAULT_SCHEMA_CALL_TIMEOUT_MS,
    build_connection_state,
    connect_from_state,
)
from schema import SchemaConfig, create_schema_parallel
from workload import WorkloadConfig, WorkloadRunner


def emit(event: str, **payload) -> None:
    """Write one JSON event to stdout for the parent controller."""
    message = {"event": event, **payload}
    sys.stdout.write(json.dumps(message, default=str) + "\n")
    sys.stdout.flush()


def load_spec(path: str) -> dict:
    with Path(path).open() as handle:
        return json.load(handle)


def install_signal_handlers(stop_requested: threading.Event) -> None:
    def _handle_signal(signum, _frame) -> None:
        stop_requested.set()
        emit("notice", message=f"Received signal {signum}; stopping child worker.")

    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signum, _handle_signal)
        except Exception:
            pass


def run_workload(spec: dict) -> int:
    shard_index = int(spec.get("shard_index", 0))
    cfg = WorkloadConfig(**dict(spec.get("config", {})))
    runner = WorkloadRunner()
    stop_requested = threading.Event()
    install_signal_handlers(stop_requested)

    def on_progress(status_dict: dict) -> None:
        emit("progress", shard_index=shard_index, data=status_dict)

    try:
        runner.prepare(cfg, prepare_callback=on_progress)
        if stop_requested.is_set():
            runner._stop_event.set()
            final_status = runner.stop(timeout=0.5)
            emit("complete", shard_index=shard_index, summary=final_status)
            return 0
        runner.start(progress_callback=on_progress)
        while runner.is_running and not stop_requested.is_set():
            time.sleep(0.5)
        if stop_requested.is_set():
            runner._stop_event.set()
        final_status = runner.stop(timeout=1.0)
        emit("complete", shard_index=shard_index, summary=final_status)
        return 0
    except Exception as exc:
        try:
            runner._stop_event.set()
            final_status = runner.stop(timeout=0.5)
        except Exception:
            final_status = {"running": False, "phase": "ERROR"}
        emit(
            "error",
            shard_index=shard_index,
            message=f"Workload shard failed: {exc}",
            details=traceback.format_exc(),
            summary=final_status,
        )
        return 1


def run_login_workload(spec: dict) -> int:
    shard_index = int(spec.get("shard_index", 0))
    cfg = LoginWorkloadConfig(**dict(spec.get("config", {})))
    runner = LoginWorkloadRunner()
    stop_requested = threading.Event()
    install_signal_handlers(stop_requested)

    def on_progress(status_dict: dict) -> None:
        emit("login_progress", shard_index=shard_index, data=status_dict)

    try:
        runner.start(cfg, progress_callback=on_progress)
        if stop_requested.is_set():
            runner._stop_event.set()
        while runner.is_running and not stop_requested.is_set():
            time.sleep(0.5)
        if stop_requested.is_set():
            runner._stop_event.set()
        final_status = runner.stop(timeout=1.0)
        emit("login_complete", shard_index=shard_index, summary=final_status)
        return 0
    except Exception as exc:
        try:
            runner._stop_event.set()
            final_status = runner.stop(timeout=0.5)
        except Exception:
            final_status = {"running": False, "phase": "ERROR"}
        emit(
            "error",
            shard_index=shard_index,
            message=f"Login workload shard failed: {exc}",
            details=traceback.format_exc(),
            summary=final_status,
        )
        return 1


def run_schema_create(spec: dict) -> int:
    cfg = SchemaConfig(**dict(spec.get("config", {})))
    connection_state = build_connection_state(spec.get("connection_state", {}))
    stop_requested = threading.Event()
    install_signal_handlers(stop_requested)

    def connection_factory():
        return connect_from_state(
            connection_state,
            call_timeout_ms=DEFAULT_SCHEMA_CALL_TIMEOUT_MS,
        )

    try:
        for message in create_schema_parallel(cfg, connection_factory):
            emit("schema_progress", message=message)
            if stop_requested.is_set():
                emit("schema_progress", message="Schema creation stop requested; terminating child.")
                return 2
        emit("schema_complete", message="Schema creation finished.")
        return 0
    except Exception as exc:
        emit(
            "schema_error",
            message=f"ERROR: {exc}",
            details=traceback.format_exc(),
        )
        return 1


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write("usage: job_worker.py <workload|login|schema-create> <spec.json>\n")
        return 2

    job_type = argv[1].strip().lower()
    spec = load_spec(argv[2])

    if job_type == "workload":
        return run_workload(spec)
    if job_type == "login":
        return run_login_workload(spec)
    if job_type == "schema-create":
        return run_schema_create(spec)

    sys.stderr.write(f"unknown job type: {job_type}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
