"""FastAPI application for the Oracle RAC GC Wait Benchmark Tool.

Serves the SPA frontend, exposes REST endpoints for connection
management, schema operations, workload execution, and result
retrieval, and provides a WebSocket channel for live progress
streaming during workload runs.

Run with:
    uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import configparser
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Optional
from uuid import uuid4

import oracledb
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles

from schema import (
    SchemaConfig,
    preview_ddl,
    drop_schema,
    drop_tables_by_prefix,
    get_table_names,
)
from metrics import (
    snapshot_system_events,
    snapshot_system_events_aggregated,
    compute_delta,
    compute_aggregated_delta,
    check_privileges,
    GC_SYSTEM_EVENTS,
    PRIMARY_GC_EVENTS,
)
from login_workload import (
    LoginWorkloadConfig,
    _MFES_PROCEDURE_NAME,
    call_mfes_online_session_procedure,
)
from oracle_session import (
    MAX_TOTAL_GC_WORKERS,
    MAX_TOTAL_LOGIN_WORKERS,
    SUBPROCESS_FORCE_KILL_SECONDS,
    SUBPROCESS_STOP_GRACE_SECONDS,
    build_connection_state,
    build_dsn_from_state,
    connect_from_state,
    shard_worker_counts,
)
from sql_replay import SqlReplayConfig
from workload import MAX_WORKLOAD_SEED_ROWS, WorkloadConfig
import report

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = str(BASE_DIR / "results.db")
CONFIG_PATH           = str(BASE_DIR / "config.ini")
RECENT_CONNS_PATH     = str(BASE_DIR / "connections.json")
RECENT_CPOOL_CONNS_PATH = str(BASE_DIR / "cpool_connections.json")
ACTIVE_WORKLOADS_PATH = str(BASE_DIR / "active_workloads.json")
RECENT_CONNS_MAX      = 5          # how many entries to keep

# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------

_conn_state: dict = {
    "host": "localhost",
    "port": 1521,
    "service_name": "orclpdb1",
    "user": "",
    "password": "",
    "mode": "thin",
}

_cpool_conn_state: dict = {
    "host": "",
    "port": 1521,
    "service_name": "",
    "user": "",
    "password": "",
    "mode": "thin",
}

_workload_runners: dict[str, Any] = {}
_workload_runners_lock = threading.Lock()
_active_workloads_file_lock = threading.Lock()
_login_runner: Optional[Any] = None
_sql_replay_runner: Optional[Any] = None
_sql_replay_runner_lock = threading.Lock()
_schema_job = None
_ws_clients: list[WebSocket] = []
_ws_lock = asyncio.Lock()

# Last schema configuration — populated when "Create Schema" runs so the
# workload endpoint can automatically record partition/compression metadata.
_schema_state: dict = {
    "table_prefix":    "GCB",
    "table_count":     10,
    "seed_rows":       500,
    "partition_type":  "NONE",
    "partition_detail": "",
    "compression":     "NONE",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dsn() -> str:
    """Build a connect-string from the current state."""
    return build_dsn_from_state(_conn_state)


def _get_connection():
    """Create a new Oracle connection from the current state."""
    return connect_from_state(_conn_state)


def _cpool_dsn() -> str:
    """Build a connect-string for the CDB pool-stats connection."""
    return build_dsn_from_state(_cpool_conn_state)


def _get_cpool_connection():
    """Create a new Oracle connection for GV$CPOOL_STATS queries."""
    return connect_from_state(_cpool_conn_state)


def _restart_pdb_with_cdb_connection(pdb_name: str) -> list[str]:
    """Close and reopen a PDB using the configured CDB connection."""
    name = (pdb_name or "").strip().upper()
    if not name:
        raise ValueError("PDB name is required when PDB restart is enabled.")
    if not (
        _cpool_conn_state.get("host")
        and _cpool_conn_state.get("service_name")
        and _cpool_conn_state.get("user")
        and _cpool_conn_state.get("password")
    ):
        raise ValueError("Set the CDB Connection first.")

    conn = _get_cpool_connection()
    steps: list[str] = []
    try:
        cur = conn.cursor()
        try:
            close_sql = f"ALTER PLUGGABLE DATABASE {name} CLOSE IMMEDIATE"
            open_sql = f"ALTER PLUGGABLE DATABASE {name} OPEN READ WRITE"
            steps.append(f"Executing: {close_sql}")
            cur.execute(close_sql)
            steps.append(f"PDB {name} closed.")
            steps.append(f"Executing: {open_sql}")
            cur.execute(open_sql)
            steps.append(f"PDB {name} opened READ WRITE.")
        finally:
            cur.close()
    finally:
        conn.close()
    return steps


def _normalize_login_sql(sql_text: str) -> str:
    """Validate the login simulation SQL and normalize trailing delimiters."""
    sql = (sql_text or "").strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    if not sql:
        raise ValueError("Enter a SQL query for Login Workload Simulation.")
    first_token = sql.split(None, 1)[0].upper()
    if first_token not in {"SELECT", "WITH"}:
        raise ValueError(
            "Login Workload Simulation only supports SELECT or WITH queries."
        )
    return sql


def _normalize_login_session_case(value: str) -> str:
    session_case = str(value or "SIMPLE_QUERY").strip().upper()
    if session_case not in {"SIMPLE_QUERY", "MFES_ONLINE"}:
        return "SIMPLE_QUERY"
    return session_case


def _normalize_login_module_name(value: str) -> str:
    module_name = str(value or "DBSTRESS_LOGIN_SESSION_00000000").strip()
    return module_name[:48] or "DBSTRESS_LOGIN_SESSION_00000000"


def _login_workload_procedure_ddl() -> str:
    return f"""
CREATE OR REPLACE PROCEDURE {_MFES_PROCEDURE_NAME} (pModuleName VARCHAR2)
IS
    pActionName      VARCHAR2(14);
    pModuleName_mod  VARCHAR2(48);
BEGIN
    pActionName := 'MFES_ONLINE';

    pModuleName_mod := substr(pModuleName, 1, 22)
                       || '0000000'
                       || substr(pModuleName, 22 + 8);

    DBMS_APPLICATION_INFO.SET_MODULE(pModuleName_mod, pActionName);
    DBMS_SESSION.SET_IDENTIFIER(pModuleName);

    EXECUTE IMMEDIATE 'ALTER SESSION SET OPTIMIZER_MODE = first_rows_1';
    EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_use_feedback" = false';
    EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_adaptive_cursor_sharing" = false';
    EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_extended_cursor_sharing_rel" = none';
END;
"""


def _get_login_procedure_status(state: Optional[dict] = None) -> dict:
    if not (state or _conn_state).get("password"):
        return {
            "ok": False,
            "exists": False,
            "valid": False,
            "name": _MFES_PROCEDURE_NAME,
            "message": "No password is set for this session.",
        }

    conn = connect_from_state(state or _conn_state)
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT status
                FROM user_objects
                WHERE object_type = 'PROCEDURE'
                  AND object_name = :name
                """,
                name=_MFES_PROCEDURE_NAME,
            )
            row = cur.fetchone()
            exists = bool(row)
            valid = bool(row and str(row[0]).upper() == "VALID")
            errors: list[str] = []
            if exists and not valid:
                cur.execute(
                    """
                    SELECT line, position, text
                    FROM user_errors
                    WHERE type = 'PROCEDURE'
                      AND name = :name
                    ORDER BY sequence
                    """,
                    name=_MFES_PROCEDURE_NAME,
                )
                errors = [
                    f"line {line}:{position} {text}"
                    for line, position, text in cur.fetchall()
                ]

            message = (
                f"Procedure {_MFES_PROCEDURE_NAME} is ready."
                if valid else
                (f"Procedure {_MFES_PROCEDURE_NAME} exists but is INVALID."
                 if exists else
                 f"Procedure {_MFES_PROCEDURE_NAME} is not created.")
            )
            return {
                "ok": True,
                "exists": exists,
                "valid": valid,
                "name": _MFES_PROCEDURE_NAME,
                "errors": errors,
                "message": message,
            }
        finally:
            cur.close()
    finally:
        conn.close()


def _create_login_procedure(state: Optional[dict] = None) -> dict:
    conn = connect_from_state(state or _conn_state)
    try:
        cur = conn.cursor()
        try:
            cur.execute(_login_workload_procedure_ddl())
        finally:
            cur.close()
        conn.commit()
    finally:
        conn.close()
    return _get_login_procedure_status(state)


def _drop_login_procedure(state: Optional[dict] = None) -> dict:
    conn = connect_from_state(state or _conn_state)
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP PROCEDURE {_MFES_PROCEDURE_NAME}';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -4043 THEN
                            RAISE;
                        END IF;
                END;
                """
            )
        finally:
            cur.close()
        conn.commit()
    finally:
        conn.close()
    status = _get_login_procedure_status(state)
    status["message"] = f"Procedure {_MFES_PROCEDURE_NAME} dropped."
    return status


def _validate_login_workload(
    sql_text: str,
    *,
    session_case: str = "SIMPLE_QUERY",
    module_name: str = "DBSTRESS_LOGIN_SESSION_00000000",
    state: Optional[dict] = None,
) -> None:
    """Run the login-simulation path once before starting many workers."""
    conn = connect_from_state(state or _conn_state)
    try:
        cur = conn.cursor()
        try:
            if _normalize_login_session_case(session_case) == "MFES_ONLINE":
                call_mfes_online_session_procedure(
                    cur,
                    _normalize_login_module_name(module_name),
                )
            else:
                try:
                    conn.module = "LOGIN_WORKLOAD_SIM"
                    conn.action = "preflight"
                except Exception:
                    pass
            cur.arraysize = 1
            cur.execute(sql_text)
            cur.fetchmany(1)
        finally:
            cur.close()
    finally:
        conn.close()


def _load_recent_conns() -> list[dict]:
    """Return the saved list of recent connections (no passwords)."""
    if not os.path.exists(RECENT_CONNS_PATH):
        return []
    try:
        with open(RECENT_CONNS_PATH) as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_recent_conn(entry: dict) -> None:
    """Prepend *entry* to the recent-connections list and persist to disk.

    Deduplicates on (host, port, service_name, user) — if the same
    combination already exists it is removed before re-inserting at the
    front, so the list is always ordered most-recent-first.
    Passwords are never stored.
    """
    record = {
        "host":         entry.get("host", ""),
        "port":         int(entry.get("port", 1521)),
        "service_name": entry.get("service_name", ""),
        "user":         entry.get("user", ""),
        "mode":         entry.get("mode", "thin"),
        "saved_at":     datetime.now(timezone.utc).isoformat(),
    }
    record["label"] = (
        f"{record['user']}@{record['host']}:{record['port']}/{record['service_name']}"
    )

    key = (record["host"], record["port"], record["service_name"], record["user"])
    existing = [
        c for c in _load_recent_conns()
        if (c.get("host"), c.get("port"), c.get("service_name"), c.get("user")) != key
    ]
    updated = [record] + existing
    updated = updated[:RECENT_CONNS_MAX]

    with open(RECENT_CONNS_PATH, "w") as f:
        json.dump(updated, f, indent=2)


def _load_recent_cpool_conns() -> list[dict]:
    """Return the saved list of recent CDB pool-stat connections."""
    if not os.path.exists(RECENT_CPOOL_CONNS_PATH):
        return []
    try:
        with open(RECENT_CPOOL_CONNS_PATH) as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_recent_cpool_conn(entry: dict) -> None:
    """Persist a recent CDB connection without storing the password."""
    record = {
        "host": entry.get("host", ""),
        "port": int(entry.get("port", 1521)),
        "service_name": entry.get("service_name", ""),
        "user": entry.get("user", ""),
        "mode": entry.get("mode", "thin"),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    record["label"] = (
        f"{record['user']}@{record['host']}:{record['port']}/{record['service_name']}"
    )

    key = (record["host"], record["port"], record["service_name"], record["user"])
    existing = [
        c for c in _load_recent_cpool_conns()
        if (c.get("host"), c.get("port"), c.get("service_name"), c.get("user")) != key
    ]
    updated = [record] + existing
    updated = updated[:RECENT_CONNS_MAX]

    with open(RECENT_CPOOL_CONNS_PATH, "w") as f:
        json.dump(updated, f, indent=2)


def _load_active_workload_records() -> dict[str, dict]:
    """Return the persisted active workload registry."""
    if not os.path.exists(ACTIVE_WORKLOADS_PATH):
        return {}
    try:
        with open(ACTIVE_WORKLOADS_PATH) as handle:
            data = json.load(handle)
        workloads = data.get("workloads", {}) if isinstance(data, dict) else {}
        return workloads if isinstance(workloads, dict) else {}
    except Exception:
        return {}


def _save_active_workload_records(records: dict[str, dict]) -> None:
    """Persist the active workload registry atomically."""
    payload = {"workloads": records}
    handle = NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix="gcb-active-workloads-",
        dir=str(BASE_DIR),
        delete=False,
    )
    try:
        json.dump(payload, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = handle.name
    finally:
        handle.close()
    os.replace(temp_path, ACTIVE_WORKLOADS_PATH)


def _persist_active_workload(
    workload_id: str,
    payload: dict,
    *,
    child_pids: Optional[list[int]] = None,
) -> None:
    """Upsert one workload record used for restart recovery."""
    if not workload_id:
        return
    record = dict(payload or {})
    record["workload_id"] = workload_id
    if child_pids is not None:
        record["child_pids"] = [
            int(pid) for pid in child_pids
            if str(pid).strip() and int(pid) > 0
        ]
    elif "child_pids" in record:
        record["child_pids"] = [
            int(pid) for pid in record.get("child_pids", [])
            if str(pid).strip() and int(pid) > 0
        ]
    else:
        record["child_pids"] = []
    record["updated_at"] = datetime.now(timezone.utc).isoformat()

    with _active_workloads_file_lock:
        records = _load_active_workload_records()
        records[workload_id] = record
        _save_active_workload_records(records)


def _remove_persisted_active_workload(workload_id: str) -> None:
    """Delete one workload from the restart recovery registry."""
    if not workload_id:
        return
    with _active_workloads_file_lock:
        records = _load_active_workload_records()
        if workload_id not in records:
            return
        records.pop(workload_id, None)
        _save_active_workload_records(records)


def _get_persisted_active_workload_record(workload_id: str) -> Optional[dict]:
    """Return one raw workload record from the persisted registry."""
    if not workload_id:
        return None
    with _active_workloads_file_lock:
        records = _load_active_workload_records()
        record = records.get(workload_id)
        return dict(record) if isinstance(record, dict) else None


def _pid_is_alive(pid: int) -> bool:
    """True when *pid* currently exists on this host."""
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def _build_recovered_workload_payload(record: dict, alive_pids: list[int]) -> dict:
    """Return a UI payload for a workload recovered from disk after restart."""
    payload = {
        key: value
        for key, value in dict(record or {}).items()
        if key not in {"child_pids", "updated_at"}
    }
    prior_phase = str(payload.get("phase", "") or "").upper()
    prior_message = str(payload.get("status_message", "") or "").strip()
    payload["workload_id"] = str(payload.get("workload_id", "") or "")
    payload["running"] = True
    payload["phase"] = "STOPPING" if prior_phase == "STOPPING" else "RECOVERED"
    payload["orphaned"] = True
    payload["live_updates_available"] = False
    payload["recovered_child_process_count"] = len(alive_pids)
    if prior_phase == "STOPPING" and prior_message:
        payload["status_message"] = prior_message
    else:
        payload["status_message"] = (
            "Recovered after app restart. Child workload process(es) are still running, "
            "but live progress updates are unavailable until you stop them or they finish."
        )
    if not payload.get("execution_model"):
        payload["execution_model"] = "recovered child-process workers"
    if not payload.get("physical_workers"):
        payload["physical_workers"] = int(payload.get("thread_count", 0) or 0)
    if not payload.get("process_count"):
        payload["process_count"] = len(alive_pids)
    return payload


def _list_recovered_active_workloads() -> list[dict]:
    """Return persisted workloads whose child processes survived an app restart."""
    recovered: list[dict] = []
    changed = False

    with _active_workloads_file_lock:
        records = _load_active_workload_records()
        for workload_id, record in list(records.items()):
            child_pids = [
                int(pid) for pid in record.get("child_pids", [])
                if str(pid).strip()
            ]
            alive_pids = [pid for pid in child_pids if _pid_is_alive(pid)]
            if not alive_pids:
                records.pop(workload_id, None)
                changed = True
                continue
            if alive_pids != child_pids:
                record["child_pids"] = alive_pids
                record["updated_at"] = datetime.now(timezone.utc).isoformat()
                records[workload_id] = record
                changed = True
            recovered.append(_build_recovered_workload_payload(record, alive_pids))

        if changed:
            _save_active_workload_records(records)

    return recovered


def _stop_recovered_workload(record: dict) -> dict:
    """Stop child processes for a workload found only in the persisted registry."""
    workload_id = str(record.get("workload_id", "") or "").strip()
    child_pids = [
        int(pid) for pid in record.get("child_pids", [])
        if str(pid).strip()
    ]
    alive_pids = [pid for pid in child_pids if _pid_is_alive(pid)]
    if not alive_pids:
        _remove_persisted_active_workload(workload_id)
        payload = _build_recovered_workload_payload(record, [])
        payload["running"] = False
        payload["phase"] = "STOPPED"
        payload["status_message"] = "Recovered workload is no longer running."
        return payload

    for pid in alive_pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass

    deadline = time.monotonic() + SUBPROCESS_STOP_GRACE_SECONDS
    while time.monotonic() < deadline:
        remaining = [pid for pid in alive_pids if _pid_is_alive(pid)]
        if not remaining:
            break
        time.sleep(0.2)

    remaining = [pid for pid in alive_pids if _pid_is_alive(pid)]
    if remaining:
        for pid in remaining:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass

        deadline = time.monotonic() + SUBPROCESS_FORCE_KILL_SECONDS
        while time.monotonic() < deadline:
            remaining = [pid for pid in remaining if _pid_is_alive(pid)]
            if not remaining:
                break
            time.sleep(0.2)

    remaining = [pid for pid in alive_pids if _pid_is_alive(pid)]
    if remaining:
        persisted = _build_recovered_workload_payload(record, remaining)
        persisted["phase"] = "STOPPING"
        persisted["status_message"] = (
            "Stop requested for recovered workload. Waiting for child process(es) to exit."
        )
        _persist_active_workload(workload_id, persisted, child_pids=remaining)
        return persisted

    _remove_persisted_active_workload(workload_id)
    payload = _build_recovered_workload_payload(record, [])
    payload["running"] = False
    payload["phase"] = "STOPPED"
    payload["status_message"] = "Recovered workload stopped."
    return payload


def _load_config() -> None:
    """Load saved connection settings from config.ini (no password)."""
    if not os.path.exists(CONFIG_PATH):
        return
    cp = configparser.ConfigParser()
    cp.read(CONFIG_PATH)
    if "oracle" in cp:
        sec = cp["oracle"]
        _conn_state["host"] = sec.get("host", _conn_state["host"])
        _conn_state["port"] = int(sec.get("port", _conn_state["port"]))
        _conn_state["service_name"] = sec.get("service_name", _conn_state["service_name"])
        _conn_state["user"] = sec.get("user", _conn_state["user"])
        _conn_state["mode"] = sec.get("mode", _conn_state["mode"])


def _save_config() -> None:
    """Persist connection settings to config.ini (no password)."""
    cp = configparser.ConfigParser()
    cp["oracle"] = {
        "host": _conn_state["host"],
        "port": str(_conn_state["port"]),
        "service_name": _conn_state["service_name"],
        "user": _conn_state["user"],
        "mode": _conn_state["mode"],
    }
    with open(CONFIG_PATH, "w") as f:
        cp.write(f)


def _connection_key(
    host: str = "",
    port: int = 0,
    service_name: str = "",
    user: str = "",
) -> dict:
    """Return a comparable connection identity payload."""
    try:
        port_value = int(port or 0)
    except Exception:
        port_value = 0
    return {
        "host": str(host or "").strip(),
        "port": port_value,
        "service_name": str(service_name or "").strip(),
        "user": str(user or "").strip(),
    }


def _connection_key_from_state(state: dict) -> dict:
    """Build a connection identity from a connection-state dict."""
    return _connection_key(
        host=state.get("host", ""),
        port=state.get("port", 0),
        service_name=state.get("service_name", ""),
        user=state.get("user", ""),
    )


def _normalized_connection_key(key: Optional[dict]) -> Optional[tuple[str, int, str, str]]:
    """Return a normalized tuple for case-insensitive connection matching."""
    if not key:
        return None
    raw = _connection_key(
        host=key.get("host", ""),
        port=key.get("port", 0),
        service_name=key.get("service_name", ""),
        user=key.get("user", ""),
    )
    return (
        raw["host"].lower(),
        raw["port"],
        raw["service_name"].lower(),
        raw["user"].lower(),
    )


def _has_connection_key(key: Optional[dict]) -> bool:
    """True when at least one identity field is populated."""
    if not key:
        return False
    raw = _connection_key(
        host=key.get("host", ""),
        port=key.get("port", 0),
        service_name=key.get("service_name", ""),
        user=key.get("user", ""),
    )
    return bool(raw["host"] or raw["service_name"] or raw["user"])


def _same_connection(left: Optional[dict], right: Optional[dict]) -> bool:
    """Return True when two connection identities refer to the same DB login."""
    if not (_has_connection_key(left) and _has_connection_key(right)):
        return False
    return _normalized_connection_key(left) == _normalized_connection_key(right)


def _format_connection_key(key: Optional[dict]) -> str:
    """Render a connection identity in the same form the UI uses."""
    if not _has_connection_key(key):
        return "unknown connection"
    raw = _connection_key(
        host=key.get("host", ""),
        port=key.get("port", 0),
        service_name=key.get("service_name", ""),
        user=key.get("user", ""),
    )
    return (
        f"{raw['user'] or '?'}@{raw['host'] or '?'}:{raw['port'] or '?'}"
        f"/{raw['service_name'] or '?'}"
    )


def _workload_message(
    msg: dict,
    connection_key: Optional[dict],
    workload_id: str = "",
) -> dict:
    """Attach workload connection metadata so clients can scope live events."""
    payload = dict(msg)
    if workload_id:
        payload["workload_id"] = workload_id
    if _has_connection_key(connection_key):
        payload["connection_key"] = _connection_key(
            host=connection_key.get("host", ""),
            port=connection_key.get("port", 0),
            service_name=connection_key.get("service_name", ""),
            user=connection_key.get("user", ""),
        )
        payload["connection_label"] = _format_connection_key(connection_key)
    return payload


async def _broadcast(msg: dict) -> None:
    """Send a JSON message to all connected WebSocket clients."""
    data = json.dumps(msg)
    async with _ws_lock:
        dead: list[WebSocket] = []
        for ws in _ws_clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _ws_clients.remove(ws)


def _new_workload_id() -> str:
    """Return a short workload identifier safe to expose in the UI."""
    return uuid4().hex[:12]


def _build_workload_status_payload(
    controller: Any,
    workload_id: str,
    *,
    status_override: Optional[dict] = None,
) -> dict:
    """Return one UI-ready workload status payload."""
    payload = dict(status_override or controller.status.to_dict())
    payload["workload_id"] = workload_id

    runner_connection = getattr(controller, "_connection_key", None)
    if _has_connection_key(runner_connection):
        payload["connection_key"] = _connection_key(
            host=runner_connection.get("host", ""),
            port=runner_connection.get("port", 0),
            service_name=runner_connection.get("service_name", ""),
            user=runner_connection.get("user", ""),
        )
        payload["connection_label"] = _format_connection_key(runner_connection)

    cfg = getattr(controller, "_config", None)
    schema_name = getattr(controller, "_schema_name", "")
    if cfg:
        payload.update({
            "schema_name": schema_name or getattr(cfg, "table_prefix", "GCB"),
            "table_prefix": getattr(cfg, "table_prefix", "GCB"),
            "table_count": getattr(cfg, "table_count", 0),
            "thread_count": getattr(cfg, "thread_count", 0),
            "requested_threads": getattr(cfg, "thread_count", 0),
            "physical_workers": getattr(controller, "_worker_count", 0),
            "process_count": getattr(controller, "_process_count", 0),
            "duration": getattr(cfg, "duration_seconds", payload.get("duration", 0)),
            "seed_rows": getattr(cfg, "seed_rows", 0),
            "contention_mode": getattr(cfg, "contention_mode", "NORMAL"),
            "execution_model": "pooled child-process workers",
        })
    elif schema_name:
        payload["schema_name"] = schema_name

    created_at = getattr(controller, "_created_at", "")
    if created_at:
        payload["created_at"] = created_at

    return payload


def _is_active_workload_payload(payload: Optional[dict]) -> bool:
    """True while a workload should stay visible as active in the UI."""
    if not payload:
        return False
    if bool(payload.get("running")):
        return True
    return str(payload.get("phase", "")).upper() in {"PREPARING", "WARMING", "RUNNING", "STOPPING"}


def _register_workload_runner(workload_id: str, controller: Any) -> None:
    with _workload_runners_lock:
        _workload_runners[workload_id] = controller


def _remove_workload_runner(workload_id: str, controller: Optional[Any] = None) -> None:
    with _workload_runners_lock:
        current = _workload_runners.get(workload_id)
        if current is None:
            return
        if controller is not None and current is not controller:
            return
        _workload_runners.pop(workload_id, None)


def _get_workload_runner(workload_id: str) -> Optional[Any]:
    with _workload_runners_lock:
        return _workload_runners.get(workload_id)


def _list_workload_runners() -> list[tuple[str, Any]]:
    with _workload_runners_lock:
        return list(_workload_runners.items())


def _build_schema_config(data: dict) -> SchemaConfig:
    """Create a SchemaConfig from request JSON."""
    return SchemaConfig(
        table_prefix=data.get("table_prefix", "GCB"),
        table_count=max(1, int(data.get("table_count", 10))),
        partition_type=data.get("partition_type", "NONE"),
        partition_count=int(data.get("partition_count", 8)),
        range_interval=data.get("range_interval", "MONTHLY"),
        compression=data.get("compression", "NONE"),
        seed_rows=max(0, int(data.get("seed_rows", 500))),
    )


def _query_instance_counts(conn, view_name: str, label: str) -> dict:
    """Return per-instance and total counts from a GV$ view."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT inst_id, COUNT(*) AS current_count
            FROM   {view_name}
            GROUP  BY inst_id
            ORDER  BY inst_id
            """
        )
        rows = cursor.fetchall()
        per_instance = [
            {"inst_id": int(inst_id), "count": int(count)}
            for inst_id, count in rows
        ]
        return {
            "label": label,
            "total": sum(item["count"] for item in per_instance),
            "instances": per_instance,
        }
    finally:
        cursor.close()


def _query_session_counts(conn, username: str | None = None) -> dict:
    """Return per-instance session counts, optionally filtered by username."""
    cursor = conn.cursor()
    try:
        binds = {}
        where_clause = ""
        normalized = (username or "").strip().upper()
        if normalized:
            where_clause = "WHERE username = :username"
            binds["username"] = normalized

        cursor.execute(
            f"""
            SELECT inst_id, COUNT(*) AS current_count
            FROM   gv$session
            {where_clause}
            GROUP  BY inst_id
            ORDER  BY inst_id
            """,
            binds,
        )
        rows = cursor.fetchall()
        per_instance = [
            {"inst_id": int(inst_id), "count": int(count)}
            for inst_id, count in rows
        ]
        label = "Sessions"
        if normalized:
            label = f"Sessions ({normalized})"
        return {
            "label": label,
            "username_filter": normalized,
            "total": sum(item["count"] for item in per_instance),
            "instances": per_instance,
        }
    finally:
        cursor.close()


def _query_connection_pool_stats(conn) -> list[dict]:
    """Return current GV$CPOOL_STATS rows."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT pool_name,
                   num_open_servers,
                   num_busy_servers,
                   num_hits,
                   num_misses,
                   num_purged
            FROM   gv$cpool_stats
            ORDER  BY pool_name
            """
        )
        rows = cursor.fetchall()
        return [
            {
                "pool_name": row[0],
                "num_open_servers": int(row[1] or 0),
                "num_busy_servers": int(row[2] or 0),
                "num_hits": int(row[3] or 0),
                "num_misses": int(row[4] or 0),
                "num_purged": int(row[5] or 0),
            }
            for row in rows
        ]
    finally:
        cursor.close()


def _parse_api_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Start and end time are required.")
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for pattern in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%y %H:%M"):
        try:
            return datetime.strptime(raw, pattern)
        except ValueError:
            continue
    raise ValueError(f"Invalid datetime value: {value}")


def _coerce_number(text: str) -> int | float | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        try:
            return float(raw)
        except Exception:
            return None


def _safe_sql_statement_type(sql_text: str) -> str:
    sql = str(sql_text or "").strip()
    if not sql:
        return "UNKNOWN"
    first = sql.split(None, 1)[0].upper()
    return first if first in {"SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "BEGIN", "DECLARE"} else "UNKNOWN"


def _fetch_peak_replay_analysis(
    conn,
    *,
    dbid: int,
    start_time: datetime,
    end_time: datetime,
    top_n: int,
) -> dict:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM   dba_hist_active_sess_history
            WHERE  dbid = :dbid
            AND    sample_time BETWEEN :start_time AND :end_time
            """,
            {"dbid": dbid, "start_time": start_time, "end_time": end_time},
        )
        total_samples = int(cursor.fetchone()[0] or 0)
        if total_samples <= 0:
            return {
                "dbid": dbid,
                "start_time": start_time.isoformat(timespec="minutes"),
                "end_time": end_time.isoformat(timespec="minutes"),
                "sample_count": 0,
                "waits": [],
                "sql": [],
                "summary": "No ASH rows found for that interval.",
            }

        cursor.execute(
            """
            SELECT NVL(event, 'ON CPU') AS event_name,
                   NVL(wait_class, 'CPU') AS wait_class,
                   COUNT(*) AS sample_count
            FROM   dba_hist_active_sess_history
            WHERE  dbid = :dbid
            AND    sample_time BETWEEN :start_time AND :end_time
            GROUP  BY event, wait_class
            ORDER  BY sample_count DESC
            FETCH FIRST 10 ROWS ONLY
            """,
            {"dbid": dbid, "start_time": start_time, "end_time": end_time},
        )
        waits = [
            {
                "event": row[0],
                "wait_class": row[1],
                "sample_count": int(row[2] or 0),
                "sample_pct": round((int(row[2] or 0) / total_samples) * 100, 1),
            }
            for row in cursor.fetchall()
        ]

        safe_top_n = max(1, min(25, int(top_n or 8)))
        cursor.execute(
            f"""
            WITH ash AS (
                SELECT COALESCE(sql_id, top_level_sql_id) AS effective_sql_id,
                       event,
                       wait_class,
                       session_id,
                       session_serial#,
                       instance_number
                FROM   dba_hist_active_sess_history
                WHERE  dbid = :dbid
                AND    sample_time BETWEEN :start_time AND :end_time
                AND    COALESCE(sql_id, top_level_sql_id) IS NOT NULL
            ),
            ranked AS (
                SELECT effective_sql_id AS sql_id,
                       COUNT(*) AS sample_count,
                       COUNT(DISTINCT TO_CHAR(session_id) || ':' || TO_CHAR(session_serial#) || ':' || TO_CHAR(instance_number)) AS session_count
                FROM   ash
                GROUP  BY effective_sql_id
            ),
            top_event AS (
                SELECT effective_sql_id AS sql_id,
                       NVL(event, 'ON CPU') AS event_name,
                       NVL(wait_class, 'CPU') AS wait_class,
                       COUNT(*) AS event_samples,
                       ROW_NUMBER() OVER (
                           PARTITION BY effective_sql_id
                           ORDER BY COUNT(*) DESC, NVL(event, 'ON CPU')
                       ) AS rn
                FROM   ash
                GROUP  BY effective_sql_id, event, wait_class
            )
            SELECT r.sql_id,
                   r.sample_count,
                   r.session_count,
                   NVL(t.event_name, 'ON CPU') AS primary_event,
                   NVL(t.wait_class, 'CPU') AS primary_wait_class,
                   NVL(t.event_samples, r.sample_count) AS primary_event_samples,
                   s.sql_text
            FROM   ranked r
                   LEFT JOIN top_event t
                          ON t.sql_id = r.sql_id
                         AND t.rn = 1
                   LEFT JOIN dba_hist_sqltext s
                          ON s.dbid = :dbid
                         AND s.sql_id = r.sql_id
            ORDER  BY r.sample_count DESC
            FETCH FIRST {safe_top_n} ROWS ONLY
            """,
            {
                "dbid": dbid,
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        sql_rows = cursor.fetchall()
        statements: list[dict] = []
        for row in sql_rows:
            sql_id = str(row[0] or "")
            sample_count = int(row[1] or 0)
            statement = {
                "sql_id": sql_id,
                "sample_count": sample_count,
                "sample_pct": round((sample_count / total_samples) * 100, 1),
                "session_count": int(row[2] or 0),
                "primary_event": row[3] or "ON CPU",
                "primary_wait_class": row[4] or "CPU",
                "primary_event_samples": int(row[5] or 0),
                "module": "",
                "program": "",
                "top_object_id": 0,
                "sql_text": str(row[6] or "").strip(),
            }
            statement["statement_type"] = _safe_sql_statement_type(statement["sql_text"])
            statement["replayable"] = statement["statement_type"] != "UNKNOWN" and bool(statement["sql_text"])
            statement["weight"] = max(1, sample_count)
            cursor.execute(
                """
                SELECT NVL(module, ''),
                       NVL(program, ''),
                       NVL(current_obj#, 0),
                       COUNT(*) AS sample_count
                FROM   dba_hist_active_sess_history
                WHERE  dbid = :dbid
                AND    sample_time BETWEEN :start_time AND :end_time
                AND    COALESCE(sql_id, top_level_sql_id) = :sql_id
                GROUP  BY module, program, current_obj#
                ORDER  BY sample_count DESC
                FETCH FIRST 1 ROWS ONLY
                """,
                {
                    "dbid": dbid,
                    "start_time": start_time,
                    "end_time": end_time,
                    "sql_id": statement["sql_id"],
                },
            )
            top_dim = cursor.fetchone()
            if top_dim:
                statement["module"] = top_dim[0] or ""
                statement["program"] = top_dim[1] or ""
                statement["top_object_id"] = int(top_dim[2] or 0)
            statements.append(statement)

        for statement in statements:
            cursor.execute(
                """
                SELECT position,
                       MAX(name) KEEP (DENSE_RANK LAST ORDER BY last_captured) AS bind_name,
                       MAX(datatype_string) KEEP (DENSE_RANK LAST ORDER BY last_captured) AS datatype_string,
                       MAX(CASE WHEN was_captured = 'YES' THEN 1 ELSE 0 END) AS has_sample,
                       MAX(CASE
                               WHEN was_captured = 'YES'
                               AND REGEXP_LIKE(datatype_string, 'NUMBER|INTEGER|DECIMAL', 'i')
                               THEN TO_CHAR(ANYDATA.ACCESSNUMBER(value_anydata))
                           END) AS number_text,
                       MAX(CASE
                               WHEN was_captured = 'YES'
                               AND REGEXP_LIKE(datatype_string, 'DATE', 'i')
                               THEN TO_CHAR(ANYDATA.ACCESSDATE(value_anydata), 'YYYY-MM-DD HH24:MI:SS')
                           END) AS date_text,
                       MAX(CASE
                               WHEN was_captured = 'YES'
                               AND REGEXP_LIKE(datatype_string, 'TIMESTAMP', 'i')
                               THEN TO_CHAR(ANYDATA.ACCESSTIMESTAMP(value_anydata), 'YYYY-MM-DD HH24:MI:SS')
                           END) AS timestamp_text,
                       MAX(CASE
                               WHEN was_captured = 'YES'
                               THEN SUBSTR(ANYDATA.ACCESSVARCHAR2(value_anydata), 1, 200)
                           END) AS varchar_text
                FROM   dba_hist_sqlbind
                WHERE  dbid = :dbid
                AND    sql_id = :sql_id
                GROUP  BY position
                ORDER  BY position
                """,
                {"dbid": dbid, "sql_id": statement["sql_id"]},
            )
            binds = []
            captured_values = 0
            for bind_row in cursor.fetchall():
                sample_text = bind_row[4] or bind_row[5] or bind_row[6] or bind_row[7] or ""
                sample_kind = "text"
                if bind_row[4]:
                    sample_kind = "number"
                elif bind_row[5]:
                    sample_kind = "date"
                elif bind_row[6]:
                    sample_kind = "timestamp"
                bind_entry = {
                    "position": int(bind_row[0] or 0),
                    "name": bind_row[1] or "",
                    "datatype": bind_row[2] or "",
                    "sample_value_text": str(sample_text or ""),
                    "sample_value_kind": sample_kind if sample_text else "",
                }
                numeric_value = _coerce_number(bind_entry["sample_value_text"]) if sample_kind == "number" else None
                bind_entry["sample_value"] = numeric_value if numeric_value is not None else None
                if sample_text:
                    captured_values += 1
                binds.append(bind_entry)
            statement["binds"] = binds
            if not binds:
                statement["bind_quality"] = "none"
            elif captured_values > 0:
                statement["bind_quality"] = "captured"
            else:
                statement["bind_quality"] = "metadata"

        summary = waits[0]["event"] if waits else "Unknown"
        return {
            "dbid": dbid,
            "start_time": start_time.isoformat(timespec="minutes"),
            "end_time": end_time.isoformat(timespec="minutes"),
            "sample_count": total_samples,
            "waits": waits,
            "sql": statements,
            "summary": f"Dominant event: {summary}. Top SQL count: {len(statements)}.",
        }
    finally:
        cursor.close()


def _kill_sessions_for_user(connection, username: str):
    """Kill all non-background sessions for a database user except the current control session."""
    username = (username or "").strip().upper()
    if not username:
        yield "ERROR: Username is required."
        return

    cursor = connection.cursor()
    try:
        yield "Lookup command: SELECT SYS_CONTEXT('USERENV', 'SESSIONID') FROM dual"
        cursor.execute(
            """
            SELECT SYS_CONTEXT('USERENV', 'SESSIONID')
            FROM   dual
            """
        )
        current_audsid = cursor.fetchone()[0]
        yield (
            "Lookup query: SELECT inst_id, sid, serial#, username, status "
            f"FROM gv$session WHERE username = '{username}' "
            "AND type <> 'BACKGROUND' "
            f"AND audsid <> {current_audsid} "
            "ORDER BY inst_id, sid"
        )

        cursor.execute(
            """
            SELECT inst_id, sid, serial#, username, status
            FROM   gv$session
            WHERE  username = :username
            AND    type <> 'BACKGROUND'
            AND    audsid <> :current_audsid
            ORDER  BY inst_id, sid
            """,
            {"username": username, "current_audsid": current_audsid},
        )
        sessions = cursor.fetchall()

        if not sessions:
            yield f"No active sessions found for user {username}."
            return

        total = len(sessions)
        for i, (inst_id, sid, serial, session_user, status) in enumerate(sessions, 1):
            kill_sql = (
                f"ALTER SYSTEM KILL SESSION "
                f"'{int(sid)},{int(serial)},@{int(inst_id)}' IMMEDIATE"
            )
            yield f"[{i}/{total}] Command: {kill_sql}"
            try:
                cursor.execute(kill_sql)
                yield (
                    f"[{i}/{total}] Killed session sid={sid}, serial#={serial}, "
                    f"inst_id={inst_id}, user={session_user}, status={status}"
                )
            except Exception as exc:
                yield (
                    f"[{i}/{total}] ERROR killing session sid={sid}, serial#={serial}, "
                    f"inst_id={inst_id}: {exc}"
                )

        yield f"Session cleanup complete for user {username}."
    finally:
        cursor.close()


def _discover_sessions_for_user(connection, username: str) -> tuple[int, list[tuple[int, int, int, str, str]]]:
    """Return current control-session audsid and kill targets for a user."""
    username = (username or "").strip().upper()
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT SYS_CONTEXT('USERENV', 'SESSIONID')
            FROM   dual
            """
        )
        current_audsid = int(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT inst_id, sid, serial#, username, status
            FROM   gv$session
            WHERE  username = :username
            AND    type <> 'BACKGROUND'
            AND    audsid <> :current_audsid
            ORDER  BY inst_id, sid
            """,
            {"username": username, "current_audsid": current_audsid},
        )
        sessions = [
            (int(inst_id), int(sid), int(serial), session_user, status)
            for inst_id, sid, serial, session_user, status in cursor.fetchall()
        ]
        return current_audsid, sessions
    finally:
        cursor.close()


def _kill_sessions_for_user_parallel(connection_factory, username: str):
    """Kill many sessions for a user in parallel using multiple control sessions."""
    username = (username or "").strip().upper()
    if not username:
        yield "ERROR: Username is required."
        return

    lookup_conn = connection_factory()
    try:
        current_audsid, sessions = _discover_sessions_for_user(lookup_conn, username)
    finally:
        try:
            lookup_conn.close()
        except Exception:
            pass

    yield "Lookup command: SELECT SYS_CONTEXT('USERENV', 'SESSIONID') FROM dual"
    yield (
        "Lookup query: SELECT inst_id, sid, serial#, username, status "
        f"FROM gv$session WHERE username = '{username}' "
        "AND type <> 'BACKGROUND' "
        f"AND audsid <> {current_audsid} "
        "ORDER BY inst_id, sid"
    )

    if not sessions:
        yield f"No active sessions found for user {username}."
        return

    total = len(sessions)
    worker_count = min(10, max(1, (total + 99) // 100))
    chunk_size = (total + worker_count - 1) // worker_count
    yield (
        f"Parallel kill plan: {total} sessions, {worker_count} killer sessions, "
        f"about {chunk_size} target sessions per killer."
    )

    progress_queue: queue.Queue[str] = queue.Queue()

    def _worker(worker_idx: int, chunk: list[tuple[int, int, int, str, str]]) -> None:
        conn = None
        cursor = None
        try:
            conn = connection_factory()
            cursor = conn.cursor()
            progress_queue.put(
                f"[worker {worker_idx}] Opened killer control session for {len(chunk)} target sessions"
            )
            for inst_id, sid, serial, session_user, status in chunk:
                kill_sql = f"ALTER SYSTEM KILL SESSION '{sid},{serial},@{inst_id}' IMMEDIATE"
                progress_queue.put(f"[worker {worker_idx}] Command: {kill_sql}")
                try:
                    cursor.execute(kill_sql)
                    progress_queue.put(
                        f"[worker {worker_idx}] Killed session sid={sid}, serial#={serial}, "
                        f"inst_id={inst_id}, user={session_user}, status={status}"
                    )
                except Exception as exc:
                    progress_queue.put(
                        f"[worker {worker_idx}] ERROR killing session sid={sid}, serial#={serial}, "
                        f"inst_id={inst_id}: {exc}"
                    )
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    chunks = [
        sessions[i:i + chunk_size]
        for i in range(0, total, chunk_size)
    ]

    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = [
            executor.submit(_worker, idx + 1, chunk)
            for idx, chunk in enumerate(chunks)
        ]

        while True:
            try:
                yield progress_queue.get(timeout=0.2)
                continue
            except queue.Empty:
                pass
            if all(f.done() for f in futures):
                break

        while not progress_queue.empty():
            yield progress_queue.get()

        for future in futures:
            future.result()

    yield f"Session cleanup complete for user {username}."


# ---------------------------------------------------------------------------
# Subprocess controllers
# ---------------------------------------------------------------------------

def _last_nonempty(values: list[str]) -> str:
    for value in reversed(values):
        if value:
            return value
    return ""


def _aggregate_workload_metrics(statuses: list[dict]) -> dict:
    return {
        "inserts": sum(int(s.get("inserts", 0) or 0) for s in statuses),
        "updates": sum(int(s.get("updates", 0) or 0) for s in statuses),
        "deletes": sum(int(s.get("deletes", 0) or 0) for s in statuses),
        "selects": sum(int(s.get("selects", 0) or 0) for s in statuses),
        "select_intos": sum(int(s.get("select_intos", 0) or 0) for s in statuses),
        "pedt_updates": sum(int(s.get("pedt_updates", 0) or 0) for s in statuses),
        "errors": sum(int(s.get("errors", 0) or 0) for s in statuses),
    }


def _aggregate_login_metrics(statuses: list[dict]) -> dict:
    cycles = sum(int(s.get("cycles", 0) or 0) for s in statuses)
    weighted_cycle_ms = sum(
        float(s.get("avg_cycle_ms", 0) or 0) * int(s.get("cycles", 0) or 0)
        for s in statuses
    )
    avg_cycle_ms = (weighted_cycle_ms / cycles) if cycles > 0 else 0.0
    return {
        "logons": sum(int(s.get("logons", 0) or 0) for s in statuses),
        "queries": sum(int(s.get("queries", 0) or 0) for s in statuses),
        "logouts": sum(int(s.get("logouts", 0) or 0) for s in statuses),
        "cycles": cycles,
        "errors": sum(int(s.get("errors", 0) or 0) for s in statuses),
        "active_connections": sum(int(s.get("active_connections", 0) or 0) for s in statuses),
        "target_cycles": sum(int(s.get("target_cycles", 0) or 0) for s in statuses),
        "target_seconds": max(int(s.get("target_seconds", 0) or 0) for s in statuses) if statuses else 0,
        "avg_cycle_ms": round(avg_cycle_ms, 2),
    }


def _aggregate_sql_replay_metrics(statuses: list[dict]) -> dict:
    return {
        "executions": sum(int(s.get("executions", 0) or 0) for s in statuses),
        "selects": sum(int(s.get("selects", 0) or 0) for s in statuses),
        "dml": sum(int(s.get("dml", 0) or 0) for s in statuses),
        "plsql": sum(int(s.get("plsql", 0) or 0) for s in statuses),
        "commits": sum(int(s.get("commits", 0) or 0) for s in statuses),
        "rollbacks": sum(int(s.get("rollbacks", 0) or 0) for s in statuses),
        "errors": sum(int(s.get("errors", 0) or 0) for s in statuses),
    }


class _StatusProxy:
    def __init__(self, controller: "ShardedSubprocessController") -> None:
        self._controller = controller

    @property
    def running(self) -> bool:
        return bool(self.to_dict().get("running"))

    def to_dict(self) -> dict:
        return self._controller.status_dict()


class ShardedSubprocessController:
    """Run heavy benchmark jobs in child processes and aggregate live status."""

    def __init__(
        self,
        *,
        job_type: str,
        job_label: str,
        base_config: dict,
        requested_threads: int,
        total_cap: int,
        aggregate_metrics: Callable[[list[dict]], dict],
        progress_event: str,
        complete_event: str,
        progress_callback: Optional[Callable[[dict], None]] = None,
        complete_callback: Optional[Callable[[dict], None]] = None,
        error_callback: Optional[Callable[[str], None]] = None,
        connection_key: Optional[dict] = None,
    ) -> None:
        self._job_type = job_type
        self._job_label = job_label
        self._base_config = dict(base_config)
        self._requested_threads = max(1, int(requested_threads or 1))
        self._shards = shard_worker_counts(self._requested_threads, total_cap)
        self._worker_count = sum(self._shards)
        self._process_count = len(self._shards)
        self._aggregate_metrics = aggregate_metrics
        self._progress_event = progress_event
        self._complete_event = complete_event
        self._progress_callback = progress_callback
        self._complete_callback = complete_callback
        self._error_callback = error_callback
        self._connection_key = connection_key or {}

        self._lock = threading.Lock()
        self._procs: dict[int, subprocess.Popen] = {}
        self._monitor_threads: list[threading.Thread] = []
        self._spec_paths: dict[int, str] = {}
        self._shard_status: dict[int, dict] = {}
        self._completed_shards: set[int] = set()
        self._errors: list[str] = []
        self._done_event = threading.Event()
        self._stop_requested = False
        self._fatal_error = False
        self._forced_stop_count = 0
        self._start_monotonic = time.monotonic()
        self._initial_notice = self._build_initial_notice()
        self._status_data = {
            **self._aggregate_metrics([]),
            "elapsed": 0.0,
            "running": True,
            "phase": "PREPARING",
            "status_message": self._initial_notice,
            "last_error": "",
        }
        self.status = _StatusProxy(self)

    @property
    def is_running(self) -> bool:
        return bool(self.status.running)

    @property
    def done_event(self) -> threading.Event:
        return self._done_event

    def _build_initial_notice(self) -> str:
        message = (
            f"Using {self._process_count} child process(es) and "
            f"{self._worker_count} physical worker(s)."
        )
        if self._worker_count != self._requested_threads:
            message += (
                f" Requested {self._requested_threads}, capped at {self._worker_count}."
            )
        return message

    def _active_process_count(self) -> int:
        return sum(1 for proc in self._procs.values() if proc.poll() is None)

    def _safe_callback(self, callback: Optional[Callable], *args) -> None:
        if not callback:
            return
        try:
            callback(*args)
        except Exception:
            pass

    def _write_spec(self, payload: dict) -> str:
        handle = NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix=f"gcb-{self._job_type}-",
            delete=False,
        )
        try:
            json.dump(payload, handle)
            handle.flush()
            return handle.name
        finally:
            handle.close()

    def start(self) -> None:
        try:
            for shard_index, shard_workers in enumerate(self._shards):
                shard_config = dict(self._base_config)
                shard_config["thread_count"] = shard_workers
                spec_path = self._write_spec({
                    "shard_index": shard_index,
                    "config": shard_config,
                })
                proc = subprocess.Popen(
                    [sys.executable, str(BASE_DIR / "job_worker.py"), self._job_type, spec_path],
                    cwd=str(BASE_DIR),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                self._spec_paths[shard_index] = spec_path
                self._procs[shard_index] = proc
                monitor = threading.Thread(
                    target=self._monitor_process,
                    args=(shard_index, proc, spec_path),
                    name=f"{self._job_type}-monitor-{shard_index}",
                    daemon=True,
                )
                monitor.start()
                self._monitor_threads.append(monitor)
        except Exception:
            for proc in self._procs.values():
                if proc.poll() is None:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            for spec_path in self._spec_paths.values():
                try:
                    os.unlink(spec_path)
                except Exception:
                    pass
            raise

    def stop(
        self,
        grace_seconds: float = SUBPROCESS_STOP_GRACE_SECONDS,
        force_kill_seconds: float = SUBPROCESS_FORCE_KILL_SECONDS,
    ) -> dict:
        with self._lock:
            self._stop_requested = True
            active = bool(self._active_process_count())
            snapshot = self._build_status_snapshot(
                running_override=active,
                force_phase="STOPPING" if active else "STOPPED",
            )
            self._status_data = snapshot
        self._safe_callback(self._progress_callback, snapshot)

        for proc in self._procs.values():
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass

        deadline = time.monotonic() + max(0.0, grace_seconds)
        while time.monotonic() < deadline and self._active_process_count():
            time.sleep(0.2)

        alive = [proc for proc in self._procs.values() if proc.poll() is None]
        if alive:
            self._forced_stop_count += len(alive)
            for proc in alive:
                try:
                    proc.kill()
                except Exception:
                    pass
            deadline = time.monotonic() + max(0.0, force_kill_seconds)
            while time.monotonic() < deadline and self._active_process_count():
                time.sleep(0.2)

        self._done_event.wait(timeout=force_kill_seconds + 1.0)
        return self.status_dict()

    def status_dict(self) -> dict:
        with self._lock:
            return dict(self._status_data)

    def _monitor_process(self, shard_index: int, proc: subprocess.Popen, spec_path: str) -> None:
        try:
            stream = proc.stdout
            if stream is not None:
                for raw_line in stream:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        self._record_error(
                            f"{self._job_label} child {shard_index + 1} emitted non-JSON output: {line}"
                        )
                        continue
                    self._handle_event(shard_index, payload)

            return_code = proc.wait()
            if return_code != 0 and shard_index not in self._completed_shards:
                self._record_error(
                    f"{self._job_label} child {shard_index + 1} exited with code {return_code}."
                )
        finally:
            try:
                if proc.stdout is not None:
                    proc.stdout.close()
            except Exception:
                pass
            try:
                os.unlink(spec_path)
            except Exception:
                pass
            self._maybe_finalize()

    def _record_error(self, message: str) -> None:
        with self._lock:
            self._fatal_error = True
            self._stop_requested = True
            self._errors.append(message)
            active = bool(self._active_process_count())
            snapshot = self._build_status_snapshot(
                running_override=active,
                force_phase="ERROR" if not active else "STOPPING",
            )
            self._status_data = snapshot
        for proc in self._procs.values():
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        self._safe_callback(self._error_callback, message)
        self._safe_callback(self._progress_callback, snapshot)

    def _handle_event(self, shard_index: int, payload: dict) -> None:
        event = str(payload.get("event", "")).strip()
        if event == self._progress_event:
            data = payload.get("data", {})
            if isinstance(data, dict):
                with self._lock:
                    self._shard_status[shard_index] = dict(data)
                    snapshot = self._build_status_snapshot()
                    self._status_data = snapshot
                self._safe_callback(self._progress_callback, snapshot)
            return

        if event == self._complete_event:
            summary = payload.get("summary", {})
            if isinstance(summary, dict):
                with self._lock:
                    self._completed_shards.add(shard_index)
                    self._shard_status[shard_index] = dict(summary)
                    active = bool(self._active_process_count())
                    snapshot = self._build_status_snapshot(running_override=active)
                    self._status_data = snapshot
                self._safe_callback(self._progress_callback, snapshot)
            return

        if event == "error":
            summary = payload.get("summary", {})
            if isinstance(summary, dict):
                with self._lock:
                    self._shard_status[shard_index] = dict(summary)
            self._record_error(str(payload.get("message", "Child worker failed.")))
            return

    def _build_status_snapshot(
        self,
        *,
        running_override: Optional[bool] = None,
        force_phase: str = "",
    ) -> dict:
        statuses = [dict(value) for value in self._shard_status.values()]
        metrics = self._aggregate_metrics(statuses)
        running = bool(self._active_process_count()) if running_override is None else bool(running_override)
        elapsed = time.monotonic() - self._start_monotonic if self._start_monotonic else 0.0
        if statuses:
            elapsed = max(elapsed, max(float(s.get("elapsed", 0) or 0) for s in statuses))

        last_error = _last_nonempty(
            [str(s.get("last_error", "")).strip() for s in statuses] + self._errors
        )
        last_message = _last_nonempty(
            [str(s.get("status_message", "")).strip() for s in statuses]
        )

        phase = force_phase or self._derive_phase(statuses, running=running)
        status_message = self._derive_message(
            phase=phase,
            running=running,
            last_message=last_message,
            last_error=last_error,
        )

        return {
            **metrics,
            "elapsed": round(elapsed, 1),
            "running": running,
            "phase": phase,
            "status_message": status_message,
            "last_error": last_error,
        }

    def _derive_phase(self, statuses: list[dict], *, running: bool) -> str:
        phases = [str(s.get("phase", "")).strip().upper() for s in statuses]
        if self._fatal_error:
            return "ERROR"
        if self._stop_requested and running:
            return "STOPPING"
        if "PREPARING" in phases:
            return "PREPARING"
        if "WARMING" in phases:
            return "WARMING"
        if "RUNNING" in phases or running:
            return "RUNNING" if not self._stop_requested else "STOPPING"
        if self._stop_requested:
            return "STOPPED"
        if "STOPPED" in phases:
            return "STOPPED"
        if "ERROR" in phases:
            return "ERROR"
        return "COMPLETE"

    def _derive_message(
        self,
        *,
        phase: str,
        running: bool,
        last_message: str,
        last_error: str,
    ) -> str:
        if phase == "ERROR":
            return last_error or f"{self._job_label} failed."
        if phase == "STOPPING":
            return "Stopping child workers and waiting for Oracle calls to timeout..."
        if phase == "STOPPED":
            if self._forced_stop_count > 0:
                return f"{self._job_label} stopped after forcing {self._forced_stop_count} child process(es)."
            return f"{self._job_label} stopped."
        if phase == "COMPLETE":
            return f"{self._job_label} finished."
        if last_message:
            return last_message
        if running:
            return self._initial_notice
        return ""

    def _maybe_finalize(self) -> None:
        if self._done_event.is_set():
            return
        if self._active_process_count() > 0:
            return
        with self._lock:
            if self._done_event.is_set():
                return
            snapshot = self._build_status_snapshot(running_override=False)
            if snapshot["phase"] in ("PREPARING", "WARMING", "RUNNING", "STOPPING"):
                snapshot["phase"] = "ERROR" if self._fatal_error else ("STOPPED" if self._stop_requested else "COMPLETE")
                snapshot["status_message"] = self._derive_message(
                    phase=snapshot["phase"],
                    running=False,
                    last_message=snapshot.get("status_message", ""),
                    last_error=snapshot.get("last_error", ""),
                )
            snapshot["running"] = False
            self._status_data = snapshot
            self._done_event.set()
        self._safe_callback(self._progress_callback, snapshot)
        self._safe_callback(self._complete_callback, snapshot)


def _watch_workload_runner(
    workload_id: str,
    controller: Any,
    connection_key: Optional[dict],
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Send one final workload snapshot, then remove it from the active registry."""
    controller.done_event.wait()
    try:
        final_payload = _build_workload_status_payload(controller, workload_id)
        asyncio.run_coroutine_threadsafe(
            _broadcast(
                _workload_message(
                    {"type": "progress", "data": final_payload},
                    connection_key,
                    workload_id,
                )
            ),
            loop,
        )
    except Exception:
        pass
    finally:
        _remove_workload_runner(workload_id, controller)


class SchemaCreateController:
    """Run schema creation in a child process so table-build threads stay out of FastAPI."""

    def __init__(
        self,
        *,
        config: dict,
        connection_state: dict,
        progress_callback: Optional[Callable[[str], None]] = None,
        complete_callback: Optional[Callable[[str], None]] = None,
        error_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._config = dict(config)
        self._connection_state = build_connection_state(connection_state)
        self._progress_callback = progress_callback
        self._complete_callback = complete_callback
        self._error_callback = error_callback
        self._lock = threading.Lock()
        self._running = False
        self._done_event = threading.Event()
        self._proc: Optional[subprocess.Popen] = None
        self._spec_path = ""
        self._monitor_thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def start(self) -> None:
        handle = NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="gcb-schema-",
            delete=False,
        )
        try:
            json.dump(
                {
                    "config": self._config,
                    "connection_state": self._connection_state,
                },
                handle,
            )
            handle.flush()
            self._spec_path = handle.name
        finally:
            handle.close()

        try:
            self._proc = subprocess.Popen(
                [sys.executable, str(BASE_DIR / "job_worker.py"), "schema-create", self._spec_path],
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception:
            try:
                if self._spec_path:
                    os.unlink(self._spec_path)
            except Exception:
                pass
            raise
        with self._lock:
            self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_process,
            name="schema-create-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

    def _monitor_process(self) -> None:
        try:
            stream = self._proc.stdout if self._proc else None
            if stream is not None:
                for raw_line in stream:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        self._safe_callback(
                            self._error_callback,
                            f"Schema create child emitted non-JSON output: {line}",
                        )
                        continue
                    event = str(payload.get("event", "")).strip()
                    if event == "schema_progress":
                        self._safe_callback(self._progress_callback, str(payload.get("message", "")))
                    elif event == "schema_complete":
                        self._safe_callback(self._complete_callback, str(payload.get("message", "Schema creation finished.")))
                    elif event == "schema_error":
                        self._safe_callback(self._error_callback, str(payload.get("message", "Schema creation failed.")))
            if self._proc is not None:
                self._proc.wait()
        finally:
            with self._lock:
                self._running = False
            try:
                if self._proc and self._proc.stdout is not None:
                    self._proc.stdout.close()
            except Exception:
                pass
            try:
                if self._spec_path:
                    os.unlink(self._spec_path)
            except Exception:
                pass
            self._done_event.set()

    def _safe_callback(self, callback: Optional[Callable[[str], None]], arg: str) -> None:
        if not callback:
            return
        try:
            callback(arg)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    _load_config()
    await report.init_db(DB_PATH)
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="GC Benchmark Tool", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def root():
    """Serve the SPA."""
    return FileResponse(str(STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# Connection endpoints
# ---------------------------------------------------------------------------

@app.get("/api/connections/recent")
async def connections_recent():
    """Return the last N successful connections (no passwords)."""
    return {"ok": True, "connections": _load_recent_conns()}


@app.get("/api/cpool-connections/recent")
async def cpool_connections_recent():
    """Return the last N successful CDB pool-stat connections (no passwords)."""
    return {"ok": True, "connections": _load_recent_cpool_conns()}


@app.post("/api/connection/test")
async def connection_test(body: dict):
    """Test the Oracle connection."""
    _conn_state.update({
        "host": body.get("host", _conn_state["host"]),
        "port": int(body.get("port", _conn_state["port"])),
        "service_name": body.get("service_name", _conn_state["service_name"]),
        "user": body.get("user", _conn_state["user"]),
        "password": body.get("password", _conn_state["password"]),
        "mode": body.get("mode", _conn_state["mode"]),
    })
    try:
        conn = _get_connection()
        cur = conn.cursor()
        cur.execute("SELECT SYSDATE, SYS_CONTEXT('USERENV', 'INSTANCE_NAME') FROM dual")
        row = cur.fetchone()
        cur.close()
        conn.close()
        # Persist to recent-connections list (no password stored)
        _save_recent_conn(_conn_state)
        _save_config()
        return {
            "ok": True,
            "message": f"Connected successfully. Instance: {row[1]}, Server time: {row[0]}",
        }
    except oracledb.DatabaseError as exc:
        error_obj = exc.args[0] if exc.args else exc
        return {"ok": False, "message": str(error_obj)}


@app.post("/api/connection/privileges")
async def connection_privileges():
    """Check SELECT privileges on required V$ views."""
    try:
        conn = _get_connection()
        result = check_privileges(conn)
        conn.close()
        return {"ok": True, "privileges": result}
    except oracledb.DatabaseError as exc:
        return {"ok": False, "message": str(exc)}


@app.post("/api/connection/save")
async def connection_save(body: dict):
    """Save connection settings to config.ini (no password)."""
    _conn_state.update({
        "host": body.get("host", _conn_state["host"]),
        "port": int(body.get("port", _conn_state["port"])),
        "service_name": body.get("service_name", _conn_state["service_name"]),
        "user": body.get("user", _conn_state["user"]),
        "password": body.get("password", _conn_state["password"]),
        "mode": body.get("mode", _conn_state["mode"]),
    })
    _save_config()
    return {"ok": True, "message": "Connection settings saved."}


@app.get("/api/connection/status")
async def connection_status():
    """Return current (non-secret) connection state."""
    return {
        "host": _conn_state["host"],
        "port": _conn_state["port"],
        "service_name": _conn_state["service_name"],
        "user": _conn_state["user"],
        "mode": _conn_state["mode"],
        "has_password": bool(_conn_state.get("password")),
    }


# ---------------------------------------------------------------------------
# Schema endpoints
# ---------------------------------------------------------------------------

@app.get("/api/schema/preview")
async def schema_preview(
    table_count: int = 10,
    partition_type: str = "NONE",
    partition_count: int = 8,
    range_interval: str = "MONTHLY",
    compression: str = "NONE",
    table_prefix: str = "GCB",
    seed_rows: int = 500,
):
    """Return the DDL that would be executed (dry-run)."""
    cfg = SchemaConfig(
        table_prefix=table_prefix,
        table_count=max(1, table_count),
        partition_type=partition_type,
        partition_count=partition_count,
        range_interval=range_interval,
        compression=compression,
        seed_rows=max(0, seed_rows),
    )
    return {"ddl": preview_ddl(cfg)}


@app.post("/api/schema/create")
async def schema_create(body: dict):
    """Create the benchmark schema.  Streams progress via WebSocket."""
    global _schema_job, _schema_state
    if _schema_job and _schema_job.is_running:
        return {"ok": False, "message": "A schema creation job is already running."}

    cfg = _build_schema_config(body)
    loop = asyncio.get_event_loop()
    connection_state = build_connection_state(_conn_state)

    # Persist schema metadata so the workload run can record it accurately
    part_detail = ""
    if cfg.partition_type.upper() == "HASH":
        part_detail = f"{cfg.partition_count} partitions"
    elif cfg.partition_type.upper() == "RANGE":
        part_detail = cfg.range_interval
    elif cfg.partition_type.upper() == "LIST":
        part_detail = "status"

    _schema_state.update({
        "table_prefix":    cfg.table_prefix,
        "table_count":     cfg.table_count,
        "seed_rows":       cfg.seed_rows,
        "partition_type":  cfg.partition_type.upper(),
        "partition_detail": part_detail,
        "compression":     cfg.compression.upper(),
    })

    def on_progress(message: str):
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "schema_progress", "message": message}),
            loop,
        )

    def on_complete(message: str):
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "schema_complete", "message": message}),
            loop,
        )

    def on_error(message: str):
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "schema_progress", "message": message}),
            loop,
        )

    _schema_job = SchemaCreateController(
        config={
            "table_prefix": cfg.table_prefix,
            "table_count": cfg.table_count,
            "partition_type": cfg.partition_type,
            "partition_count": cfg.partition_count,
            "range_interval": cfg.range_interval,
            "compression": cfg.compression,
            "seed_rows": cfg.seed_rows,
        },
        connection_state=connection_state,
        progress_callback=on_progress,
        complete_callback=on_complete,
        error_callback=on_error,
    )
    try:
        _schema_job.start()
    except Exception as exc:
        _schema_job = None
        return {"ok": False, "message": f"Failed to start schema creation: {exc}"}

    return {
        "ok": True,
        "message": (
            f"Creating {cfg.table_count} tables with {cfg.seed_rows} rows per table "
            f"using a child process."
        ),
    }


@app.delete("/api/schema/drop")
async def schema_drop(body: dict = None):
    """Drop the benchmark schema."""
    body = body or {}
    cfg = _build_schema_config(body)
    loop = asyncio.get_event_loop()

    def _run():
        try:
            conn = _get_connection()
            for msg in drop_schema(cfg, conn):
                asyncio.run_coroutine_threadsafe(
                    _broadcast({"type": "schema_progress", "message": msg}),
                    loop,
                )
            conn.close()
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_complete", "message": "Schema drop finished."}),
                loop,
            )
        except oracledb.DatabaseError as exc:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_progress", "message": f"ERROR: {exc}"}),
                loop,
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"ok": True, "message": f"Dropping {cfg.table_count} tables..."}


@app.delete("/api/schema/drop-prefix")
async def schema_drop_prefix(body: dict = None):
    """Drop existing benchmark tables matching a prefix."""
    body = body or {}
    prefix = str(body.get("table_prefix", "")).strip().upper()
    if not prefix:
        return {"ok": False, "message": "table_prefix is required."}

    loop = asyncio.get_event_loop()

    def _run():
        try:
            conn = _get_connection()
            for msg in drop_tables_by_prefix(prefix, conn):
                asyncio.run_coroutine_threadsafe(
                    _broadcast({"type": "schema_progress", "message": msg}),
                    loop,
                )
            conn.close()
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_complete", "message": f"Prefix drop finished for {prefix}."}),
                loop,
            )
        except oracledb.DatabaseError as exc:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_progress", "message": f"ERROR: {exc}"}),
                loop,
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"ok": True, "message": f"Dropping existing tables with prefix {prefix}..."}


@app.post("/api/schema/kill-sessions")
async def schema_kill_sessions(body: dict = None):
    """Kill all sessions for the specified table-owner user."""
    body = body or {}
    username = str(body.get("username", "PP")).strip().upper()
    if not username:
        return {"ok": False, "message": "username is required."}

    loop = asyncio.get_event_loop()

    def _run():
        try:
            for msg in _kill_sessions_for_user_parallel(_get_connection, username):
                asyncio.run_coroutine_threadsafe(
                    _broadcast({"type": "schema_progress", "message": msg}),
                    loop,
                )
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_complete", "message": f"Session kill finished for {username}."}),
                loop,
            )
        except oracledb.DatabaseError as exc:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_progress", "message": f"ERROR: {exc}"}),
                loop,
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"ok": True, "message": f"Killing sessions for user {username}..."}


@app.get("/api/schema/state")
async def schema_state_get():
    """Return the last-created schema configuration for display on the workload tab."""
    return _schema_state


# ---------------------------------------------------------------------------
# Oracle GC stress parameter endpoints
# ---------------------------------------------------------------------------

# Parameters that amplify GC congestion and their safe default values.
_GC_STRESS_PARAMS: list[dict] = [
    {
        "param":   "_lm_lms",
        "stress":  "1",
        "default": None,          # None = reset with ALTER SYSTEM RESET
        "label":   "LMS processes",
        "why":     "Reduces Lock Manager Server processes to 1 — smallest possible "
                   "drain capacity. LMS queue fills fastest.",
    },
    {
        "param":   "_gc_defer_time",
        "stress":  "0",
        "default": None,
        "label":   "GC defer time",
        "why":     "Disables GC request deferral — every block request hits LMS "
                   "immediately with no buffering.",
    },
    {
        "param":   "_db_block_max_cr_dba",
        "stress":  "0",
        "default": None,
        "label":   "Max CR dba",
        "why":     "Forces current-block mode — stops CR optimisation so every read "
                   "on a dirty block generates a gc current block request.",
    },
]


def _query_gc_params(conn) -> dict[str, str]:
    """Return current values of the GC stress hidden parameters.

    Tries four progressively less-privileged sources in order:

    1. ``V$PARAMETER``        — works for any user with SELECT on V$ views
    2. ``V$SYSTEM_PARAMETER`` — alternative V$ view, same privilege level
    3. ``X$KSPPI / X$KSPPCV``— Oracle internals, requires SYS or SELECT ANY
                                DICTIONARY; silently skipped if access denied
    4. Literal ``"hidden"``   — fallback when no source returns the value;
                                the UI shows a neutral badge instead of an error

    Hidden underscore parameters are intentionally absent from ``V$PARAMETER``
    for non-privileged users, so sources 3/4 act as a graceful fallback.
    """
    result: dict[str, str] = {}
    names = [p["param"] for p in _GC_STRESS_PARAMS]
    placeholders = ", ".join(f":n{i}" for i in range(len(names)))
    binds = {f"n{i}": n for i, n in enumerate(names)}

    cursor = conn.cursor()
    try:
        # --- Source 1: V$PARAMETER (hidden params appear here for privileged users) ---
        try:
            cursor.execute(
                f"SELECT name, value FROM v$parameter WHERE name IN ({placeholders})",
                binds,
            )
            for row in cursor.fetchall():
                result[row[0]] = str(row[1]) if row[1] is not None else "(default)"
        except Exception:
            pass

        # --- Source 2: V$SYSTEM_PARAMETER (instance-level, sometimes more complete) ---
        missing = [n for n in names if n not in result]
        if missing:
            ph2 = ", ".join(f":n{i}" for i in range(len(missing)))
            b2  = {f"n{i}": n for i, n in enumerate(missing)}
            try:
                cursor.execute(
                    f"SELECT name, value FROM v$system_parameter WHERE name IN ({ph2})",
                    b2,
                )
                for row in cursor.fetchall():
                    result[row[0]] = str(row[1]) if row[1] is not None else "(default)"
            except Exception:
                pass

        # --- Source 3: X$KSPPI / X$KSPPCV (requires SYS-level access) ---
        missing = [n for n in names if n not in result]
        if missing:
            ph3 = ", ".join(f":n{i}" for i in range(len(missing)))
            b3  = {f"n{i}": n for i, n in enumerate(missing)}
            try:
                cursor.execute(
                    f"""SELECT x.ksppinm, y.ksppstvl
                        FROM   x$ksppi x JOIN x$ksppcv y ON x.indx = y.indx
                        WHERE  x.ksppinm IN ({ph3})""",
                    b3,
                )
                for row in cursor.fetchall():
                    result[row[0]] = str(row[1]) if row[1] is not None else "(default)"
            except Exception:
                # ORA-00942 or ORA-01031 — no access to X$ tables; silently continue
                pass

        # --- Source 4: fallback — mark remaining as hidden ---
        for n in names:
            if n not in result:
                result[n] = "hidden"

    finally:
        cursor.close()
    return result


@app.get("/api/oracle/gc_params")
async def oracle_gc_params_get():
    """Return current values of GC stress hidden parameters from Oracle."""
    try:
        conn = _get_connection()
        values = _query_gc_params(conn)
        conn.close()
        out = []
        for p in _GC_STRESS_PARAMS:
            out.append({
                "param":   p["param"],
                "label":   p["label"],
                "why":     p["why"],
                "stress":  p["stress"],
                "current": values.get(p["param"], "unknown"),
            })
        return {"ok": True, "params": out}
    except Exception as exc:
        return {"ok": False, "message": str(exc), "params": []}


@app.post("/api/oracle/apply_gc_stress")
async def oracle_apply_gc_stress():
    """Apply all GC stress hidden parameters with ALTER SYSTEM … SCOPE=MEMORY.

    Uses SCOPE=MEMORY so changes are **not** written to the spfile and are
    automatically reverted on the next instance restart.  Requires ALTER
    SYSTEM privilege.
    """
    applied: list[str] = []
    errors:  list[str] = []
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        for p in _GC_STRESS_PARAMS:
            sql = (
                f'ALTER SYSTEM SET "{p["param"]}" = {p["stress"]} SCOPE=MEMORY'
            )
            try:
                cursor.execute(sql)
                applied.append(p["param"])
            except Exception as exc:
                errors.append(f'{p["param"]}: {exc}')
        cursor.close()
        conn.close()
    except Exception as exc:
        return {"ok": False, "message": str(exc), "applied": [], "errors": []}

    return {
        "ok":      len(errors) == 0,
        "applied": applied,
        "errors":  errors,
        "message": (
            f"Applied {len(applied)} parameter(s). "
            + (f"Errors: {'; '.join(errors)}" if errors else "Restart DB to revert.")
        ),
    }


@app.post("/api/oracle/reset_gc_params")
async def oracle_reset_gc_params():
    """Reset GC stress parameters to Oracle defaults via ALTER SYSTEM RESET … SCOPE=MEMORY."""
    reset:  list[str] = []
    errors: list[str] = []
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        for p in _GC_STRESS_PARAMS:
            sql = f'ALTER SYSTEM RESET "{p["param"]}" SCOPE=MEMORY'
            try:
                cursor.execute(sql)
                reset.append(p["param"])
            except Exception as exc:
                errors.append(f'{p["param"]}: {exc}')
        cursor.close()
        conn.close()
    except Exception as exc:
        return {"ok": False, "message": str(exc), "reset": [], "errors": []}

    return {
        "ok":      len(errors) == 0,
        "reset":   reset,
        "errors":  errors,
        "message": (
            f"Reset {len(reset)} parameter(s) to defaults. "
            + (f"Errors: {'; '.join(errors)}" if errors else "")
        ),
    }


@app.get("/api/schema/list")
async def schema_list():
    """Discover existing benchmark schemas from Oracle data dictionary.

    Queries USER_TABLES and USER_PART_TABLES for tables that match the
    benchmark naming pattern ``<PREFIX>_ORDER_NN``, groups them by prefix,
    and returns partition / compression metadata for each schema set.
    """
    # Map Oracle COMPRESS_FOR values → our internal compression keys
    _COMPRESS_FOR_MAP = {
        "BASIC":        "BASIC",
        "ADVANCED":     "ADVANCED",
        "OLTP":         "ADVANCED",
        "QUERY LOW":    "HCC_QUERY_LOW",
        "QUERY HIGH":   "HCC_QUERY_HIGH",
        "ARCHIVE LOW":  "HCC_ARCHIVE_LOW",
        "ARCHIVE HIGH": "HCC_ARCHIVE_HIGH",
    }

    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT t.table_name,
                   NVL(t.compression,  'DISABLED') AS compression,
                   NVL(t.compress_for, 'NONE')     AS compress_for,
                   NVL(p.partitioning_type, 'NONE') AS partition_type,
                   NVL(p.partition_count, 0)         AS partition_count
            FROM   user_tables t
            LEFT JOIN user_part_tables p
                   ON p.table_name = t.table_name
            WHERE  REGEXP_LIKE(t.table_name, '^[A-Z0-9_]+_ORDER_[0-9]+$')
            ORDER  BY t.table_name
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # Group tables by prefix (everything before the last _ORDER_NN)
        from collections import defaultdict
        groups: dict[str, list[dict]] = defaultdict(list)
        for tname, compression, compress_for, part_type, part_count in rows:
            match = tname.rsplit("_ORDER_", 1)
            if len(match) == 2:
                prefix = match[0]
                groups[prefix].append({
                    "table_name":    tname,
                    "compression":   compression,
                    "compress_for":  compress_for,
                    "partition_type": part_type,
                    "partition_count": int(part_count or 0),
                })

        schemas = []
        for prefix, tables in sorted(groups.items()):
            first = tables[0]
            comp_key = "NONE"
            if first["compression"] == "ENABLED":
                raw = (first["compress_for"] or "").upper().strip()
                comp_key = _COMPRESS_FOR_MAP.get(raw, "BASIC")

            part_type   = first["partition_type"]
            part_count  = first["partition_count"]
            part_detail = ""
            if part_type == "HASH":
                part_detail = f"{part_count} partitions"
            elif part_type in ("RANGE", "INTERVAL"):
                part_detail = "interval"
            elif part_type == "LIST":
                part_detail = "status"

            # Human-readable label shown in the dropdown
            parts = [f"{prefix}"]
            parts.append(f"{len(tables)} tables")
            parts.append(f"Partition: {part_type}" + (f" ({part_detail})" if part_detail else ""))
            parts.append(f"Compress: {comp_key}")
            label = "  |  ".join(parts)

            schemas.append({
                "prefix":          prefix,
                "table_count":     len(tables),
                "partition_type":  part_type,
                "partition_detail": part_detail,
                "partition_count": part_count,
                "compression":     comp_key,
                "label":           label,
            })

        return {"ok": True, "schemas": schemas}

    except Exception as exc:
        return {"ok": False, "message": str(exc), "schemas": []}


# ---------------------------------------------------------------------------
# Workload endpoints
# ---------------------------------------------------------------------------

@app.post("/api/workload/start")
async def workload_start(body: dict):
    """Start the benchmark workload."""
    # Guard: password must be set in the current session (not persisted to disk)
    if not _conn_state.get("password"):
        return {
            "ok": False,
            "message": (
                "No password is set for this session. "
                "Go to the Connection tab, enter your password, and click 'Test Connection' first."
            ),
        }

    connection_state = build_connection_state(_conn_state)
    cfg = WorkloadConfig(
        dsn=build_dsn_from_state(connection_state),
        user=connection_state["user"],
        password=connection_state["password"],
        mode=connection_state.get("mode", "thin"),
        table_prefix=body.get("table_prefix", "GCB"),
        table_count=max(1, int(body.get("table_count", 10))),
        thread_count=max(2, min(10000, int(body.get("thread_count", 8)))),
        duration_seconds=max(10, int(body.get("duration_seconds", 60))),
        hot_row_pct=max(1, min(10, int(body.get("hot_row_pct", 5)))),
        seed_rows=max(
            1,
            min(
                MAX_WORKLOAD_SEED_ROWS,
                int(body.get("seed_rows", 500)),
            ),
        ),
        commit_batch=int(body.get("commit_batch", 10)),
        insert_pct=max(0, int(body.get("insert_pct", 40))),
        update_pct=max(0, int(body.get("update_pct", 40))),
        delete_pct=max(0, int(body.get("delete_pct", 20))),
        select_pct=max(0, int(body.get("select_pct", 0))),
        select_into_pct=max(0, int(body.get("select_into_pct", 0))),
        pedt_update_pct=max(0, int(body.get("pedt_update_pct", 0))),
        contention_mode=body.get("contention_mode", "NORMAL").upper(),
        lock_hold_ms=max(0, min(500, int(body.get("lock_hold_ms", 0)))),
    )

    loop = asyncio.get_running_loop()
    started_at = datetime.now(timezone.utc).isoformat()
    before_snapshot: dict = {}
    workload_connection_key = _connection_key_from_state(connection_state)
    workload_id = _new_workload_id()
    controller: Optional[ShardedSubprocessController] = None

    def current_status_payload(status_dict: Optional[dict] = None) -> dict:
        if controller is None:
            payload = dict(status_dict or {})
            payload["workload_id"] = workload_id
            payload["schema_name"] = str(body.get("schema_name") or cfg.table_prefix or "")
            return payload
        return _build_workload_status_payload(
            controller,
            workload_id,
            status_override=status_dict,
        )

    def on_progress(status_dict: dict):
        _persist_active_workload(
            workload_id,
            current_status_payload(status_dict),
            child_pids=[
                proc.pid for proc in controller._procs.values()
                if getattr(proc, "pid", 0)
            ] if controller is not None else None,
        )
        asyncio.run_coroutine_threadsafe(
            _broadcast(
                _workload_message(
                    {
                        "type": "progress",
                        "data": current_status_payload(status_dict),
                    },
                    workload_connection_key,
                    workload_id,
                )
            ),
            loop,
        )

    def on_error(message: str):
        asyncio.run_coroutine_threadsafe(
            _broadcast(
                _workload_message(
                    {
                        "type": "error",
                        "source": "workload",
                        "message": message,
                    },
                    workload_connection_key,
                    workload_id,
                )
            ),
            loop,
        )

    def on_complete(final_status: dict):
        _remove_persisted_active_workload(workload_id)
        after_snapshot: dict = {}
        try:
            conn = connect_from_state(connection_state)
            after_snapshot = snapshot_system_events(conn)
            conn.close()
        except Exception:
            pass

        delta = compute_delta(before_snapshot, after_snapshot)
        delta_agg = compute_aggregated_delta(before_snapshot, after_snapshot)
        finished_at = datetime.now(timezone.utc).isoformat()

        async def _save():
            run_id = await report.save_run(
                DB_PATH,
                started_at=started_at,
                finished_at=finished_at,
                duration_secs=cfg.duration_seconds,
                schema_name=str(body.get("schema_name") or cfg.table_prefix or ""),
                table_prefix=cfg.table_prefix,
                table_count=cfg.table_count,
                partition_type=body.get("partition_type") or _schema_state.get("partition_type", "NONE"),
                partition_detail=body.get("partition_detail") or _schema_state.get("partition_detail", ""),
                compression=body.get("compression") or _schema_state.get("compression", "NONE"),
                thread_count=cfg.thread_count,
                hot_row_pct=cfg.hot_row_pct,
                inserts=final_status["inserts"],
                updates=final_status["updates"],
                deletes=final_status["deletes"],
                errors=final_status["errors"],
                gc_delta=delta,
                gc_delta_aggregated=delta_agg,
                before_snapshot=before_snapshot,
                after_snapshot=after_snapshot,
            )
            await _broadcast(
                _workload_message(
                    {
                        "type": "complete",
                        "run_id": run_id,
                        "status": current_status_payload(final_status),
                        "summary": {
                            **final_status,
                            "gc_delta": delta_agg,
                        },
                    },
                    workload_connection_key,
                    workload_id,
                )
            )

        asyncio.run_coroutine_threadsafe(_save(), loop)

    # Prime the live chart immediately so short runs do not appear blank
    # before the first non-zero GC sample arrives.
    await _broadcast(
        _workload_message(
            {
                "type": "gc_snapshot",
                "data": {
                    "elapsed": 0,
                    "events": {ev: 0 for ev in GC_SYSTEM_EVENTS},
                },
            },
            workload_connection_key,
            workload_id,
        )
    )

    try:
        conn = connect_from_state(connection_state)
        before_snapshot.update(snapshot_system_events(conn))
        conn.close()
    except Exception as exc:
        asyncio.run_coroutine_threadsafe(
            _broadcast(
                _workload_message(
                    {
                        "type": "warning",
                        "source": "workload",
                        "message": f"Could not capture before snapshot: {exc}",
                    },
                    workload_connection_key,
                    workload_id,
                )
            ),
            loop,
        )

    controller = ShardedSubprocessController(
        job_type="workload",
        job_label="Workload",
        base_config={
            "dsn": cfg.dsn,
            "user": cfg.user,
            "password": cfg.password,
            "mode": cfg.mode,
            "table_prefix": cfg.table_prefix,
            "table_count": cfg.table_count,
            "thread_count": cfg.thread_count,
            "duration_seconds": cfg.duration_seconds,
            "hot_row_pct": cfg.hot_row_pct,
            "seed_rows": cfg.seed_rows,
            "commit_batch": cfg.commit_batch,
            "insert_pct": cfg.insert_pct,
            "update_pct": cfg.update_pct,
            "delete_pct": cfg.delete_pct,
            "select_pct": cfg.select_pct,
            "select_into_pct": cfg.select_into_pct,
            "pedt_update_pct": cfg.pedt_update_pct,
            "contention_mode": cfg.contention_mode,
            "lock_hold_ms": cfg.lock_hold_ms,
            "call_timeout_ms": cfg.call_timeout_ms,
        },
        requested_threads=cfg.thread_count,
        total_cap=MAX_TOTAL_GC_WORKERS,
        aggregate_metrics=_aggregate_workload_metrics,
        progress_event="progress",
        complete_event="complete",
        progress_callback=on_progress,
        complete_callback=on_complete,
        error_callback=on_error,
        connection_key=workload_connection_key,
    )
    controller._connection_key = workload_connection_key
    controller._config = cfg
    controller._connection_state = connection_state
    controller._schema_name = str(body.get("schema_name") or cfg.table_prefix or "")
    controller._created_at = started_at
    _register_workload_runner(workload_id, controller)

    try:
        controller.start()
    except Exception as exc:
        _remove_workload_runner(workload_id, controller)
        _remove_persisted_active_workload(workload_id)
        return {"ok": False, "message": f"Failed to start workload: {exc}"}

    _persist_active_workload(
        workload_id,
        current_status_payload(),
        child_pids=[
            proc.pid for proc in controller._procs.values()
            if getattr(proc, "pid", 0)
        ],
    )

    lifecycle_thread = threading.Thread(
        target=_watch_workload_runner,
        args=(workload_id, controller, workload_connection_key, loop),
        name=f"workload-watch-{workload_id}",
        daemon=True,
    )
    lifecycle_thread.start()

    def sample_gc_live():
        last_gc_sample = 0.0
        while not controller.done_event.wait(timeout=0.5):
            status_dict = controller.status.to_dict()
            if not status_dict.get("running") or status_dict.get("phase") != "RUNNING":
                continue
            now = time.monotonic()
            if now - last_gc_sample < 2.0:
                continue
            last_gc_sample = now
            try:
                conn = connect_from_state(connection_state)
                snap = snapshot_system_events_aggregated(conn)
                conn.close()
                gc_data = {"elapsed": status_dict.get("elapsed", 0), "events": {}}
                for ev in GC_SYSTEM_EVENTS:
                    before_waits = 0
                    before_time_waited = 0
                    for key, value in before_snapshot.items():
                        if key.endswith(f":{ev}"):
                            before_waits += value.get("total_waits", 0)
                            before_time_waited += value.get("time_waited_micro", 0)
                    current = snap.get(ev, {})
                    delta_waits = current.get("total_waits", 0) - before_waits
                    delta_time_waited = current.get("time_waited_micro", 0) - before_time_waited
                    if delta_waits > 0 and delta_time_waited >= 0:
                        gc_data["events"][ev] = (delta_time_waited / delta_waits) / 1000.0
                    else:
                        gc_data["events"][ev] = 0
                asyncio.run_coroutine_threadsafe(
                    _broadcast(
                        _workload_message(
                            {"type": "gc_snapshot", "data": gc_data},
                            workload_connection_key,
                            workload_id,
                        )
                    ),
                    loop,
                )
            except Exception:
                pass

    sampler_thread = threading.Thread(
        target=sample_gc_live,
        name=f"gc-live-sampler-{workload_id}",
        daemon=True,
    )
    sampler_thread.start()

    return {
        "ok": True,
        "workload_id": workload_id,
        "status": current_status_payload(),
        "message": (
            f"Workload started using {controller._worker_count} physical workers "
            f"across {controller._process_count} child processes. "
            f"Workload ID: {workload_id}."
        ),
    }


@app.post("/api/workload/stop")
async def workload_stop(body: dict):
    """Stop one active workload."""
    workload_id = str(body.get("workload_id", "")).strip()
    if not workload_id:
        return {"ok": False, "message": "workload_id is required."}

    controller = _get_workload_runner(workload_id)
    if not controller:
        recovered_record = _get_persisted_active_workload_record(workload_id)
        if not recovered_record:
            return {"ok": False, "message": f"Workload {workload_id} was not found."}
        result = _stop_recovered_workload(recovered_record)
        return {
            "ok": True,
            "status": result,
        }

    result = controller.stop()
    return {
        "ok": True,
        "status": _build_workload_status_payload(
            controller,
            workload_id,
            status_override=result,
        ),
    }


@app.get("/api/workload/status")
async def workload_status(
    host: str = "",
    port: int = 0,
    service_name: str = "",
    user: str = "",
):
    """Return active workload status for the requested connection."""
    requested_connection = _connection_key(
        host=host,
        port=port,
        service_name=service_name,
        user=user,
    )

    workloads: list[dict] = []
    other_labels: set[str] = set()
    other_workload_count = 0
    seen_workload_ids: set[str] = set()

    for workload_id, controller in _list_workload_runners():
        payload = _build_workload_status_payload(controller, workload_id)
        if not _is_active_workload_payload(payload):
            continue
        seen_workload_ids.add(str(workload_id))

        runner_connection = getattr(controller, "_connection_key", None)
        if _has_connection_key(requested_connection) and _has_connection_key(runner_connection):
            if not _same_connection(requested_connection, runner_connection):
                other_workload_count += 1
                other_labels.add(_format_connection_key(runner_connection))
                continue

        workloads.append(payload)

    for payload in _list_recovered_active_workloads():
        workload_id = str(payload.get("workload_id", "") or "")
        if workload_id in seen_workload_ids:
            continue

        runner_connection = payload.get("connection_key")
        if _has_connection_key(requested_connection) and _has_connection_key(runner_connection):
            if not _same_connection(requested_connection, runner_connection):
                other_workload_count += 1
                other_labels.add(_format_connection_key(runner_connection))
                continue

        workloads.append(payload)

    workloads.sort(
        key=lambda item: (item.get("created_at", ""), item.get("workload_id", "")),
        reverse=True,
    )

    return {
        "running": bool(workloads),
        "count": len(workloads),
        "workloads": workloads,
        "other_workload_running": bool(other_workload_count),
        "other_connection_workload_count": other_workload_count,
        "other_connection_labels": sorted(other_labels),
    }


@app.post("/api/login-workload/start")
async def login_workload_start(body: dict):
    """Start the repeated login/query/logout simulation."""
    global _login_runner
    if _login_runner and _login_runner.is_running:
        message = "A login workload simulation is already running."
        active_connection = getattr(_login_runner, "_connection_key", None)
        if _has_connection_key(active_connection):
            message += f" Active connection: {_format_connection_key(active_connection)}."
        return {"ok": False, "message": message}

    if not _conn_state.get("password"):
        return {
            "ok": False,
            "message": (
                "No password is set for this session. "
                "Go to the Connection tab, enter your password, and click 'Test Connection' first."
            ),
        }

    connection_state = build_connection_state(_conn_state)
    try:
        sql_text = _normalize_login_sql(body.get("sql_text", "select 1 from dual"))
        session_case = _normalize_login_session_case(body.get("session_case", "SIMPLE_QUERY"))
        module_name = _normalize_login_module_name(body.get("module_name", "DBSTRESS_LOGIN_SESSION_00000000"))
        if session_case == "MFES_ONLINE":
            proc_status = _get_login_procedure_status(connection_state)
            if not proc_status.get("valid"):
                raise ValueError(
                    f"Procedure {_MFES_PROCEDURE_NAME} is not ready. "
                    "Create it from the Login Simulation tab before starting MFES Online tests."
                )
        _validate_login_workload(
            sql_text,
            session_case=session_case,
            module_name=module_name,
            state=connection_state,
        )
    except Exception as exc:
        return {"ok": False, "message": f"Preflight query failed: {exc}"}

    stop_mode = str(body.get("stop_mode", "CYCLES") or "CYCLES").upper()
    if stop_mode not in {"CYCLES", "DURATION", "MANUAL"}:
        stop_mode = "CYCLES"

    cfg = LoginWorkloadConfig(
        dsn=build_dsn_from_state(connection_state),
        user=connection_state["user"],
        password=connection_state["password"],
        mode=connection_state.get("mode", "thin"),
        sql_text=sql_text,
        thread_count=max(1, min(MAX_TOTAL_LOGIN_WORKERS, int(body.get("thread_count", 20)))),
        stop_mode=stop_mode,
        iterations_per_thread=max(0, int(body.get("iterations_per_thread", 1000))),
        duration_seconds=max(0, int(body.get("duration_seconds", 0))),
        think_time_ms=max(0, min(60000, int(body.get("think_time_ms", 0)))),
        session_case=session_case,
        module_name=module_name,
    )

    loop = asyncio.get_event_loop()
    login_connection_key = _connection_key_from_state(connection_state)
    started_at = datetime.now(timezone.utc).isoformat()
    before_snapshot: dict = {}

    def on_progress(status_dict: dict):
        asyncio.run_coroutine_threadsafe(
            _broadcast(
                _workload_message(
                    {"type": "login_progress", "data": status_dict},
                    login_connection_key,
                )
            ),
            loop,
        )

    def on_error(message: str):
        asyncio.run_coroutine_threadsafe(
            _broadcast(
                _workload_message(
                    {
                        "type": "error",
                        "source": "login_workload",
                        "message": message,
                    },
                    login_connection_key,
                )
            ),
            loop,
        )

    def on_complete(final_status: dict):
        after_snapshot: dict = {}
        try:
            conn = connect_from_state(connection_state)
            after_snapshot = snapshot_system_events(conn)
            conn.close()
        except Exception:
            pass

        delta = compute_delta(before_snapshot, after_snapshot)
        delta_agg = compute_aggregated_delta(before_snapshot, after_snapshot)
        finished_at = datetime.now(timezone.utc).isoformat()

        login_notes = json.dumps({
            "run_type": "LOGIN_SIM",
            "session_case": cfg.session_case,
            "module_name": cfg.module_name,
            "sql_text": cfg.sql_text,
            "stop_mode": cfg.stop_mode,
            "think_time_ms": cfg.think_time_ms,
            "logons": int(final_status.get("logons", 0) or 0),
            "queries": int(final_status.get("queries", 0) or 0),
            "logouts": int(final_status.get("logouts", 0) or 0),
            "cycles": int(final_status.get("cycles", 0) or 0),
            "avg_cycle_ms": float(final_status.get("avg_cycle_ms", 0) or 0),
        })

        async def _save():
            run_id = await report.save_run(
                DB_PATH,
                started_at=started_at,
                finished_at=finished_at,
                duration_secs=float(final_status.get("elapsed", 0) or 0),
                schema_name="Login Simulation",
                table_prefix="LOGIN",
                table_count=0,
                partition_type=cfg.session_case,
                partition_detail=cfg.stop_mode,
                compression="N/A",
                thread_count=cfg.thread_count,
                hot_row_pct=0,
                inserts=0,
                updates=0,
                deletes=0,
                errors=int(final_status.get("errors", 0) or 0),
                gc_delta=delta,
                gc_delta_aggregated=delta_agg,
                before_snapshot=before_snapshot,
                after_snapshot=after_snapshot,
                notes=login_notes,
            )
            await _broadcast(
                _workload_message(
                    {
                        "type": "login_complete",
                        "run_id": run_id,
                        "summary": {
                            **final_status,
                            "gc_delta": delta_agg,
                            "session_case": cfg.session_case,
                            "module_name": cfg.module_name,
                        },
                    },
                    login_connection_key,
                )
            )

        asyncio.run_coroutine_threadsafe(_save(), loop)

    _login_runner = ShardedSubprocessController(
        job_type="login",
        job_label="Login workload",
        base_config={
            "dsn": cfg.dsn,
            "user": cfg.user,
            "password": cfg.password,
            "mode": cfg.mode,
            "sql_text": cfg.sql_text,
            "thread_count": cfg.thread_count,
            "stop_mode": cfg.stop_mode,
            "iterations_per_thread": cfg.iterations_per_thread,
            "duration_seconds": cfg.duration_seconds,
            "think_time_ms": cfg.think_time_ms,
            "call_timeout_ms": cfg.call_timeout_ms,
        },
        requested_threads=cfg.thread_count,
        total_cap=MAX_TOTAL_LOGIN_WORKERS,
        aggregate_metrics=_aggregate_login_metrics,
        progress_event="login_progress",
        complete_event="login_complete",
        progress_callback=on_progress,
        complete_callback=on_complete,
        error_callback=on_error,
        connection_key=login_connection_key,
    )
    _login_runner._connection_key = login_connection_key
    _login_runner._config = cfg
    _login_runner._connection_state = connection_state
    controller = _login_runner

    try:
        conn = connect_from_state(connection_state)
        before_snapshot.update(snapshot_system_events(conn))
        conn.close()
    except Exception:
        before_snapshot = {}

    try:
        controller.start()
    except Exception as exc:
        _login_runner = None
        return {"ok": False, "message": f"Failed to start login workload simulation: {exc}"}

    return {
        "ok": True,
        "message": (
            f"Login workload simulation started in {cfg.session_case} mode using "
            f"{controller._worker_count} physical workers across {controller._process_count} child processes."
        ),
    }


@app.post("/api/login-workload/stop")
async def login_workload_stop():
    """Stop the running login workload simulation."""
    global _login_runner
    if not _login_runner or not _login_runner.status.running:
        return {"ok": False, "message": "No login workload simulation is running."}
    result = _login_runner.stop()
    return {"ok": True, "status": result}


@app.get("/api/login-workload/procedure/status")
async def login_workload_procedure_status():
    """Return the status of the optional MFES login procedure."""
    try:
        status = _get_login_procedure_status()
        return status
    except Exception as exc:
        return {
            "ok": False,
            "exists": False,
            "valid": False,
            "name": _MFES_PROCEDURE_NAME,
            "message": f"Failed to check procedure status: {exc}",
        }


@app.post("/api/login-workload/procedure/create")
async def login_workload_procedure_create():
    """Create or replace the MFES login procedure in the current schema."""
    if not _conn_state.get("password"):
        return {
            "ok": False,
            "message": (
                "No password is set for this session. "
                "Go to the Connection tab, enter your password, and click 'Test Connection' first."
            ),
        }
    try:
        status = _create_login_procedure()
        status["ok"] = bool(status.get("valid"))
        status["message"] = (
            f"Procedure {_MFES_PROCEDURE_NAME} created successfully."
            if status.get("valid")
            else status.get("message", f"Procedure {_MFES_PROCEDURE_NAME} created.")
        )
        return status
    except Exception as exc:
        return {
            "ok": False,
            "exists": False,
            "valid": False,
            "name": _MFES_PROCEDURE_NAME,
            "message": f"Failed to create procedure: {exc}",
        }


@app.post("/api/login-workload/procedure/drop")
async def login_workload_procedure_drop():
    """Drop the MFES login procedure from the current schema."""
    if not _conn_state.get("password"):
        return {
            "ok": False,
            "message": (
                "No password is set for this session. "
                "Go to the Connection tab, enter your password, and click 'Test Connection' first."
            ),
        }
    try:
        status = _drop_login_procedure()
        status["ok"] = True
        return status
    except Exception as exc:
        return {
            "ok": False,
            "exists": True,
            "valid": False,
            "name": _MFES_PROCEDURE_NAME,
            "message": f"Failed to drop procedure: {exc}",
        }


@app.get("/api/login-workload/status")
async def login_workload_status(
    host: str = "",
    port: int = 0,
    service_name: str = "",
    user: str = "",
):
    """Return current login workload simulation status."""
    if not _login_runner:
        return {"running": False}

    requested_connection = _connection_key(
        host=host,
        port=port,
        service_name=service_name,
        user=user,
    )
    runner_connection = getattr(_login_runner, "_connection_key", None)
    if _has_connection_key(requested_connection) and _has_connection_key(runner_connection):
        if not _same_connection(requested_connection, runner_connection):
            return {
                "running": False,
                "other_login_workload_running": bool(_login_runner.status.running),
                "other_connection_key": _connection_key(
                    host=runner_connection.get("host", ""),
                    port=runner_connection.get("port", 0),
                    service_name=runner_connection.get("service_name", ""),
                    user=runner_connection.get("user", ""),
                ),
                "other_connection_label": _format_connection_key(runner_connection),
            }

    payload = _login_runner.status.to_dict()
    if _has_connection_key(runner_connection):
        payload["connection_key"] = _connection_key(
            host=runner_connection.get("host", ""),
            port=runner_connection.get("port", 0),
            service_name=runner_connection.get("service_name", ""),
            user=runner_connection.get("user", ""),
        )
        payload["connection_label"] = _format_connection_key(runner_connection)

    cfg = getattr(_login_runner, "_config", None)
    if cfg:
        payload.update({
            "thread_count": getattr(cfg, "thread_count", 0),
            "requested_threads": getattr(cfg, "thread_count", 0),
            "physical_workers": getattr(_login_runner, "_worker_count", 0),
            "process_count": getattr(_login_runner, "_process_count", 0),
            "stop_mode": getattr(cfg, "stop_mode", "CYCLES"),
            "iterations_per_thread": getattr(cfg, "iterations_per_thread", 0),
            "duration_seconds": getattr(cfg, "duration_seconds", 0),
            "think_time_ms": getattr(cfg, "think_time_ms", 0),
            "sql_text": getattr(cfg, "sql_text", ""),
            "session_case": getattr(cfg, "session_case", "SIMPLE_QUERY"),
            "module_name": getattr(cfg, "module_name", ""),
            "execution_model": "dedicated child-process workers",
        })
    try:
        payload["procedure_status"] = _get_login_procedure_status(
            getattr(_login_runner, "_connection_state", None) or _conn_state
        )
    except Exception:
        payload["procedure_status"] = {
            "ok": False,
            "exists": False,
            "valid": False,
            "name": _MFES_PROCEDURE_NAME,
        }
    return payload


@app.post("/api/peak-replay/analyze")
async def peak_replay_analyze(body: dict):
    """Analyze an AWR/ASH interval and return a replayable SQL mix."""
    try:
        dbid = int(body.get("dbid", 0) or 0)
    except Exception:
        dbid = 0
    if dbid <= 0:
        return {"ok": False, "message": "dbid is required."}

    try:
        start_time = _parse_api_datetime(body.get("start_time", ""))
        end_time = _parse_api_datetime(body.get("end_time", ""))
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}

    if end_time < start_time:
        return {"ok": False, "message": "End time must be after start time."}

    top_n = max(1, min(25, int(body.get("top_n", 8) or 8)))

    try:
        conn = _get_connection()
        analysis = _fetch_peak_replay_analysis(
            conn,
            dbid=dbid,
            start_time=start_time,
            end_time=end_time,
            top_n=top_n,
        )
        conn.close()
        return {"ok": True, "analysis": analysis}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


@app.post("/api/peak-replay/start")
async def peak_replay_start(body: dict):
    """Start replaying the selected SQL mix on the current connection."""
    global _sql_replay_runner

    with _sql_replay_runner_lock:
        if _sql_replay_runner and _sql_replay_runner.is_running:
            message = "A SQL peak replay is already running."
            active_connection = getattr(_sql_replay_runner, "_connection_key", None)
            if _has_connection_key(active_connection):
                message += f" Active connection: {_format_connection_key(active_connection)}."
            return {"ok": False, "message": message}

    if not _conn_state.get("password"):
        return {
            "ok": False,
            "message": (
                "No password is set for this session. "
                "Connect to the target database first from the Connection tab."
            ),
        }

    selected_sql = [item for item in (body.get("statements") or []) if item.get("replayable", True)]
    if not selected_sql:
        return {"ok": False, "message": "Select at least one replayable SQL statement first."}

    connection_state = build_connection_state(_conn_state)
    cfg = SqlReplayConfig(
        dsn=build_dsn_from_state(connection_state),
        user=connection_state["user"],
        password=connection_state["password"],
        mode=connection_state.get("mode", "thin"),
        statements=selected_sql,
        thread_count=max(1, min(MAX_TOTAL_LOGIN_WORKERS, int(body.get("thread_count", 8) or 8))),
        duration_seconds=max(10, min(86400, int(body.get("duration_seconds", 60) or 60))),
        think_time_ms=max(0, min(60000, int(body.get("think_time_ms", 0) or 0))),
        module=str(body.get("module", "GC_PEAK_REPLAY") or "GC_PEAK_REPLAY"),
        action_prefix=str(body.get("action_prefix", "peak") or "peak"),
        commit_every=max(1, min(1000, int(body.get("commit_every", 1) or 1))),
    )

    replay_connection_key = _connection_key_from_state(connection_state)
    controller = ShardedSubprocessController(
        job_type="sql-replay",
        job_label="SQL replay",
        base_config={
            "dsn": cfg.dsn,
            "user": cfg.user,
            "password": cfg.password,
            "mode": cfg.mode,
            "statements": cfg.statements,
            "thread_count": cfg.thread_count,
            "duration_seconds": cfg.duration_seconds,
            "think_time_ms": cfg.think_time_ms,
            "module": cfg.module,
            "action_prefix": cfg.action_prefix,
            "commit_every": cfg.commit_every,
            "call_timeout_ms": cfg.call_timeout_ms,
        },
        requested_threads=cfg.thread_count,
        total_cap=MAX_TOTAL_LOGIN_WORKERS,
        aggregate_metrics=_aggregate_sql_replay_metrics,
        progress_event="replay_progress",
        complete_event="replay_complete",
        connection_key=replay_connection_key,
    )
    controller._connection_key = replay_connection_key
    controller._analysis_summary = str(body.get("analysis_summary", "") or "")
    controller._created_at = datetime.now(timezone.utc).isoformat()
    controller._selected_sql_count = len(selected_sql)
    controller._config = cfg

    try:
        controller.start()
    except Exception as exc:
        return {"ok": False, "message": f"Failed to start SQL replay: {exc}"}

    with _sql_replay_runner_lock:
        _sql_replay_runner = controller

    def _watch() -> None:
        global _sql_replay_runner
        controller.done_event.wait()
        with _sql_replay_runner_lock:
            if _sql_replay_runner is controller:
                _sql_replay_runner = None

    threading.Thread(target=_watch, name="sql-replay-watch", daemon=True).start()

    return {
        "ok": True,
        "message": (
            f"SQL replay started with {controller._worker_count} physical workers "
            f"across {controller._process_count} child processes."
        ),
    }


@app.post("/api/peak-replay/stop")
async def peak_replay_stop():
    """Stop the running SQL peak replay."""
    global _sql_replay_runner
    with _sql_replay_runner_lock:
        controller = _sql_replay_runner
    if not controller:
        return {"ok": False, "message": "No SQL peak replay is running."}

    result = controller.stop()
    with _sql_replay_runner_lock:
        if _sql_replay_runner is controller:
            _sql_replay_runner = None
    return {"ok": True, "status": result}


@app.get("/api/peak-replay/status")
async def peak_replay_status(
    host: str = "",
    port: int = 0,
    service_name: str = "",
    user: str = "",
):
    """Return current SQL peak replay status."""
    with _sql_replay_runner_lock:
        controller = _sql_replay_runner
    if not controller:
        return {"running": False, "other_replay_running": False}

    requested_connection = _connection_key(
        host=host,
        port=port,
        service_name=service_name,
        user=user,
    )
    runner_connection = getattr(controller, "_connection_key", None)
    if _has_connection_key(requested_connection) and _has_connection_key(runner_connection):
        if not _same_connection(requested_connection, runner_connection):
            return {
                "running": False,
                "other_replay_running": bool(controller.status.running),
                "other_connection_label": _format_connection_key(runner_connection),
            }

    payload = controller.status.to_dict()
    payload["analysis_summary"] = getattr(controller, "_analysis_summary", "")
    payload["selected_sql_count"] = int(getattr(controller, "_selected_sql_count", 0) or 0)
    payload["connection_label"] = _format_connection_key(runner_connection) if _has_connection_key(runner_connection) else ""
    cfg = getattr(controller, "_config", None)
    if cfg:
        payload.update({
            "thread_count": getattr(cfg, "thread_count", 0),
            "duration_seconds": getattr(cfg, "duration_seconds", 0),
            "think_time_ms": getattr(cfg, "think_time_ms", 0),
            "module": getattr(cfg, "module", ""),
            "physical_workers": getattr(controller, "_worker_count", 0),
            "process_count": getattr(controller, "_process_count", 0),
        })
    return payload


@app.get("/api/db/activity")
async def db_activity(username: str = ""):
    """Return current GV$ process, session, and transaction counts."""
    try:
        conn = _get_connection()
        processes = _query_instance_counts(conn, "gv$process", "Processes")
        sessions = _query_session_counts(conn, username)
        transactions = _query_instance_counts(conn, "gv$transaction", "Transactions")
        conn.close()
        return {
            "ok": True,
            "processes": processes,
            "sessions": sessions,
            "transactions": transactions,
            "sampled_at": datetime.now(timezone.utc).isoformat(),
        }
    except oracledb.DatabaseError as exc:
        return {"ok": False, "message": str(exc)}


@app.get("/api/db/cpool-stats")
async def db_cpool_stats():
    """Return current GV$CPOOL_STATS rows."""
    try:
        if not (_cpool_conn_state.get("host") and _cpool_conn_state.get("service_name") and _cpool_conn_state.get("user") and _cpool_conn_state.get("password")):
            return {"ok": False, "message": "Set the CDB connection above Connection Pool Stats first."}
        conn = _get_cpool_connection()
        rows = _query_connection_pool_stats(conn)
        conn.close()
        return {
            "ok": True,
            "rows": rows,
            "sampled_at": datetime.now(timezone.utc).isoformat(),
        }
    except oracledb.DatabaseError as exc:
        return {"ok": False, "message": str(exc)}


@app.post("/api/db/cpool-connection/test")
async def db_cpool_connection_test(body: dict):
    """Test and store the dedicated CDB connection for GV$CPOOL_STATS."""
    _cpool_conn_state.update({
        "host": body.get("host", _cpool_conn_state["host"]),
        "port": int(body.get("port", _cpool_conn_state["port"] or 1521)),
        "service_name": body.get("service_name", _cpool_conn_state["service_name"]),
        "user": body.get("user", _cpool_conn_state["user"]),
        "password": body.get("password", _cpool_conn_state["password"]),
        "mode": body.get("mode", _cpool_conn_state["mode"]),
    })
    try:
        conn = _get_cpool_connection()
        cur = conn.cursor()
        cur.execute("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM dual")
        row = cur.fetchone()
        cur.close()
        conn.close()
        _save_recent_cpool_conn(_cpool_conn_state)
        return {
            "ok": True,
            "message": f"CDB pool-stats connection ready. Container: {row[0]}",
        }
    except oracledb.DatabaseError as exc:
        error_obj = exc.args[0] if exc.args else exc
        return {"ok": False, "message": str(error_obj)}


@app.post("/api/db/pdb-restart")
async def db_pdb_restart(body: dict):
    """Restart a PDB using the configured CDB connection."""
    pdb_name = str(body.get("pdb_name", "")).strip()
    try:
        steps = _restart_pdb_with_cdb_connection(pdb_name)
        return {"ok": True, "message": f"PDB {pdb_name.upper()} restarted.", "steps": steps}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


# ---------------------------------------------------------------------------
# Results endpoints
# ---------------------------------------------------------------------------

@app.get("/api/results")
async def results_list():
    """List all benchmark runs."""
    runs = await report.list_runs(DB_PATH)
    return {"runs": runs}


@app.get("/api/results/export/csv")
async def results_export_csv():
    """Export all runs as CSV."""
    csv_text = await report.export_csv(DB_PATH)
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gc_benchmark_results.csv"},
    )


@app.get("/api/results/compare")
async def results_compare(ids: str = Query(...)):
    """Compare selected runs.  ids is a comma-separated list."""
    run_ids = [int(x.strip()) for x in ids.split(",") if x.strip().isdigit()]
    if not run_ids:
        return {"error": "No valid run IDs provided."}
    data = await report.compare_runs(DB_PATH, run_ids)
    return data


@app.get("/api/results/{run_id}")
async def results_detail(run_id: int):
    """Return details for a single run."""
    run = await report.get_run(DB_PATH, run_id)
    if not run:
        return {"error": "Run not found."}
    return run


@app.delete("/api/results/{run_id}")
async def results_delete(run_id: int):
    """Delete a benchmark run."""
    deleted = await report.delete_run(DB_PATH, run_id)
    return {"ok": deleted}


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket for live progress streaming."""
    await ws.accept()
    async with _ws_lock:
        _ws_clients.append(ws)
    try:
        while True:
            # Keep connection alive; handle incoming messages if needed
            data = await ws.receive_text()
            # Client can send commands here if needed in the future
    except WebSocketDisconnect:
        pass
    finally:
        async with _ws_lock:
            if ws in _ws_clients:
                _ws_clients.remove(ws)
