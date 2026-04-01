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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import oracledb
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles

from schema import (
    SchemaConfig,
    preview_ddl,
    create_schema,
    create_schema_parallel,
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
from workload import WorkloadConfig, WorkloadRunner
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

_runner: Optional[WorkloadRunner] = None
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
    return f"{_conn_state['host']}:{_conn_state['port']}/{_conn_state['service_name']}"


def _get_connection():
    """Create a new Oracle connection from the current state."""
    mode = _conn_state.get("mode", "thin")
    if mode == "thick":
        try:
            if oracledb.is_thin_mode():
                oracledb.init_oracle_client()
        except Exception:
            pass

    return oracledb.connect(
        user=_conn_state["user"],
        password=_conn_state["password"],
        dsn=_dsn(),
    )


def _cpool_dsn() -> str:
    """Build a connect-string for the CDB pool-stats connection."""
    return (
        f"{_cpool_conn_state['host']}:"
        f"{_cpool_conn_state['port']}/"
        f"{_cpool_conn_state['service_name']}"
    )


def _get_cpool_connection():
    """Create a new Oracle connection for GV$CPOOL_STATS queries."""
    mode = _cpool_conn_state.get("mode", "thin")
    if mode == "thick":
        try:
            if oracledb.is_thin_mode():
                oracledb.init_oracle_client()
        except Exception:
            pass

    return oracledb.connect(
        user=_cpool_conn_state["user"],
        password=_cpool_conn_state["password"],
        dsn=_cpool_dsn(),
    )


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
    global _schema_state
    cfg = _build_schema_config(body)
    loop = asyncio.get_event_loop()

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

    def _run():
        try:
            for msg in create_schema_parallel(cfg, _get_connection):
                asyncio.run_coroutine_threadsafe(
                    _broadcast({"type": "schema_progress", "message": msg}),
                    loop,
                )
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_complete", "message": "Schema creation finished."}),
                loop,
            )
        except oracledb.DatabaseError as exc:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "schema_progress", "message": f"ERROR: {exc}"}),
                loop,
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {
        "ok": True,
        "message": f"Creating {cfg.table_count} tables with {cfg.seed_rows} rows per table...",
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
    global _runner
    if _runner and _runner.is_running:
        return {"ok": False, "message": "A workload is already running."}

    # Guard: password must be set in the current session (not persisted to disk)
    if not _conn_state.get("password"):
        return {
            "ok": False,
            "message": (
                "No password is set for this session. "
                "Go to the Connection tab, enter your password, and click 'Test Connection' first."
            ),
        }

    cfg = WorkloadConfig(
        dsn=_dsn(),
        user=_conn_state["user"],
        password=_conn_state["password"],
        mode=_conn_state.get("mode", "thin"),
        table_prefix=body.get("table_prefix", "GCB"),
        table_count=max(1, int(body.get("table_count", 10))),
        thread_count=max(2, min(10000, int(body.get("thread_count", 8)))),
        duration_seconds=max(10, int(body.get("duration_seconds", 60))),
        hot_row_pct=max(1, min(10, int(body.get("hot_row_pct", 5)))),
        seed_rows=int(body.get("seed_rows", 500)),
        commit_batch=int(body.get("commit_batch", 10)),
        insert_pct=max(0, int(body.get("insert_pct", 40))),
        update_pct=max(0, int(body.get("update_pct", 40))),
        delete_pct=max(0, int(body.get("delete_pct", 20))),
        select_pct=max(0, int(body.get("select_pct", 0))),
        contention_mode=body.get("contention_mode", "NORMAL").upper(),
        lock_hold_ms=max(0, min(500, int(body.get("lock_hold_ms", 0)))),
    )

    loop = asyncio.get_event_loop()
    started_at = datetime.now(timezone.utc).isoformat()

    # Take before snapshot
    before_snapshot: dict = {}
    try:
        conn = _get_connection()
        before_snapshot = snapshot_system_events(conn)
        conn.close()
    except Exception as exc:
        await _broadcast({"type": "warning", "source": "workload", "message": f"Could not capture before snapshot: {exc}"})

    # Progress callback — bridges threads → async
    gc_sample_interval = 2.0
    last_gc_sample = [0.0]

    def on_progress(status_dict: dict):
        asyncio.run_coroutine_threadsafe(
            _broadcast({"type": "progress", "data": status_dict}),
            loop,
        )
        # Periodic live GC snapshot for the real-time chart
        now = time.monotonic()
        if now - last_gc_sample[0] >= gc_sample_interval:
            last_gc_sample[0] = now
            try:
                c = _get_connection()
                snap = snapshot_system_events_aggregated(c)
                c.close()
                gc_data = {"elapsed": status_dict.get("elapsed", 0), "events": {}}
                # Send average wait ms since run start for ALL GC events.
                for ev in GC_SYSTEM_EVENTS:
                    before_waits = 0
                    before_time_waited = 0
                    for k, v in before_snapshot.items():
                        if k.endswith(f":{ev}"):
                            before_waits += v.get("total_waits", 0)
                            before_time_waited += v.get("time_waited_micro", 0)
                    current = snap.get(ev, {})
                    delta_waits = current.get("total_waits", 0) - before_waits
                    delta_time_waited = current.get("time_waited_micro", 0) - before_time_waited
                    if delta_waits > 0 and delta_time_waited >= 0:
                        gc_data["events"][ev] = (delta_time_waited / delta_waits) / 1000.0
                    else:
                        gc_data["events"][ev] = 0
                asyncio.run_coroutine_threadsafe(
                    _broadcast({"type": "gc_snapshot", "data": gc_data}),
                    loop,
                )
            except Exception:
                pass

    # Prime the live chart immediately so short runs do not appear blank
    # before the first non-zero GC sample arrives.
    await _broadcast({
        "type": "gc_snapshot",
        "data": {
            "elapsed": 0,
            "events": {ev: 0 for ev in GC_SYSTEM_EVENTS},
        },
    })

    # Start workload in background threads
    _runner = WorkloadRunner()

    def _run_workload():
        try:
            _runner.start(cfg, progress_callback=on_progress)
            # Wait for completion
            while _runner.is_running:
                time.sleep(0.5)
        except Exception as exc:
            asyncio.run_coroutine_threadsafe(
                _broadcast({"type": "error", "source": "workload", "message": f"Workload error: {exc}"}),
                loop,
            )
            return

        final_status = _runner.stop(timeout=1.0)

        # After snapshot
        after_snapshot: dict = {}
        try:
            c = _get_connection()
            after_snapshot = snapshot_system_events(c)
            c.close()
        except Exception:
            pass

        # Compute deltas
        delta = compute_delta(before_snapshot, after_snapshot)
        delta_agg = compute_aggregated_delta(before_snapshot, after_snapshot)
        finished_at = datetime.now(timezone.utc).isoformat()

        # Save to SQLite
        async def _save():
            run_id = await report.save_run(
                DB_PATH,
                started_at=started_at,
                finished_at=finished_at,
                duration_secs=cfg.duration_seconds,
                table_prefix=cfg.table_prefix,
                table_count=cfg.table_count,
                # Prefer values sent by the frontend (from selected schema dropdown)
                # and fall back to the last schema created in this session.
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
            await _broadcast({
                "type": "complete",
                "run_id": run_id,
                "summary": {
                    **final_status,
                    "gc_delta": delta_agg,
                },
            })

        asyncio.run_coroutine_threadsafe(_save(), loop)

    thread = threading.Thread(target=_run_workload, daemon=True)
    thread.start()

    return {"ok": True, "message": "Workload started."}


@app.post("/api/workload/stop")
async def workload_stop():
    """Stop the running workload."""
    global _runner
    if not _runner or not _runner.is_running:
        return {"ok": False, "message": "No workload is running."}
    result = _runner.stop()
    return {"ok": True, "status": result}


@app.get("/api/workload/status")
async def workload_status():
    """Return current workload status."""
    if not _runner:
        return {"running": False}
    payload = _runner.status.to_dict()
    cfg = getattr(_runner, "_config", None)
    if cfg:
        payload.update({
            "table_prefix": getattr(cfg, "table_prefix", "GCB"),
            "table_count": getattr(cfg, "table_count", 0),
            "thread_count": getattr(cfg, "thread_count", 0),
            "requested_threads": getattr(cfg, "thread_count", 0),
            "physical_workers": getattr(_runner, "_worker_count", 0),
            "duration": getattr(cfg, "duration_seconds", payload.get("duration", 0)),
            "contention_mode": getattr(cfg, "contention_mode", "NORMAL"),
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
