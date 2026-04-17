"""Shared Oracle connection helpers with defensive timeout defaults."""

from __future__ import annotations

from typing import Any, Dict

import oracledb

DEFAULT_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_CALL_TIMEOUT_MS = 15000
DEFAULT_SCHEMA_CALL_TIMEOUT_MS = 1800000
MAX_WORKERS_PER_PROCESS = 128
MAX_SCHEMA_PARALLEL_WORKERS = 32
MAX_TOTAL_GC_WORKERS = 512
MAX_TOTAL_LOGIN_WORKERS = 2048
SUBPROCESS_STOP_GRACE_SECONDS = 5.0
SUBPROCESS_FORCE_KILL_SECONDS = 3.0


def init_mode_if_needed(mode: str) -> None:
    """Initialise thick mode only when requested and not already active."""
    if str(mode or "thin").lower() != "thick":
        return
    try:
        if oracledb.is_thin_mode():
            oracledb.init_oracle_client()
    except Exception:
        pass


def with_timeout_dsn(
    dsn: str,
    connect_timeout_seconds: int = DEFAULT_CONNECT_TIMEOUT_SECONDS,
) -> str:
    """Append Easy Connect timeout parameters when they are not already present."""
    raw = str(dsn or "").strip()
    if not raw:
        return raw

    lower = raw.lower()
    params = []
    if "transport_connect_timeout=" not in lower:
        params.append(f"transport_connect_timeout={max(1, int(connect_timeout_seconds))}")
    if "retry_count=" not in lower:
        params.append("retry_count=0")
    if "expire_time=" not in lower:
        params.append("expire_time=1")
    if not params:
        return raw

    separator = "&" if "?" in raw else "?"
    return raw + separator + "&".join(params)


def build_connection_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Return a detached connection-state payload safe to pass to child workers."""
    return {
        "host": str(state.get("host", "")).strip(),
        "port": int(state.get("port", 1521) or 1521),
        "service_name": str(state.get("service_name", "")).strip(),
        "user": str(state.get("user", "")).strip(),
        "password": str(state.get("password", "")),
        "mode": str(state.get("mode", "thin") or "thin"),
    }


def build_dsn_from_state(
    state: Dict[str, Any],
    connect_timeout_seconds: int = DEFAULT_CONNECT_TIMEOUT_SECONDS,
) -> str:
    """Build a timeout-protected Easy Connect string from a state dict."""
    raw = (
        f"{state.get('host', '')}:{int(state.get('port', 1521) or 1521)}/"
        f"{state.get('service_name', '')}"
    )
    return with_timeout_dsn(raw, connect_timeout_seconds=connect_timeout_seconds)


def apply_call_timeout(conn, call_timeout_ms: int = DEFAULT_CALL_TIMEOUT_MS) -> None:
    """Set Oracle call timeout when supported by the active client."""
    try:
        conn.call_timeout = max(1000, int(call_timeout_ms))
    except Exception:
        pass


def connect_from_state(
    state: Dict[str, Any],
    connect_timeout_seconds: int = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    call_timeout_ms: int = DEFAULT_CALL_TIMEOUT_MS,
):
    """Open a new Oracle connection from a saved state with defensive timeouts."""
    mode = str(state.get("mode", "thin") or "thin")
    init_mode_if_needed(mode)
    conn = oracledb.connect(
        user=state.get("user", ""),
        password=state.get("password", ""),
        dsn=build_dsn_from_state(
            state,
            connect_timeout_seconds=connect_timeout_seconds,
        ),
    )
    apply_call_timeout(conn, call_timeout_ms=call_timeout_ms)
    return conn


def shard_worker_counts(
    requested_workers: int,
    total_cap: int,
    per_process_cap: int = MAX_WORKERS_PER_PROCESS,
) -> list[int]:
    """Split a requested worker count into safe per-process shard sizes."""
    requested = max(1, int(requested_workers or 0))
    actual_total = max(1, min(requested, int(total_cap)))
    shards: list[int] = []
    remaining = actual_total
    while remaining > 0:
        shard_size = min(int(per_process_cap), remaining)
        shards.append(shard_size)
        remaining -= shard_size
    return shards
