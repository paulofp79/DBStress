"""SQLite-backed results storage and comparison logic.

All functions are async and use aiosqlite so they integrate cleanly
with the FastAPI async request lifecycle.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite

# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT    NOT NULL,
    finished_at     TEXT,
    duration_secs   REAL,
    schema_name     TEXT,
    table_prefix    TEXT,
    table_count     INTEGER,
    partition_type  TEXT,
    partition_detail TEXT,
    compression     TEXT,
    thread_count    INTEGER,
    hot_row_pct     INTEGER,
    inserts         INTEGER DEFAULT 0,
    updates         INTEGER DEFAULT 0,
    deletes         INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    gc_metrics      TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS gc_snapshots (
    snapshot_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            INTEGER NOT NULL REFERENCES benchmark_runs(run_id) ON DELETE CASCADE,
    phase             TEXT    NOT NULL,
    source            TEXT    NOT NULL DEFAULT 'system_event',
    event_key         TEXT    NOT NULL,
    total_waits       INTEGER DEFAULT 0,
    time_waited_micro INTEGER DEFAULT 0
);
"""


def _parse_json_field(value: Any) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def init_db(db_path: str) -> None:
    """Create tables if they do not already exist."""
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(_INIT_SQL)
        cursor = await db.execute("PRAGMA table_info(benchmark_runs)")
        cols = await cursor.fetchall()
        col_names = {row[1] for row in cols}
        if "schema_name" not in col_names:
            await db.execute("ALTER TABLE benchmark_runs ADD COLUMN schema_name TEXT")
        if "table_prefix" not in col_names:
            await db.execute("ALTER TABLE benchmark_runs ADD COLUMN table_prefix TEXT")
        if "notes" not in col_names:
            await db.execute("ALTER TABLE benchmark_runs ADD COLUMN notes TEXT")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.commit()


# ---------------------------------------------------------------------------
# Run persistence
# ---------------------------------------------------------------------------

async def save_run(
    db_path: str,
    *,
    started_at: str,
    finished_at: str,
    duration_secs: float,
    schema_name: str,
    table_prefix: str,
    table_count: int,
    partition_type: str,
    partition_detail: str,
    compression: str,
    thread_count: int,
    hot_row_pct: int,
    inserts: int,
    updates: int,
    deletes: int,
    errors: int,
    gc_delta: dict[str, dict[str, int]],
    gc_delta_aggregated: dict[str, int],
    before_snapshot: dict[str, dict[str, int]],
    after_snapshot: dict[str, dict[str, int]],
    notes: str = "",
) -> int:
    """Persist a completed run and its GC snapshots.  Returns the run_id."""
    gc_metrics_json = json.dumps({
        "delta": gc_delta,
        "delta_aggregated": gc_delta_aggregated,
    })

    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            """
            INSERT INTO benchmark_runs
                (started_at, finished_at, duration_secs, schema_name, table_prefix, table_count,
                 partition_type, partition_detail, compression,
                 thread_count, hot_row_pct, inserts, updates, deletes,
                 errors, gc_metrics, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                started_at, finished_at, duration_secs, schema_name, table_prefix, table_count,
                partition_type, partition_detail, compression,
                thread_count, hot_row_pct, inserts, updates, deletes,
                errors, gc_metrics_json, notes,
            ),
        )
        run_id = cursor.lastrowid

        # Store before snapshots
        for key, vals in before_snapshot.items():
            await db.execute(
                """
                INSERT INTO gc_snapshots
                    (run_id, phase, source, event_key, total_waits, time_waited_micro)
                VALUES (?, 'before', 'system_event', ?, ?, ?)
                """,
                (run_id, key, vals.get("total_waits", 0), vals.get("time_waited_micro", 0)),
            )

        # Store after snapshots
        for key, vals in after_snapshot.items():
            await db.execute(
                """
                INSERT INTO gc_snapshots
                    (run_id, phase, source, event_key, total_waits, time_waited_micro)
                VALUES (?, 'after', 'system_event', ?, ?, ?)
                """,
                (run_id, key, vals.get("total_waits", 0), vals.get("time_waited_micro", 0)),
            )

        await db.commit()
        return run_id


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

async def list_runs(db_path: str) -> list[dict]:
    """Return all benchmark runs, newest first."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM benchmark_runs ORDER BY started_at DESC"
        )
        rows = await cursor.fetchall()
        results = []
        for row in rows:
            d = dict(row)
            d["gc_metrics_parsed"] = _parse_json_field(d.get("gc_metrics"))
            d["notes_parsed"] = _parse_json_field(d.get("notes"))
            results.append(d)
        return results


async def get_run(db_path: str, run_id: int) -> Optional[dict]:
    """Return a single run with its parsed GC metrics."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM benchmark_runs WHERE run_id = ?",
            (run_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        d = dict(row)
        d["gc_metrics_parsed"] = _parse_json_field(d.get("gc_metrics"))
        d["notes_parsed"] = _parse_json_field(d.get("notes"))
        return d


async def delete_run(db_path: str, run_id: int) -> bool:
    """Delete a run and its snapshots.  Returns True if a row was deleted."""
    async with aiosqlite.connect(db_path) as db:
        # Delete snapshots first (in case FK cascade is not supported)
        await db.execute("DELETE FROM gc_snapshots WHERE run_id = ?", (run_id,))
        cursor = await db.execute(
            "DELETE FROM benchmark_runs WHERE run_id = ?", (run_id,),
        )
        await db.commit()
        return cursor.rowcount > 0


async def compare_runs(db_path: str, run_ids: list[int]) -> dict:
    """Build a Chart.js-ready comparison payload focused on gc current block congested.

    Each run becomes one bar, labelled with its schema configuration
    (partition / compression / thread count) so differences between
    executions are immediately visible.

    Returns:
        labels      – one entry per run (config description)
        values      – gc current block congested wait count per run
        details     – full per-run breakdown for tooltips / table display
        datasets    – Chart.js-compatible single-dataset structure
        target_event – the metric being compared (constant)
    """
    TARGET_EVENT = "gc current block congested"

    labels: list[str] = []
    values: list[int] = []
    details: list[dict] = []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        for rid in run_ids:
            cursor = await db.execute(
                "SELECT * FROM benchmark_runs WHERE run_id = ?", (rid,),
            )
            row = await cursor.fetchone()
            if not row:
                continue

            r = dict(row)
            gc_parsed = _parse_json_field(r.get("gc_metrics"))
            notes = _parse_json_field(r.get("notes"))

            delta_agg = gc_parsed.get("delta_aggregated", {})
            threads = r.get("thread_count") or "?"
            dur = int(r.get("duration_secs") or 0)
            run_type = str(notes.get("run_type") or "GC_WORKLOAD")
            if run_type == "LOGIN_SIM":
                schema = "Login Simulation"
                scenario = str(notes.get("session_case") or "SIMPLE_QUERY")
                layout = str(notes.get("stop_mode") or "N/A")
            else:
                schema = r.get("schema_name") or "?"
                scenario = r.get("partition_type") or "NONE"
                layout = r.get("compression") or "NONE"

            # Short label shown on the chart axis
            label = f"#{rid} | {schema} | {scenario} / {layout} / {threads}t / {dur}s"
            labels.append(label)

            congested = delta_agg.get(TARGET_EVENT, 0)
            values.append(congested)

            details.append({
                "run_id": rid,
                "run_type": run_type,
                "schema_name": schema,
                "scenario": scenario,
                "layout": layout,
                "threads": threads,
                "duration": dur,
                "gc_congested": congested,
                "gc_3way": delta_agg.get("gc current block 3-way", 0),
                "gc_cr": delta_agg.get("gc cr grant congested", 0),
            })

    return {
        "labels":       labels,
        "values":       values,
        "details":      details,
        "target_event": TARGET_EVENT,
        # Chart.js datasets – single dataset, one bar per run
        "datasets": [{
            "label": TARGET_EVENT,
            "data":  values,
        }],
    }


async def export_csv(db_path: str) -> str:
    """Generate a CSV string of all runs."""
    runs = await list_runs(db_path)

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    header = [
        "run_id", "started_at", "finished_at", "duration_secs", "schema_name",
        "run_type", "scenario", "table_count", "partition_type", "partition_detail", "compression",
        "thread_count", "hot_row_pct", "inserts", "updates", "deletes", "errors",
        "logons", "queries", "logouts", "cycles", "avg_cycle_ms",
        "gc_curr_congested", "gc_curr_3way", "gc_cr_congested",
    ]
    writer.writerow(header)

    for r in runs:
        delta_agg = {}
        parsed = r.get("gc_metrics_parsed", {})
        if parsed:
            delta_agg = parsed.get("delta_aggregated", {})
        notes = r.get("notes_parsed", {})
        run_type = notes.get("run_type", "GC_WORKLOAD")
        scenario = (
            notes.get("session_case", "")
            if run_type == "LOGIN_SIM"
            else (r.get("partition_type") or "")
        )

        writer.writerow([
            r.get("run_id"),
            r.get("started_at"),
            r.get("finished_at"),
            r.get("duration_secs"),
            r.get("schema_name"),
            run_type,
            scenario,
            r.get("table_count"),
            r.get("partition_type"),
            r.get("partition_detail"),
            r.get("compression"),
            r.get("thread_count"),
            r.get("hot_row_pct"),
            r.get("inserts"),
            r.get("updates"),
            r.get("deletes"),
            r.get("errors"),
            notes.get("logons", 0),
            notes.get("queries", 0),
            notes.get("logouts", 0),
            notes.get("cycles", 0),
            notes.get("avg_cycle_ms", 0),
            delta_agg.get("gc current block congested", 0),
            delta_agg.get("gc current block 3-way", 0),
            delta_agg.get("gc cr grant congested", 0),
        ])

    return output.getvalue()
