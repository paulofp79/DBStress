"""GC wait-event snapshot collection from Oracle V$ views.

Provides before / after snapshot capture and delta computation
for Global Cache system events and per-segment statistics.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Target GC events
# ---------------------------------------------------------------------------

GC_SYSTEM_EVENTS: list[str] = [
    "gc current block congested",
    "gc current block 2-way",
    "gc current block 3-way",
    "gc current block busy",
    "gc cr block congested",
    "gc cr block 2-way",
    "gc cr block 3-way",
    "gc cr block busy",
    "gc cr grant congested",
    "gc cr grant 2-way",
]

# The three primary events the UI highlights
PRIMARY_GC_EVENTS: list[str] = [
    "gc current block congested",
    "gc current block 3-way",
    "gc cr grant congested",
]

GC_SEGMENT_STATS: list[str] = [
    "gc buffer busy acquire",
    "gc buffer busy release",
    "gc cr blocks received",
    "gc current blocks received",
]


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

def snapshot_system_events(connection) -> dict[str, dict[str, int]]:
    """Query GV$SYSTEM_EVENT for the tracked GC events.

    Returns a dict keyed by ``"inst_id:event_name"`` with values
    ``{"total_waits": int, "time_waited_micro": int}``.
    """
    placeholders = ", ".join(f":e{i}" for i in range(len(GC_SYSTEM_EVENTS)))
    sql = (
        f"SELECT inst_id, event, total_waits, time_waited_micro "
        f"FROM gv$system_event "
        f"WHERE event IN ({placeholders}) "
        f"ORDER BY inst_id, event"
    )
    binds = {f"e{i}": ev for i, ev in enumerate(GC_SYSTEM_EVENTS)}

    result: dict[str, dict[str, int]] = {}
    cursor = connection.cursor()
    try:
        cursor.execute(sql, binds)
        for inst_id, event, total_waits, time_waited_micro in cursor:
            key = f"{inst_id}:{event}"
            result[key] = {
                "total_waits": int(total_waits or 0),
                "time_waited_micro": int(time_waited_micro or 0),
            }
    finally:
        cursor.close()

    return result


def snapshot_system_events_aggregated(connection) -> dict[str, dict[str, int]]:
    """Aggregated (all instances) snapshot from GV$SYSTEM_EVENT.

    Returns a dict keyed by event name with summed waits / time.
    """
    placeholders = ", ".join(f":e{i}" for i in range(len(GC_SYSTEM_EVENTS)))
    sql = (
        f"SELECT event, SUM(total_waits), SUM(time_waited_micro) "
        f"FROM gv$system_event "
        f"WHERE event IN ({placeholders}) "
        f"GROUP BY event "
        f"ORDER BY event"
    )
    binds = {f"e{i}": ev for i, ev in enumerate(GC_SYSTEM_EVENTS)}

    result: dict[str, dict[str, int]] = {}
    cursor = connection.cursor()
    try:
        cursor.execute(sql, binds)
        for event, total_waits, time_waited_micro in cursor:
            result[event] = {
                "total_waits": int(total_waits or 0),
                "time_waited_micro": int(time_waited_micro or 0),
            }
    finally:
        cursor.close()

    return result


def snapshot_segment_stats(
    connection,
    table_names: list[str],
    owner: str,
) -> dict[str, dict[str, int]]:
    """Query V$SEGMENT_STATISTICS for GC-related per-segment stats.

    Returns a dict keyed by ``"object_name:statistic_name"``
    with integer values.
    """
    if not table_names:
        return {}

    tbl_placeholders = ", ".join(f":t{i}" for i in range(len(table_names)))
    stat_placeholders = ", ".join(f":s{i}" for i in range(len(GC_SEGMENT_STATS)))

    sql = (
        f"SELECT object_name, statistic_name, value "
        f"FROM v$segment_statistics "
        f"WHERE owner = :owner "
        f"AND object_name IN ({tbl_placeholders}) "
        f"AND statistic_name IN ({stat_placeholders})"
    )
    binds: dict[str, Any] = {"owner": owner.upper()}
    for i, t in enumerate(table_names):
        binds[f"t{i}"] = t.upper()
    for i, s in enumerate(GC_SEGMENT_STATS):
        binds[f"s{i}"] = s

    result: dict[str, dict[str, int]] = {}
    cursor = connection.cursor()
    try:
        cursor.execute(sql, binds)
        for obj_name, stat_name, value in cursor:
            key = f"{obj_name}:{stat_name}"
            result[key] = {"value": int(value or 0)}
    finally:
        cursor.close()

    return result


def compute_delta(
    before: dict[str, dict[str, int]],
    after: dict[str, dict[str, int]],
) -> dict[str, dict[str, int]]:
    """Compute the difference between two snapshot dicts.

    For each key in *after*, subtracts the matching *before* values
    (defaulting to 0 for missing keys).  Returns a new dict with
    the same structure.
    """
    delta: dict[str, dict[str, int]] = {}
    all_keys = set(after.keys()) | set(before.keys())

    for key in all_keys:
        a = after.get(key, {})
        b = before.get(key, {})
        d: dict[str, int] = {}
        for field in set(list(a.keys()) + list(b.keys())):
            d[field] = a.get(field, 0) - b.get(field, 0)
        delta[key] = d

    return delta


def compute_aggregated_delta(
    before: dict[str, dict[str, int]],
    after: dict[str, dict[str, int]],
) -> dict[str, int]:
    """Compute per-event total_waits delta (aggregated across instances).

    Expects dicts keyed by ``"inst_id:event"`` as returned by
    ``snapshot_system_events()``.  Returns ``{event: delta_waits}``.
    """
    before_agg: dict[str, int] = {}
    after_agg: dict[str, int] = {}

    for key, vals in before.items():
        event = key.split(":", 1)[1] if ":" in key else key
        before_agg[event] = before_agg.get(event, 0) + vals.get("total_waits", 0)

    for key, vals in after.items():
        event = key.split(":", 1)[1] if ":" in key else key
        after_agg[event] = after_agg.get(event, 0) + vals.get("total_waits", 0)

    result: dict[str, int] = {}
    for event in set(list(before_agg.keys()) + list(after_agg.keys())):
        result[event] = after_agg.get(event, 0) - before_agg.get(event, 0)

    return result


# ---------------------------------------------------------------------------
# Privilege checks
# ---------------------------------------------------------------------------

def check_privileges(connection) -> dict[str, bool]:
    """Test SELECT access to the required V$ / GV$ views.

    Returns a dict of view name -> accessible (True/False).
    """
    views = [
        "v$system_event",
        "gv$system_event",
        "v$segment_statistics",
    ]
    results: dict[str, bool] = {}
    cursor = connection.cursor()
    try:
        for view in views:
            try:
                cursor.execute(f"SELECT 1 FROM {view} WHERE ROWNUM = 1")
                cursor.fetchone()
                results[view] = True
            except Exception:
                results[view] = False
    finally:
        cursor.close()

    return results
