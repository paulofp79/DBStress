"""Helpers for building a replayable lab from historical SQL analysis."""

from __future__ import annotations

import random
import re
import string
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any


def normalize_sql_text(sql_text: str) -> tuple[str, bool, str]:
    sql = str(sql_text or "").strip().rstrip(";")
    if not sql:
        return "", False, "empty"

    upper = sql.upper()
    if upper.startswith("SET TRANSACTION"):
        return "", False, "SET TRANSACTION is skipped for simulation."
    if upper.startswith("BEGIN") or upper.startswith("DECLARE"):
        return "", False, "PL/SQL blocks require source packages and are skipped."

    if upper.startswith("SELECT") and " INTO " in upper and " FROM " in upper:
        sql = re.sub(r"\bINTO\b\s+.+?\bFROM\b", "FROM", sql, count=1, flags=re.IGNORECASE | re.DOTALL)

    return sql, True, ""


def statement_type(sql_text: str) -> str:
    sql = str(sql_text or "").strip()
    if not sql:
        return "UNKNOWN"
    first = sql.split(None, 1)[0].upper()
    return first if first in {"SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE"} else "UNKNOWN"


def extract_table_refs(sql_text: str) -> list[dict]:
    sql = re.sub(r"\s+", " ", str(sql_text or "").strip())
    upper = sql.upper()
    refs: list[dict] = []

    def add_ref(name: str, alias: str = "") -> None:
        token = name.strip().strip(",")
        if not token or token.startswith("("):
            return
        token = token.split(".")[-1]
        token = token.strip('"').upper()
        if not re.match(r"^[A-Z][A-Z0-9_$#]*$", token):
            return
        alias_token = alias.strip().strip(",").strip('"').upper()
        if alias_token in {"", "WHERE", "ORDER", "GROUP", "FOR", "FETCH", "CONNECT", "START", "JOIN", "ON"}:
            alias_token = ""
        if not any(item["name"] == token and item.get("alias", "") == alias_token for item in refs):
            refs.append({"name": token, "alias": alias_token})

    if upper.startswith("UPDATE "):
        match = re.match(r"UPDATE\s+([A-Z0-9_$.\"#]+)(?:\s+([A-Z0-9_\"#$]+))?", sql, re.IGNORECASE)
        if match:
            add_ref(match.group(1), match.group(2) or "")
        return refs

    if upper.startswith("INSERT INTO "):
        match = re.match(r"INSERT\s+INTO\s+([A-Z0-9_$.\"#]+)", sql, re.IGNORECASE)
        if match:
            add_ref(match.group(1), "")
        return refs

    if upper.startswith("DELETE FROM "):
        match = re.match(r"DELETE\s+FROM\s+([A-Z0-9_$.\"#]+)(?:\s+([A-Z0-9_\"#$]+))?", sql, re.IGNORECASE)
        if match:
            add_ref(match.group(1), match.group(2) or "")
        return refs

    if upper.startswith("MERGE INTO "):
        match = re.match(r"MERGE\s+INTO\s+([A-Z0-9_$.\"#]+)(?:\s+([A-Z0-9_\"#$]+))?", sql, re.IGNORECASE)
        if match:
            add_ref(match.group(1), match.group(2) or "")
        return refs

    from_match = re.search(r"\bFROM\b(.+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bFOR\b|\bFETCH\b|$)", sql, re.IGNORECASE)
    if from_match:
        source_clause = from_match.group(1)
        source_clause = re.sub(r"\bJOIN\b", ",", source_clause, flags=re.IGNORECASE)
        source_clause = re.sub(r"\bLEFT\b|\bRIGHT\b|\bFULL\b|\bOUTER\b|\bINNER\b|\bCROSS\b", " ", source_clause, flags=re.IGNORECASE)
        source_clause = re.sub(r"\bON\b.+?(?=,|$)", "", source_clause, flags=re.IGNORECASE)
        for piece in source_clause.split(","):
            item = piece.strip()
            if not item:
                continue
            parts = item.split()
            if not parts:
                continue
            table_name = parts[0]
            alias = parts[1] if len(parts) > 1 else ""
            add_ref(table_name, alias)
    return refs


def extract_bind_usage(sql_text: str, table_refs: list[dict], binds: list[dict]) -> dict[str, dict[str, dict]]:
    sql = re.sub(r"\s+", " ", str(sql_text or "").strip())
    ref_names = [item["name"] for item in table_refs]
    alias_map = {item["alias"]: item["name"] for item in table_refs if item.get("alias")}
    bind_lookup = {int(item.get("position", 0) or 0): item for item in binds or []}

    usage: dict[str, dict[str, dict]] = defaultdict(dict)

    def resolve_table(column_token: str) -> tuple[str, str]:
        raw = column_token.strip()
        if "." in raw:
            alias, col = raw.split(".", 1)
            return alias_map.get(alias.upper(), ""), col.upper()
        if len(ref_names) == 1:
            return ref_names[0], raw.upper()
        return "", raw.upper()

    def set_usage(table_name: str, column_name: str, role: str, position: int) -> None:
        if not table_name or not column_name:
            return
        info = usage[table_name].setdefault(column_name, {"roles": set(), "positions": [], "sample": None})
        info["roles"].add(role)
        if position and position not in info["positions"]:
            info["positions"].append(position)
        bind_item = bind_lookup.get(position)
        if bind_item and info["sample"] is None:
            info["sample"] = bind_item

    where_match = re.search(r"\bWHERE\b(.+?)(?:\bORDER\b|\bGROUP\b|\bFOR\b|\bFETCH\b|$)", sql, re.IGNORECASE)
    if where_match:
        where_clause = where_match.group(1)
        for col, pos in re.findall(r"((?:[A-Z][A-Z0-9_$#]*\.)?[A-Z][A-Z0-9_$#]*)\s*(?:=|>=|<=|>|<|LIKE)\s*:B(\d+)", where_clause, re.IGNORECASE):
            table_name, column_name = resolve_table(col)
            set_usage(table_name, column_name, "where", int(pos))
        for col, pos1, pos2 in re.findall(r"((?:[A-Z][A-Z0-9_$#]*\.)?[A-Z][A-Z0-9_$#]*)\s+BETWEEN\s+:B(\d+)\s+AND\s+:B(\d+)", where_clause, re.IGNORECASE):
            table_name, column_name = resolve_table(col)
            set_usage(table_name, column_name, "where", int(pos1))
            set_usage(table_name, column_name, "where", int(pos2))

    update_match = re.search(r"\bUPDATE\b\s+[A-Z0-9_$.\"#]+(?:\s+[A-Z0-9_\"#$]+)?\s+\bSET\b(.+?)(?:\bWHERE\b|$)", sql, re.IGNORECASE)
    if update_match and ref_names:
        table_name = ref_names[0]
        for col, pos in re.findall(r"([A-Z][A-Z0-9_$#]*)\s*=\s*:B(\d+)", update_match.group(1), re.IGNORECASE):
            set_usage(table_name, col.upper(), "set", int(pos))

    insert_match = re.search(r"\bINSERT\s+INTO\b\s+[A-Z0-9_$.\"#]+\s*\((.+?)\)\s*VALUES\s*\((.+?)\)", sql, re.IGNORECASE)
    if insert_match and ref_names:
        table_name = ref_names[0]
        columns = [part.strip().strip('"').upper() for part in insert_match.group(1).split(",")]
        positions = [int(pos) for pos in re.findall(r":B(\d+)", insert_match.group(2), re.IGNORECASE)]
        for idx, col in enumerate(columns):
            if idx < len(positions):
                set_usage(table_name, col, "insert", positions[idx])

    return usage


def enrich_analysis_with_lab_metadata(conn, statements: list[dict]) -> list[dict]:
    table_names = sorted({ref["name"] for stmt in statements for ref in stmt.get("table_refs", []) if ref.get("name")})
    table_meta = _load_table_metadata(conn, table_names)

    for stmt in statements:
        stmt["lab_replayable"] = bool(stmt.get("portable")) and stmt.get("statement_type") in {"SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE"}
        stmt["bind_usage"] = extract_bind_usage(stmt.get("normalized_sql", ""), stmt.get("table_refs", []), stmt.get("binds", []))
        stmt["tables_available"] = [name for name in [ref.get("name") for ref in stmt.get("table_refs", [])] if name in table_meta]

    models = []
    for table_name in table_names:
        meta = table_meta.get(table_name)
        if not meta:
            continue
        key_columns = sorted({
            column_name
            for stmt in statements
            for column_name, info in (stmt.get("bind_usage", {}).get(table_name, {}) or {}).items()
            if "where" in info.get("roles", set())
        })
        models.append({
            "source_table": table_name,
            "source_owner": meta["owner"],
            "column_count": len(meta["columns"]),
            "columns": meta["columns"],
            "key_columns": key_columns,
            "replay_sql_ids": sorted({
                stmt.get("sql_id", "")
                for stmt in statements
                if table_name in stmt.get("tables_available", [])
            }),
        })
    return models


def _load_table_metadata(conn, table_names: list[str]) -> dict[str, dict]:
    if not table_names:
        return {}

    cursor = conn.cursor()
    try:
        metadata: dict[str, dict] = {}
        for table_name in table_names:
            cursor.execute(
                """
                SELECT owner, COUNT(*) AS column_count
                FROM   dba_tab_columns
                WHERE  table_name = :table_name
                GROUP  BY owner
                ORDER  BY CASE WHEN owner IN ('SYS', 'SYSTEM', 'XDB') THEN 1 ELSE 0 END,
                          column_count DESC,
                          owner
                FETCH FIRST 1 ROWS ONLY
                """,
                {"table_name": table_name},
            )
            owner_row = cursor.fetchone()
            if not owner_row:
                continue
            owner = owner_row[0]
            cursor.execute(
                """
                SELECT column_name,
                       data_type,
                       data_length,
                       data_precision,
                       data_scale,
                       nullable,
                       char_length
                FROM   dba_tab_columns
                WHERE  owner = :owner
                AND    table_name = :table_name
                ORDER  BY column_id
                """,
                {"owner": owner, "table_name": table_name},
            )
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "data_type": row[1],
                    "data_length": int(row[2] or 0),
                    "data_precision": int(row[3] or 0) if row[3] is not None else None,
                    "data_scale": int(row[4] or 0) if row[4] is not None else None,
                    "nullable": row[5],
                    "char_length": int(row[6] or 0),
                })
            metadata[table_name] = {"owner": owner, "columns": columns}
        return metadata
    finally:
        cursor.close()


def build_lab_table_name(prefix: str, source_table: str) -> str:
    clean_prefix = re.sub(r"[^A-Z0-9_]", "", str(prefix or "").upper())[:18] or "PEAKLAB"
    clean_source = re.sub(r"[^A-Z0-9_]", "", str(source_table or "").upper())[:22] or "OBJ"
    return f"{clean_prefix}_{clean_source}"[:30]


def create_lab_schema(
    conn,
    *,
    table_models: list[dict],
    statements: list[dict],
    table_prefix: str,
    rows_per_table: int,
) -> list[dict]:
    cursor = conn.cursor()
    created: list[dict] = []
    try:
        for model in table_models:
            source_table = model["source_table"]
            lab_table = build_lab_table_name(table_prefix, source_table)
            _drop_table_if_exists(cursor, lab_table)
            ddl = _build_create_table_ddl(lab_table, model["columns"])
            cursor.execute(ddl)
            _create_indexes(cursor, lab_table, model.get("key_columns", []))
            special_rows = _build_special_rows(model, statements)
            _seed_table(cursor, lab_table, model["columns"], rows_per_table, special_rows)
            created.append({
                "source_table": source_table,
                "lab_table": lab_table,
                "row_count": rows_per_table,
                "special_rows": len(special_rows),
                "key_columns": model.get("key_columns", []),
            })
        conn.commit()
        return created
    finally:
        cursor.close()


def drop_lab_schema(conn, *, table_models: list[dict], table_prefix: str) -> list[str]:
    cursor = conn.cursor()
    dropped: list[str] = []
    try:
        for model in table_models:
            lab_table = build_lab_table_name(table_prefix, model["source_table"])
            _drop_table_if_exists(cursor, lab_table)
            dropped.append(lab_table)
        conn.commit()
        return dropped
    finally:
        cursor.close()


def build_simulation_statements(
    *,
    statements: list[dict],
    selected_sql_ids: list[str],
    selected_tables: list[str] | None,
    table_prefix: str,
) -> list[dict]:
    selected = set(selected_sql_ids or [])
    allowed_tables = {str(name).upper() for name in (selected_tables or []) if str(name or "").strip()}
    result: list[dict] = []
    for stmt in statements:
        sql_id = str(stmt.get("sql_id", "") or "")
        if selected and sql_id not in selected:
            continue
        if not stmt.get("lab_replayable"):
            continue
        stmt_tables = [str(ref.get("name", "") or "").upper() for ref in (stmt.get("table_refs") or []) if ref.get("name")]
        if allowed_tables and any(name not in allowed_tables for name in stmt_tables):
            continue
        normalized_sql = str(stmt.get("normalized_sql", "") or "").strip()
        if not normalized_sql:
            continue
        rewritten = normalized_sql
        for ref in stmt.get("table_refs", []):
            source_name = ref.get("name")
            if not source_name:
                continue
            lab_name = build_lab_table_name(table_prefix, source_name)
            rewritten = re.sub(rf"\b{re.escape(source_name)}\b", lab_name, rewritten, flags=re.IGNORECASE)
        rewritten = rewritten.strip()
        if not rewritten:
            continue
        result.append({
            "sql_id": sql_id,
            "sql_text": rewritten,
            "statement_type": stmt.get("statement_type", "UNKNOWN"),
            "weight": int(stmt.get("sample_count", 1) or 1),
            "sample_count": int(stmt.get("sample_count", 0) or 0),
            "primary_event": stmt.get("primary_event", ""),
            "binds": stmt.get("binds", []),
        })
    return result


def _drop_table_if_exists(cursor, table_name: str) -> None:
    try:
        cursor.execute(f"DROP TABLE {table_name} PURGE")
    except Exception:
        pass


def _build_create_table_ddl(table_name: str, columns: list[dict]) -> str:
    column_lines = []
    for column in columns:
        column_lines.append(f"{column['name']} {_column_type_ddl(column)}")
    if not column_lines:
        column_lines.append("ID NUMBER")
    return f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(column_lines) + "\n)"


def _column_type_ddl(column: dict) -> str:
    data_type = str(column.get("data_type", "VARCHAR2") or "VARCHAR2").upper()
    data_length = int(column.get("data_length", 0) or 0)
    precision = column.get("data_precision")
    scale = column.get("data_scale")
    char_length = int(column.get("char_length", 0) or 0)

    if data_type in {"VARCHAR2", "NVARCHAR2"}:
        size = max(1, min(4000, char_length or data_length or 100))
        return f"{data_type}({size})"
    if data_type in {"CHAR", "NCHAR"}:
        size = max(1, min(2000, char_length or data_length or 1))
        return f"{data_type}({size})"
    if data_type == "NUMBER":
        if precision is not None and scale is not None:
            return f"NUMBER({int(precision)},{int(scale)})"
        if precision is not None:
            return f"NUMBER({int(precision)})"
        return "NUMBER"
    if data_type.startswith("TIMESTAMP"):
        return data_type
    if data_type in {"DATE", "CLOB", "BLOB"}:
        return data_type
    if data_type == "RAW":
        size = max(1, min(2000, data_length or 32))
        return f"RAW({size})"
    return "VARCHAR2(4000)"


def _create_indexes(cursor, table_name: str, key_columns: list[str]) -> None:
    for index_no, column_name in enumerate(key_columns[:4], start=1):
        try:
            index_name = f"IX_{table_name}_{index_no}"[:30]
            cursor.execute(f"CREATE INDEX {index_name} ON {table_name} ({column_name})")
        except Exception:
            continue


def _build_special_rows(model: dict, statements: list[dict]) -> list[dict]:
    source_table = model["source_table"]
    columns = {col["name"]: col for col in model["columns"]}
    rows: list[dict] = []
    for stmt in statements:
        usage = (stmt.get("bind_usage", {}) or {}).get(source_table, {})
        if not usage:
            continue
        row: dict[str, Any] = {}
        for column_name, info in usage.items():
            if column_name not in columns:
                continue
            sample = info.get("sample") or {}
            value = _coerce_sample_value(sample, columns[column_name])
            if value is not None:
                row[column_name] = value
        if row:
            rows.append(row)
    return rows


def _coerce_sample_value(sample: dict, column: dict) -> Any:
    text = str(sample.get("sample_value_text", "") or "")
    kind = str(sample.get("sample_value_kind", "") or "").lower()
    data_type = str(column.get("data_type", "") or "").upper()

    if kind == "number":
        try:
            return int(text)
        except Exception:
            try:
                return float(text)
            except Exception:
                return 1
    if kind in {"date", "timestamp"}:
        try:
            return datetime.fromisoformat(text)
        except Exception:
            return datetime.now()
    if data_type == "DATE":
        return datetime.now().date()
    if data_type.startswith("TIMESTAMP"):
        return datetime.now()
    if "NUMBER" in data_type:
        return 1
    if text:
        return text[: max(1, min(4000, int(column.get("char_length", 0) or column.get("data_length", 100) or 100)))]
    return None


def _seed_table(cursor, table_name: str, columns: list[dict], rows_per_table: int, special_rows: list[dict]) -> None:
    total_rows = max(0, int(rows_per_table or 0))
    if total_rows <= 0:
        return

    ordered_columns = [col["name"] for col in columns]
    placeholder = ", ".join(f":{idx + 1}" for idx in range(len(ordered_columns)))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(ordered_columns)}) VALUES ({placeholder})"

    rows: list[list[Any]] = []
    for idx in range(total_rows):
        base = special_rows[idx] if idx < len(special_rows) else {}
        rows.append([base.get(col["name"], _random_value(col, idx)) for col in columns])

    batch_size = 500
    for start in range(0, len(rows), batch_size):
        cursor.executemany(insert_sql, rows[start:start + batch_size])


def _random_value(column: dict, row_index: int) -> Any:
    data_type = str(column.get("data_type", "") or "").upper()
    data_length = int(column.get("data_length", 0) or 0)
    char_length = int(column.get("char_length", 0) or 0)
    precision = column.get("data_precision")
    scale = column.get("data_scale")
    size = max(1, min(200, char_length or data_length or 20))

    if data_type == "DATE":
        return date.today() - timedelta(days=row_index % 30)
    if data_type.startswith("TIMESTAMP"):
        return datetime.now() - timedelta(minutes=row_index % 120)
    if data_type == "NUMBER":
        if scale and int(scale) > 0:
            return round((row_index % 10000) / 10.0, int(scale))
        limit = 10 ** min(int(precision or 6), 9)
        return (row_index % max(2, limit - 1)) + 1
    if data_type in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        base = f"{column['name']}_{row_index + 1}"
        return base[:size]
    if data_type == "RAW":
        raw_len = max(2, min(32, data_length or 16))
        return bytes((row_index + i) % 256 for i in range(raw_len))
    if data_type == "CLOB":
        base = f"{column['name']} replay row {row_index + 1}"
        return (base + " " + "".join(random.choices(string.ascii_uppercase, k=20)))[:4000]
    if data_type == "BLOB":
        return bytes((row_index + i) % 256 for i in range(16))
    return f"{column['name']}_{row_index + 1}"[:size]
