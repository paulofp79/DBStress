"""Historical SQL replay workload engine.

Executes a weighted mix of SQL statements captured from AWR/ASH analysis.
Each worker keeps a dedicated Oracle session open and repeatedly executes
one of the selected statements using captured bind samples when available.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Optional

import oracledb

from oracle_session import DEFAULT_CALL_TIMEOUT_MS, apply_call_timeout

_MAX_REPLAY_WORKERS = 128
_SAFE_SQL_PREFIXES = {"SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "BEGIN", "DECLARE"}


@dataclass
class ReplayBind:
    position: int
    name: str = ""
    datatype: str = ""
    sample_value: Any = None
    sample_value_text: str = ""
    sample_value_kind: str = ""


@dataclass
class ReplayStatement:
    sql_id: str
    sql_text: str
    statement_type: str = "UNKNOWN"
    weight: int = 1
    sample_count: int = 0
    primary_event: str = ""
    binds: list[ReplayBind] = field(default_factory=list)


@dataclass
class SqlReplayConfig:
    dsn: str
    user: str
    password: str
    statements: list[dict]
    mode: str = "thin"
    thread_count: int = 8
    duration_seconds: int = 60
    think_time_ms: int = 0
    module: str = "GC_PEAK_REPLAY"
    action_prefix: str = "replay"
    commit_every: int = 1
    call_timeout_ms: int = DEFAULT_CALL_TIMEOUT_MS


@dataclass
class SqlReplayStatus:
    executions: int = 0
    selects: int = 0
    dml: int = 0
    plsql: int = 0
    commits: int = 0
    rollbacks: int = 0
    errors: int = 0
    elapsed: float = 0.0
    running: bool = False
    phase: str = "IDLE"
    status_message: str = ""
    last_error: str = ""
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def increment(self, field_name: str, amount: int = 1) -> None:
        with self._lock:
            current = getattr(self, field_name, 0)
            setattr(self, field_name, current + amount)

    def set_phase(self, phase: str, message: str = "") -> None:
        with self._lock:
            self.phase = phase
            self.status_message = message

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors += 1
            self.last_error = message

    def to_dict(self, duration: int = 0) -> dict:
        with self._lock:
            return {
                "executions": self.executions,
                "selects": self.selects,
                "dml": self.dml,
                "plsql": self.plsql,
                "commits": self.commits,
                "rollbacks": self.rollbacks,
                "errors": self.errors,
                "elapsed": round(self.elapsed, 1),
                "running": self.running,
                "phase": self.phase,
                "status_message": self.status_message,
                "duration": duration,
                "last_error": self.last_error,
            }


class SqlReplayRunner:
    """Runs a weighted SQL replay workload in background worker threads."""

    def __init__(self) -> None:
        self._config: Optional[SqlReplayConfig] = None
        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._start_time: float = 0.0
        self._worker_count: int = 0
        self._statements: list[ReplayStatement] = []
        self._weighted_statements: list[ReplayStatement] = []
        self.status = SqlReplayStatus()

    @property
    def is_running(self) -> bool:
        return any(thread.is_alive() for thread in self._threads)

    def start(
        self,
        config: SqlReplayConfig,
        progress_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        if self.is_running:
            raise RuntimeError("SQL replay is already running.")

        self._config = config
        self._worker_count = max(1, min(int(config.thread_count or 1), _MAX_REPLAY_WORKERS))
        self._stop_event.clear()
        self._start_time = time.monotonic()
        self._statements = self._build_statements(config.statements)
        self._weighted_statements = self._expand_weights(self._statements)
        if not self._weighted_statements:
            raise RuntimeError("No executable SQL statements were selected for replay.")

        self.status = SqlReplayStatus(
            running=True,
            phase="RUNNING",
            status_message=(
                f"Running weighted replay with {self._worker_count} worker(s) "
                f"across {len(self._statements)} SQL statement(s)."
            ),
        )

        if str(config.mode or "thin").lower() == "thick":
            try:
                if oracledb.is_thin_mode():
                    oracledb.init_oracle_client()
            except Exception:
                pass

        self._threads = []
        for idx in range(self._worker_count):
            thread = threading.Thread(
                target=self._worker,
                args=(idx,),
                name=f"sql-replay-worker-{idx}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

        if progress_callback:
            reporter = threading.Thread(
                target=self._reporter,
                args=(progress_callback,),
                name="sql-replay-reporter",
                daemon=True,
            )
            self._threads.append(reporter)
            reporter.start()

    def stop(self, timeout: float = 10.0) -> dict:
        self._stop_event.set()
        deadline = time.monotonic() + max(0.0, timeout)
        for thread in self._threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)
        self._threads.clear()
        if self._start_time:
            self.status.elapsed = time.monotonic() - self._start_time
        self.status.running = False
        if self.status.phase not in {"COMPLETE", "STOPPED"}:
            self.status.set_phase("STOPPED", "SQL replay stopped.")
        return self.status.to_dict(duration=self._config.duration_seconds if self._config else 0)

    def _reporter(self, callback: Callable[[dict], None]) -> None:
        while not self._stop_event.is_set():
            if self._start_time:
                self.status.elapsed = time.monotonic() - self._start_time
            callback(self.status.to_dict(duration=self._config.duration_seconds if self._config else 0))
            self._stop_event.wait(timeout=1.0)
        if self._start_time:
            self.status.elapsed = time.monotonic() - self._start_time
        callback(self.status.to_dict(duration=self._config.duration_seconds if self._config else 0))

    def _build_statements(self, raw_statements: list[dict]) -> list[ReplayStatement]:
        statements: list[ReplayStatement] = []
        for item in raw_statements or []:
            sql_text = str(item.get("sql_text", "") or "").strip()
            statement_type = str(item.get("statement_type", "") or "").upper()
            if not sql_text:
                continue
            if not statement_type:
                statement_type = _classify_statement(sql_text)
            if statement_type not in _SAFE_SQL_PREFIXES:
                continue
            binds = [
                ReplayBind(
                    position=max(1, int(bind.get("position", 1) or 1)),
                    name=str(bind.get("name", "") or ""),
                    datatype=str(bind.get("datatype", "") or ""),
                    sample_value=bind.get("sample_value"),
                    sample_value_text=str(bind.get("sample_value_text", "") or ""),
                    sample_value_kind=str(bind.get("sample_value_kind", "") or ""),
                )
                for bind in (item.get("binds") or [])
            ]
            statements.append(
                ReplayStatement(
                    sql_id=str(item.get("sql_id", "") or ""),
                    sql_text=sql_text,
                    statement_type=statement_type,
                    weight=max(1, int(item.get("weight", item.get("sample_count", 1)) or 1)),
                    sample_count=max(0, int(item.get("sample_count", 0) or 0)),
                    primary_event=str(item.get("primary_event", "") or ""),
                    binds=binds,
                )
            )
        return statements

    def _expand_weights(self, statements: list[ReplayStatement]) -> list[ReplayStatement]:
        weighted: list[ReplayStatement] = []
        for statement in statements:
            copies = max(1, min(100, int(statement.weight or 1)))
            weighted.extend([statement] * copies)
        return weighted

    def _worker(self, worker_idx: int) -> None:
        assert self._config is not None
        cfg = self._config
        conn = None
        try:
            conn = oracledb.connect(user=cfg.user, password=cfg.password, dsn=cfg.dsn)
            apply_call_timeout(conn, cfg.call_timeout_ms)
            try:
                conn.module = cfg.module[:48]
                conn.action = f"{cfg.action_prefix[:20]}-{worker_idx}"
            except Exception:
                pass

            cur = conn.cursor()
            try:
                execution_count = 0
                while not self._stop_event.is_set():
                    elapsed = time.monotonic() - self._start_time if self._start_time else 0.0
                    if cfg.duration_seconds > 0 and elapsed >= cfg.duration_seconds:
                        self._stop_event.set()
                        break

                    statement = random.choice(self._weighted_statements)
                    binds = _build_bind_values(statement.binds)

                    try:
                        cur.execute(statement.sql_text, binds)
                        if statement.statement_type in {"SELECT", "WITH"}:
                            cur.fetchmany(1)
                            self.status.increment("selects")
                        elif statement.statement_type in {"BEGIN", "DECLARE"}:
                            self.status.increment("plsql")
                        else:
                            self.status.increment("dml")
                            execution_count += 1
                            if max(1, int(cfg.commit_every or 1)) == 1 or execution_count % max(1, int(cfg.commit_every or 1)) == 0:
                                conn.commit()
                                self.status.increment("commits")
                        self.status.increment("executions")
                    except Exception as exc:
                        try:
                            conn.rollback()
                            self.status.increment("rollbacks")
                        except Exception:
                            pass
                        self.status.record_error(f"{statement.sql_id or statement.statement_type}: {exc}")

                    if cfg.think_time_ms > 0 and not self._stop_event.is_set():
                        if self._stop_event.wait(cfg.think_time_ms / 1000.0):
                            break
            finally:
                cur.close()
        except Exception as exc:
            self.status.record_error(str(exc))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

            if self._start_time:
                self.status.elapsed = time.monotonic() - self._start_time

            running_threads = [thread for thread in self._threads if thread.is_alive() and thread.name.startswith("sql-replay-worker-")]
            if len(running_threads) <= 1:
                self.status.running = False
                if self.status.phase not in {"STOPPED", "ERROR"}:
                    self.status.set_phase("COMPLETE", "SQL replay completed.")
                self._stop_event.set()


def _classify_statement(sql_text: str) -> str:
    first_token = str(sql_text or "").strip().split(None, 1)[0].upper() if str(sql_text or "").strip() else ""
    if first_token == "WITH":
        return "WITH"
    if first_token in _SAFE_SQL_PREFIXES:
        return first_token
    return "UNKNOWN"


def _build_bind_values(bind_templates: list[ReplayBind]) -> list[Any]:
    if not bind_templates:
        return []
    ordered = sorted(bind_templates, key=lambda item: item.position)
    return [_coerce_bind_value(bind) for bind in ordered]


def _coerce_bind_value(bind: ReplayBind) -> Any:
    if bind.sample_value is not None:
        return bind.sample_value

    dtype = str(bind.datatype or "").upper()
    kind = str(bind.sample_value_kind or "").lower()
    text = str(bind.sample_value_text or "")

    if kind == "number":
        try:
            return int(text)
        except Exception:
            try:
                return float(text)
            except Exception:
                return 1

    if kind in {"date", "timestamp"}:
        parsed = _parse_datetime_text(text)
        if parsed is not None:
            return parsed

    if "NUMBER" in dtype or "INTEGER" in dtype or "DECIMAL" in dtype:
        return 1
    if "DATE" in dtype:
        return datetime.now().date()
    if "TIMESTAMP" in dtype:
        return datetime.now()
    if "CHAR" in dtype or "CLOB" in dtype or "VARCHAR" in dtype:
        return text or "GC_REPLAY"
    return text or 1


def _parse_datetime_text(text: str) -> Optional[datetime | date]:
    raw = str(text or "").strip()
    if not raw:
        return None
    for pattern in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
        "%d-%b-%y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(raw, pattern)
            if pattern == "%Y-%m-%d":
                return parsed.date()
            return parsed
        except Exception:
            continue
    return None
