"""Concurrent workload engine for driving GC contention.

Spawns multiple threads that perform mixed DML (INSERT / UPDATE / DELETE /
SELECT / SELECT INTO)
across the benchmark tables using an oracledb connection pool.  A
configurable hot-row percentage concentrates updates on a small set of
rows to maximise cross-instance block transfers.

The module can be used standalone or imported into another framework.
"""

from __future__ import annotations

import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import oracledb
from oracle_session import DEFAULT_CALL_TIMEOUT_MS, apply_call_timeout

_MAX_PHYSICAL_WORKERS = 128
MAX_WORKLOAD_SEED_ROWS = 100000


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class WorkloadConfig:
    """Parameters for a single workload run."""

    dsn: str
    user: str
    password: str
    mode: str = "thin"
    table_prefix: str = "GCB"
    table_count: int = 10
    thread_count: int = 8           # requested worker count for this process
    duration_seconds: int = 60
    hot_row_pct: int = 5            # 1..10
    seed_rows: int = 500            # cached / ensured rows per table for the run
    commit_batch: int = 10
    insert_pct: int = 40
    update_pct: int = 40
    delete_pct: int = 20
    select_pct: int = 0
    select_into_pct: int = 0
    pedt_update_pct: int = 0

    # ---- Contention mode ---------------------------------------------------
    # NORMAL     : mixed DML (INSERT/UPDATE/DELETE) spread across all tables
    # HAMMER     : SELECT FOR UPDATE NOWAIT + UPDATE on the same 10 hot rows
    #              across ALL tables — maximises gc current block congested
    # LMS_STRESS : rapid-fire UPDATEs on a very small hot window across all
    #              tables, commit after every op — floods the LMS queue
    # EXTREME_LMS: NOWAIT lock + UPDATE + COMMIT on an ultra-tight hot window
    #              across the first 1-2 tables — maximises repeated block
    #              bouncing on the same data and index blocks
    contention_mode: str = "NORMAL"   # NORMAL | HAMMER | LMS_STRESS | EXTREME_LMS
    lock_hold_ms: int = 0              # ms to hold lock before UPDATE (HAMMER only)
    call_timeout_ms: int = DEFAULT_CALL_TIMEOUT_MS


# ---------------------------------------------------------------------------
# Thread-safe status tracking
# ---------------------------------------------------------------------------

@dataclass
class WorkloadStatus:
    """Live counters updated by worker threads."""

    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    selects: int = 0
    select_intos: int = 0
    pedt_updates: int = 0
    errors: int = 0
    elapsed: float = 0.0
    running: bool = False
    phase: str = "IDLE"
    status_message: str = ""
    last_error: str = ""
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False,
    )

    def increment(self, op: str, count: int = 1) -> None:
        """Increment the counter for one workload operation."""
        with self._lock:
            current = getattr(self, op, 0)
            setattr(self, op, current + count)

    def record_error(self, message: str) -> None:
        """Record an error without crashing the worker."""
        with self._lock:
            self.errors += 1
            self.last_error = message

    def set_phase(self, phase: str, message: str = "") -> None:
        with self._lock:
            self.phase = phase
            self.status_message = message

    def to_dict(self, duration: int = 0) -> dict:
        """Return a JSON-serialisable snapshot of the counters."""
        with self._lock:
            return {
                "inserts": self.inserts,
                "updates": self.updates,
                "deletes": self.deletes,
                "selects": self.selects,
                "select_intos": self.select_intos,
                "pedt_updates": self.pedt_updates,
                "errors": self.errors,
                "elapsed": round(self.elapsed, 1),
                "running": self.running,
                "phase": self.phase,
                "status_message": self.status_message,
                "duration": duration,
                "last_error": self.last_error,
            }


# ---------------------------------------------------------------------------
# Random data helpers
# ---------------------------------------------------------------------------

def _random_ref() -> str:
    return "ORD-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def _random_name() -> str:
    first = random.choice(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank"])
    last = random.choice(["Smith", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"])
    return f"{first} {last}"


def _random_status() -> str:
    return random.choice(["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"])


def _random_payload(length: int = 200) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=length))


def _random_seg_class() -> str:
    return "CLASS_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def _random_seg_manager() -> str:
    return "MAN_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def _random_mod_user() -> str:
    return "USR_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def _random_term_mod() -> str:
    return "TERM_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def _random_seg_calc() -> str:
    return "CAL_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


# ---------------------------------------------------------------------------
# Workload runner
# ---------------------------------------------------------------------------

class WorkloadRunner:
    """Manages background worker threads that drive contention workload."""

    def __init__(self) -> None:
        self._pool: Optional[oracledb.ConnectionPool] = None
        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._config: Optional[WorkloadConfig] = None
        self._worker_count: int = 0
        self._start_time: float = 0.0
        self.status = WorkloadStatus()

        # Per-table row-ID caches: {table_idx: [pk_values]}
        self._all_rows: dict[int, list[int]] = {}
        self._hot_rows: dict[int, list[int]] = {}

    @property
    def is_running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    # ---- public interface -------------------------------------------------

    def prepare(
        self,
        config: WorkloadConfig,
        prepare_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """Build pool, prepare metadata, and warm sessions before timed run."""
        if self.is_running:
            raise RuntimeError("Workload already running.")

        self._config = config
        self._worker_count = max(1, min(config.thread_count, _MAX_PHYSICAL_WORKERS))
        self._stop_event.clear()
        self.status = WorkloadStatus(running=True, phase="PREPARING")
        self.status.elapsed = 0.0
        self._start_time = 0.0

        def emit(message: str, phase: str) -> None:
            self.status.set_phase(phase, message)
            if prepare_callback:
                prepare_callback(self.status.to_dict(duration=self._config.duration_seconds))

        # Initialise thick mode if needed
        if config.mode.lower() == "thick":
            try:
                if oracledb.is_thin_mode():
                    oracledb.init_oracle_client()
            except Exception:
                pass

        # Create connection pool
        self._pool = oracledb.create_pool(
            user=config.user,
            password=config.password,
            dsn=config.dsn,
            min=1,
            max=self._worker_count,
            increment=max(1, min(32, self._worker_count)),
        )

        emit(
            f"Preparing schema metadata for {config.table_count} tables and {config.seed_rows} rows/table...",
            "PREPARING",
        )
        self._validate_schema_extensions()
        self._prepare_seed_data_parallel(prepare_callback)
        emit(
            f"Warming up {self._worker_count} worker sessions before timed run...",
            "WARMING",
        )
        self._warm_sessions_parallel(prepare_callback)

    def start(
        self,
        progress_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """Launch worker threads and start the timed workload window."""
        if not self._config or not self._pool:
            raise RuntimeError("Workload must be prepared before start.")
        config = self._config

        # Start the measured workload window only after startup preparation completes.
        self._start_time = time.monotonic()
        self.status.set_phase("RUNNING", "Timed workload running.")

        # Choose worker function based on contention mode
        mode = config.contention_mode.upper()
        if mode == "HAMMER":
            worker_fn = self._worker_hammer
        elif mode == "EXTREME_LMS":
            worker_fn = self._worker_extreme_lms
        elif mode == "LMS_STRESS":
            worker_fn = self._worker_lms_stress
        else:
            worker_fn = self._worker

        # Launch worker threads
        self._threads = []
        for idx in range(self._worker_count):
            t = threading.Thread(
                target=worker_fn,
                name=f"gcb-worker-{idx}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()

        # Launch progress-reporter thread
        if progress_callback:
            reporter = threading.Thread(
                target=self._reporter,
                args=(progress_callback,),
                name="gcb-reporter",
                daemon=True,
            )
            reporter.start()
            self._threads.append(reporter)

    def stop(self, timeout: float = 10.0) -> dict:
        """Signal workers to stop and wait for them to finish."""
        self._stop_event.set()
        if self.status.phase in ("PREPARING", "WARMING") and not self._threads:
            self.status.running = False
            self.status.set_phase("STOPPED", "Preparation stopped.")
            return self.status.to_dict(
                duration=self._config.duration_seconds if self._config else 0,
            )
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads.clear()
        if self._start_time:
            self.status.elapsed = time.monotonic() - self._start_time
        self.status.running = False
        if self.status.phase not in ("ERROR", "STOPPED", "COMPLETE"):
            if self._config and self.status.elapsed + 0.01 >= self._config.duration_seconds:
                self.status.set_phase("COMPLETE", "Timed workload finished.")
            else:
                self.status.set_phase("STOPPED", "Workload stopped.")

        if self._pool:
            try:
                self._pool.close(force=True)
            except Exception:
                pass
            self._pool = None

        return self.status.to_dict(
            duration=self._config.duration_seconds if self._config else 0,
        )

    # ---- internal ---------------------------------------------------------

    def _reporter(self, callback: Callable[[dict], None]) -> None:
        """Periodically invoke the progress callback."""
        assert self._config is not None
        while not self._stop_event.is_set():
            self.status.elapsed = time.monotonic() - self._start_time
            callback(self.status.to_dict(duration=self._config.duration_seconds))
            self._stop_event.wait(timeout=2.0)
        # Final report
        self.status.elapsed = time.monotonic() - self._start_time
        callback(self.status.to_dict(duration=self._config.duration_seconds))

    def _worker_slot(self) -> int:
        """Return a stable numeric slot derived from the worker thread name."""
        name = threading.current_thread().name
        try:
            return int(name.rsplit("-", 1)[1])
        except Exception:
            return 0

    def _contention_row_pool(self, table_idx: int, minimum_rows: int) -> list[int]:
        """Return a wider hot-row pool to favor GC over same-row TX waits."""
        hot = self._hot_rows.get(table_idx, [])
        all_rows = self._all_rows.get(table_idx, [])
        if not all_rows:
            return []
        if len(hot) >= minimum_rows:
            return hot
        return all_rows[:min(len(all_rows), max(1, int(minimum_rows or 1)))]

    def _narrow_contention_row_pool(self, table_idx: int, row_limit: int) -> list[int]:
        """Return the hottest rows only, keeping the working set intentionally tiny."""
        base = self._hot_rows.get(table_idx, []) or self._all_rows.get(table_idx, [])
        if not base:
            return []
        return base[:min(len(base), max(1, int(row_limit or 1)))]

    def _thread_row_window(self, rows: list[int], slot: int, window_size: int) -> list[int]:
        """Return a thread-specific rotating window over a shared hot-row pool."""
        if not rows:
            return []
        if len(rows) <= window_size:
            return rows
        start = (slot * max(1, window_size // 2)) % len(rows)
        window = rows[start:start + window_size]
        if len(window) < window_size:
            window.extend(rows[:window_size - len(window)])
        return window

    def _validate_schema_extensions(self) -> None:
        """Fail early when optional workload columns are missing from the schema."""
        assert self._config is not None
        if self._config.pedt_update_pct <= 0:
            return
        assert self._pool is not None

        conn = self._pool.acquire()
        cursor = None
        try:
            cursor = conn.cursor()
            tname = f"{self._config.table_prefix}_ORDER_01"
            cursor.execute(
                """
                SELECT column_name
                FROM   user_tab_columns
                WHERE  table_name = :table_name
                """,
                {"table_name": tname.upper()},
            )
            present = {str(row[0]).upper() for row in cursor.fetchall()}
            required = {
                "PECDGENT",
                "PENUMPER",
                "PECLASEG",
                "PESEGCLA",
                "PEFECSEG",
                "PESEGMAN",
                "PEUSUMOD",
                "PETERMOD",
                "PESUCMOD",
                "PESEGCAL",
                "PEHSTAMP",
            }
            missing = sorted(required - present)
            if missing:
                raise RuntimeError(
                    "PEDT-style update workload requires the extended benchmark schema. "
                    "Recreate the schema first so these columns exist: " + ", ".join(missing)
                )
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            self._pool.release(conn)

    def _worker(self) -> None:
        """Single worker thread: mixed DML loop until stop or duration."""
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config

        conn = None
        cursor = None
        try:
            conn = self._pool.acquire()
            apply_call_timeout(conn, cfg.call_timeout_ms)
            cursor = conn.cursor()
            commit_counter = 0

            while not self._stop_event.is_set():
                # Check duration
                elapsed = time.monotonic() - self._start_time
                if elapsed >= cfg.duration_seconds:
                    self._stop_event.set()
                    break

                # Pick a random table
                table_idx = random.randint(1, cfg.table_count)
                tname = f"{cfg.table_prefix}_ORDER_{table_idx:02d}"

                total_mix = max(
                    1,
                    cfg.insert_pct + cfg.update_pct + cfg.delete_pct + cfg.select_pct + cfg.select_into_pct + cfg.pedt_update_pct,
                )
                roll = random.random()
                try:
                    insert_cutoff = cfg.insert_pct / total_mix
                    update_cutoff = (cfg.insert_pct + cfg.update_pct) / total_mix
                    delete_cutoff = (cfg.insert_pct + cfg.update_pct + cfg.delete_pct) / total_mix
                    select_cutoff = (
                        cfg.insert_pct + cfg.update_pct + cfg.delete_pct + cfg.select_pct
                    ) / total_mix
                    select_into_cutoff = (
                        cfg.insert_pct
                        + cfg.update_pct
                        + cfg.delete_pct
                        + cfg.select_pct
                        + cfg.select_into_pct
                    ) / total_mix

                    if roll < insert_cutoff:
                        self._do_insert(cursor, tname, table_idx)
                        self.status.increment("inserts")
                    elif roll < update_cutoff:
                        self._do_update(cursor, tname, table_idx)
                        self.status.increment("updates")
                    elif roll < delete_cutoff:
                        self._do_delete_and_replace(cursor, tname, table_idx)
                        self.status.increment("deletes")
                    elif roll < select_cutoff:
                        self._do_select(cursor, tname, table_idx)
                        self.status.increment("selects")
                    elif roll < select_into_cutoff:
                        self._do_select_into(cursor, tname, table_idx)
                        self.status.increment("select_intos")
                    else:
                        self._do_pedt_like_update(cursor, tname, table_idx)
                        self.status.increment("pedt_updates")

                    commit_counter += 1
                    if commit_counter >= cfg.commit_batch:
                        conn.commit()
                        commit_counter = 0

                except oracledb.DatabaseError as exc:
                    self.status.record_error(str(exc))
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    commit_counter = 0

            # Final commit
            try:
                conn.commit()
            except Exception:
                pass

        except oracledb.DatabaseError as exc:
            self.status.record_error(str(exc))
        finally:
            if conn is not None:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                try:
                    self._pool.release(conn)
                except Exception:
                    pass

    # ---- Hammer / LMS-Stress workers --------------------------------------

    def _worker_hammer(self) -> None:
        """Hammer mode worker.

        All threads issue ``SELECT … FOR UPDATE NOWAIT`` followed by an
        ``UPDATE`` on the same small set of hot rows across every table.
        This forces every session to request *current* block copies from
        the global cache, concentrating GC traffic on a tiny set of blocks
        and overloading the LMS process queue — the primary driver of
        ``gc current block congested`` waits.

        Strategy
        --------
        * Rotate across all tables (not just table-1) so block-master nodes
          receive requests from many different GRD entries simultaneously.
        * Use only the top-10 PKs per table (lowest PKs — densely packed,
          same data-block).
        * Optional ``lock_hold_ms`` pause between lock acquisition and UPDATE
          increases the window during which other sessions are queued behind
          LMS, raising the probability of "congested" classification.
        * On ``ORA-00054`` / ``ORA-30006`` (resource busy): skip without
          sleeping — keeps the request rate high.
        """
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config

        # Use a much wider hot pool to reduce same-row TX waits while still
        # concentrating access on nearby rows/blocks for RAC GC pressure.
        hammer_rows: dict[int, list[int]] = {
            idx: self._contention_row_pool(idx, 128)
            for idx in range(1, cfg.table_count + 1)
        }
        slot = self._worker_slot()

        conn = None
        cursor = None
        try:
            conn = self._pool.acquire()
            apply_call_timeout(conn, cfg.call_timeout_ms)
            cursor = conn.cursor()
            ops = 0

            while not self._stop_event.is_set():
                elapsed = time.monotonic() - self._start_time
                if elapsed >= cfg.duration_seconds:
                    self._stop_event.set()
                    break

                # Rotate across ALL tables for maximum GRD spread
                table_idx = random.randint(1, cfg.table_count)
                hot = hammer_rows.get(table_idx, [])
                if not hot:
                    continue
                pk = random.choice(self._thread_row_window(hot, slot, 16))
                tname = f"{cfg.table_prefix}_ORDER_{table_idx:02d}"

                try:
                    # Acquire current block via FOR UPDATE (forces gc current block transfer)
                    cursor.execute(
                        f"SELECT order_id FROM {tname} "
                        f"WHERE order_id = :pk FOR UPDATE NOWAIT",
                        {"pk": pk},
                    )
                    if cursor.fetchone() is None:
                        conn.rollback()
                        continue

                    # Hold the lock to let other sessions pile up in the LMS queue
                    if cfg.lock_hold_ms > 0:
                        time.sleep(cfg.lock_hold_ms / 1000.0)

                    # Modify the block (another gc current block if already released)
                    cursor.execute(
                        f"UPDATE {tname} SET "
                        f"amount = :amt, status = :st, ship_date = SYSDATE "
                        f"WHERE order_id = :pk",
                        {
                            "amt": round(random.uniform(10, 5000), 2),
                            "st":  _random_status(),
                            "pk":  pk,
                        },
                    )
                    self.status.increment("updates")
                    ops += 1

                    # Commit every commit_batch ops to release blocks & restart contention
                    if ops >= cfg.commit_batch:
                        conn.commit()
                        ops = 0

                except oracledb.DatabaseError as exc:
                    err_str = str(exc)
                    if "ORA-00054" in err_str or "ORA-30006" in err_str:
                        # Expected: row locked by another thread — just retry
                        pass
                    else:
                        self.status.record_error(err_str)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    ops = 0

            try:
                conn.commit()
            except Exception:
                pass

        except oracledb.DatabaseError as exc:
            self.status.record_error(str(exc))
        finally:
            if conn is not None:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                try:
                    self._pool.release(conn)
                except Exception:
                    pass

    def _worker_lms_stress(self) -> None:
        """LMS Stress mode worker — maximum GC request rate.

        All threads hammer a narrow hot window in every table with
        rapid-fire plain UPDATEs and commit after every single
        operation.  This generates a very high rate of Cache Fusion
        block-transfer requests per second, designed to overflow the
        LMS receive-queue and reliably produce
        ``gc current block congested`` waits while still avoiding the
        worst same-row lock pileups.

        Why commit=1?
        -------------
        Each COMMIT flushes the dirty block back to the global cache,
        which immediately makes it eligible for the next session's
        request.  With 32 threads all committing after every UPDATE,
        the LMS message queue receives a burst of ``BAST`` (Block
        Affinity Steal) callbacks that it cannot drain fast enough —
        the classic congestion scenario.
        """
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config

        # Tighten the working set so nearby rows and index leaves bounce
        # constantly, but leave enough room to avoid total collapse into
        # TX row-lock waits.
        stress_rows: dict[int, list[int]] = {
            idx: self._narrow_contention_row_pool(idx, 24)
            for idx in range(1, cfg.table_count + 1)
        }
        slot = self._worker_slot()

        conn = None
        cursor = None
        try:
            conn = self._pool.acquire()
            apply_call_timeout(conn, cfg.call_timeout_ms)
            cursor = conn.cursor()

            while not self._stop_event.is_set():
                elapsed = time.monotonic() - self._start_time
                if elapsed >= cfg.duration_seconds:
                    self._stop_event.set()
                    break

                table_idx = random.randint(1, cfg.table_count)
                rows = stress_rows.get(table_idx, [])
                if not rows:
                    continue
                pk = random.choice(self._thread_row_window(rows, slot, 8))
                tname = f"{cfg.table_prefix}_ORDER_{table_idx:02d}"

                try:
                    cursor.execute(
                        f"UPDATE {tname} SET "
                        f"amount = :amt, status = :st, ship_date = SYSDATE "
                        f"WHERE order_id = :pk",
                        {
                            "amt": round(random.uniform(10, 5000), 2),
                            "st":  _random_status(),
                            "pk":  pk,
                        },
                    )
                    self.status.increment("updates")
                    # Commit every op — maximises GC block-transfer request rate
                    conn.commit()

                except oracledb.DatabaseError as exc:
                    self.status.record_error(str(exc))
                    try:
                        conn.rollback()
                    except Exception:
                        pass

        except oracledb.DatabaseError as exc:
            self.status.record_error(str(exc))
        finally:
            if conn is not None:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                try:
                    self._pool.release(conn)
                except Exception:
                    pass

    def _worker_extreme_lms(self) -> None:
        """Extreme LMS mode worker — ultra-tight block bouncing.

        This mode focuses all workers on the first one or two tables and
        only the first handful of hot PKs.  It uses ``FOR UPDATE NOWAIT``
        to avoid long row-lock stalls, then commits every operation so
        the same current blocks are requested and handed off again
        immediately.
        """
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config

        focus_table_count = max(1, min(cfg.table_count, 2))
        focus_tables = list(range(1, focus_table_count + 1))
        extreme_rows: dict[int, list[int]] = {
            idx: self._narrow_contention_row_pool(idx, 8)
            for idx in focus_tables
        }
        slot = self._worker_slot()

        conn = None
        cursor = None
        try:
            conn = self._pool.acquire()
            apply_call_timeout(conn, cfg.call_timeout_ms)
            cursor = conn.cursor()

            while not self._stop_event.is_set():
                elapsed = time.monotonic() - self._start_time
                if elapsed >= cfg.duration_seconds:
                    self._stop_event.set()
                    break

                table_idx = focus_tables[(slot + random.randint(0, focus_table_count - 1)) % focus_table_count]
                rows = extreme_rows.get(table_idx, [])
                if not rows:
                    continue
                pk = random.choice(self._thread_row_window(rows, slot, 4))
                tname = f"{cfg.table_prefix}_ORDER_{table_idx:02d}"

                try:
                    cursor.execute(
                        f"SELECT order_id FROM {tname} "
                        f"WHERE order_id = :pk FOR UPDATE NOWAIT",
                        {"pk": pk},
                    )
                    if cursor.fetchone() is None:
                        conn.rollback()
                        continue

                    cursor.execute(
                        f"UPDATE {tname} SET "
                        f"amount = NVL(amount, 0) + 1, "
                        f"status = CASE status WHEN 'PROCESSING' THEN 'NEW' ELSE 'PROCESSING' END, "
                        f"ship_date = SYSDATE "
                        f"WHERE order_id = :pk",
                        {"pk": pk},
                    )
                    self.status.increment("updates")
                    conn.commit()

                except oracledb.DatabaseError as exc:
                    err_str = str(exc)
                    if "ORA-00054" in err_str or "ORA-30006" in err_str:
                        pass
                    else:
                        self.status.record_error(err_str)
                    try:
                        conn.rollback()
                    except Exception:
                        pass

        except oracledb.DatabaseError as exc:
            self.status.record_error(str(exc))
        finally:
            if conn is not None:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                try:
                    self._pool.release(conn)
                except Exception:
                    pass

    def _pick_target_row(self, table_idx: int) -> Optional[int]:
        """Pick a row PK, biased toward hot rows."""
        hot = self._hot_rows.get(table_idx, [])
        all_r = self._all_rows.get(table_idx, [])
        if not all_r:
            return None
        if hot and random.random() < 0.4:
            return random.choice(hot)
        return random.choice(all_r)

    def _do_insert(self, cursor, tname: str, table_idx: int) -> None:
        """Insert a new row with random data."""
        cursor.execute(
            f"INSERT INTO {tname} "
            f"(order_ref, customer_name, status, order_date, amount, quantity, discount, payload, "
            f"pecdgent, penumper, peclaseg, pesegcla, pefecseg, pesegman, peusumod, petermod, pesucmod, pesegcal, pehstamp) "
            f"VALUES (:ref, :cust, :st, SYSDATE - DBMS_RANDOM.VALUE(0, 365), "
            f":amt, :qty, :disc, :pay, :ent, :num, :claseg, :segcla, SYSDATE, :segman, :usumod, :termod, :sucmod, :segcal, :hstamp)",
            {
                "ref": _random_ref(),
                "cust": _random_name(),
                "st": _random_status(),
                "amt": round(random.uniform(10, 5000), 2),
                "qty": random.randint(1, 100),
                "disc": round(random.uniform(0, 50), 2),
                "pay": _random_payload(),
                "ent": 1000 + random.randint(0, 63),
                "num": random.randint(1, 10_000_000),
                "claseg": random.choice(["SEG_A", "SEG_B", "SEG_C", "SEG_D"]),
                "segcla": _random_seg_class(),
                "segman": _random_seg_manager(),
                "usumod": _random_mod_user(),
                "termod": _random_term_mod(),
                "sucmod": random.randint(1, 999),
                "segcal": _random_seg_calc(),
                "hstamp": random.randint(100000, 999999999),
            },
        )

    def _do_update(self, cursor, tname: str, table_idx: int) -> None:
        """Update a row, biased toward the hot set."""
        pk = self._pick_target_row(table_idx)
        if pk is None:
            # Fall back to insert if no rows available
            self._do_insert(cursor, tname, table_idx)
            return
        cursor.execute(
            f"UPDATE {tname} SET "
            f"payload = :pay, amount = :amt, ship_date = SYSDATE, "
            f"status = :st "
            f"WHERE order_id = :pk",
            {
                "pay": _random_payload(),
                "amt": round(random.uniform(10, 5000), 2),
                "st": _random_status(),
                "pk": pk,
            },
        )

    def _do_delete_and_replace(self, cursor, tname: str, table_idx: int) -> None:
        """Delete a non-hot row and insert a replacement."""
        all_r = self._all_rows.get(table_idx, [])
        hot = set(self._hot_rows.get(table_idx, []))
        non_hot = [r for r in all_r if r not in hot]

        if non_hot:
            victim = random.choice(non_hot)
            cursor.execute(
                f"DELETE FROM {tname} WHERE order_id = :pk",
                {"pk": victim},
            )
            # Remove from cache
            if victim in all_r:
                try:
                    all_r.remove(victim)
                except ValueError:
                    pass

        # Insert replacement
        self._do_insert(cursor, tname, table_idx)

    def _do_select(self, cursor, tname: str, table_idx: int) -> None:
        """Select a row by PK to exercise indexed reads without DML."""
        pk = self._pick_target_row(table_idx)
        if pk is None:
            return
        cursor.execute(
            f"SELECT order_id, status, amount "
            f"FROM {tname} "
            f"WHERE order_id = :pk",
            {"pk": pk},
        )
        cursor.fetchone()

    def _do_select_into(self, cursor, tname: str, table_idx: int) -> None:
        """Run an Oracle SELECT INTO via an anonymous PL/SQL block."""
        pk = self._pick_target_row(table_idx)
        if pk is None:
            return
        cursor.execute(
            f"""
            DECLARE
                v_order_id {tname}.order_id%TYPE;
                v_status   {tname}.status%TYPE;
                v_amount   {tname}.amount%TYPE;
            BEGIN
                SELECT order_id, status, amount
                INTO   v_order_id, v_status, v_amount
                FROM   {tname}
                WHERE  order_id = :pk;
            END;
            """,
            {"pk": pk},
        )

    def _do_pedt_like_update(self, cursor, tname: str, table_idx: int) -> None:
        """Run a composite-key update shaped like the PEDT030 example."""
        pk = self._pick_target_row(table_idx)
        if pk is None:
            self._do_insert(cursor, tname, table_idx)
            return

        cursor.execute(
            f"""
            SELECT pecdgent, penumper, peclaseg
            FROM   {tname}
            WHERE  order_id = :pk
            """,
            {"pk": pk},
        )
        row = cursor.fetchone()
        if row is None:
            return

        cursor.execute(
            f"""
            UPDATE {tname}
            SET    pesegcla = :b1,
                   pefecseg = SYSDATE,
                   pesegman = :b2,
                   peusumod = :b3,
                   petermod = :b4,
                   pesucmod = :b5,
                   pesegcal = :b6,
                   pehstamp = :b7
            WHERE  pecdgent = :b8
            AND    penumper = :b9
            AND    peclaseg = :b10
            """,
            {
                "b1": _random_seg_class(),
                "b2": _random_seg_manager(),
                "b3": _random_mod_user(),
                "b4": _random_term_mod(),
                "b5": random.randint(1, 999),
                "b6": _random_seg_calc(),
                "b7": random.randint(100000, 999999999),
                "b8": row[0],
                "b9": row[1],
                "b10": row[2],
            },
        )

    # ---- seed data --------------------------------------------------------

    def _prepare_table_seed(self, table_idx: int) -> None:
        """Prepare one table and cache its row metadata."""
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config
        if self._stop_event.is_set():
            return

        conn = self._pool.acquire()
        cursor = None
        try:
            apply_call_timeout(conn, cfg.call_timeout_ms)
            cursor = conn.cursor()
            tname = f"{cfg.table_prefix}_ORDER_{table_idx:02d}"

            cursor.execute(f"SELECT COUNT(*) FROM {tname}")
            existing = int(cursor.fetchone()[0])
            missing = max(cfg.seed_rows - existing, 0)
            if missing > 0:
                batch_size = 100
                for batch_start in range(0, missing, batch_size):
                    batch_end = min(batch_start + batch_size, missing)
                    rows = []
                    for _ in range(batch_end - batch_start):
                        rows.append((
                            _random_ref(),
                            _random_name(),
                            _random_status(),
                            round(random.uniform(10, 5000), 2),
                            random.randint(1, 100),
                            round(random.uniform(0, 50), 2),
                            _random_payload(100),
                            1000 + random.randint(0, 63),
                            random.randint(1, 10_000_000),
                            random.choice(["SEG_A", "SEG_B", "SEG_C", "SEG_D"]),
                            _random_seg_class(),
                            _random_seg_manager(),
                            _random_mod_user(),
                            _random_term_mod(),
                            random.randint(1, 999),
                            _random_seg_calc(),
                            random.randint(100000, 999999999),
                        ))
                    cursor.executemany(
                        f"INSERT INTO {tname} "
                        f"(order_ref, customer_name, status, amount, quantity, discount, payload, "
                        f"pecdgent, penumper, peclaseg, pesegcla, pefecseg, pesegman, peusumod, petermod, pesucmod, pesegcal, pehstamp) "
                        f"VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, SYSDATE, :12, :13, :14, :15, :16, :17)",
                        rows,
                    )
                conn.commit()

            cursor.execute(
                f"SELECT order_id FROM {tname} "
                f"ORDER BY order_id FETCH FIRST :lim ROWS ONLY",
                {"lim": cfg.seed_rows},
            )
            all_pks = [int(r[0]) for r in cursor.fetchall()]
            self._all_rows[table_idx] = all_pks
            hot_count = max(32, len(all_pks) * cfg.hot_row_pct // 100)
            self._hot_rows[table_idx] = all_pks[:min(len(all_pks), hot_count)]
            if cursor is not None:
                cursor.close()
        finally:
            self._pool.release(conn)

    def _prepare_seed_data_parallel(self, prepare_callback: Optional[Callable[[dict], None]] = None) -> None:
        """Ensure each table has seed rows and cache PKs + hot set in parallel."""
        assert self._config is not None
        cfg = self._config
        total = max(1, cfg.table_count)
        done = 0
        done_lock = threading.Lock()

        def run_one(table_idx: int) -> None:
            nonlocal done
            if self._stop_event.is_set():
                return
            self._prepare_table_seed(table_idx)
            if prepare_callback:
                with done_lock:
                    done += 1
                    self.status.set_phase("PREPARING", f"Prepared table {done}/{total}")
                    prepare_callback(self.status.to_dict(duration=cfg.duration_seconds))

        max_workers = max(1, min(total, 32))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(run_one, range(1, total + 1)))

    def _warm_sessions_parallel(self, prepare_callback: Optional[Callable[[dict], None]] = None) -> None:
        """Open and release worker sessions before the timed workload starts."""
        assert self._pool is not None
        assert self._config is not None
        total = self._worker_count
        warmed = 0
        warmed_lock = threading.Lock()

        def warm_one(_: int) -> None:
            nonlocal warmed
            if self._stop_event.is_set():
                return
            conn = self._pool.acquire()
            try:
                apply_call_timeout(conn, self._config.call_timeout_ms)
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM dual")
                cur.fetchone()
                cur.close()
            finally:
                self._pool.release(conn)
            if prepare_callback:
                with warmed_lock:
                    warmed += 1
                    if warmed == total or warmed % max(1, min(25, total // 8 or 1)) == 0:
                        self.status.set_phase("WARMING", f"Warmed {warmed}/{total} sessions")
                        prepare_callback(self.status.to_dict(duration=self._config.duration_seconds))

        max_workers = max(1, min(total, 64))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(warm_one, range(total)))
