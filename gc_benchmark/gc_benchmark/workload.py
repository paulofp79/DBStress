"""Concurrent workload engine for driving GC contention.

Spawns multiple threads that perform mixed DML (INSERT / UPDATE / DELETE)
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
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import oracledb

_MAX_PHYSICAL_WORKERS = 256


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
    thread_count: int = 8           # 2..32
    duration_seconds: int = 60
    hot_row_pct: int = 5            # 1..10
    seed_rows: int = 500
    commit_batch: int = 10
    insert_pct: int = 40
    update_pct: int = 40
    delete_pct: int = 20
    select_pct: int = 0

    # ---- Contention mode ---------------------------------------------------
    # NORMAL     : mixed DML (INSERT/UPDATE/DELETE) spread across all tables
    # HAMMER     : SELECT FOR UPDATE NOWAIT + UPDATE on the same 10 hot rows
    #              across ALL tables — maximises gc current block congested
    # LMS_STRESS : rapid-fire UPDATEs on just 5 rows in table-1, commit after
    #              every op — highest GC-request rate, designed to overflow the
    #              LMS process queue and force "congested" waits
    contention_mode: str = "NORMAL"   # NORMAL | HAMMER | LMS_STRESS
    lock_hold_ms: int = 0              # ms to hold lock before UPDATE (HAMMER only)


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
    errors: int = 0
    elapsed: float = 0.0
    running: bool = False
    last_error: str = ""
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False,
    )

    def increment(self, op: str, count: int = 1) -> None:
        """Increment the counter for *op* (inserts / updates / deletes)."""
        with self._lock:
            current = getattr(self, op, 0)
            setattr(self, op, current + count)

    def record_error(self, message: str) -> None:
        """Record an error without crashing the worker."""
        with self._lock:
            self.errors += 1
            self.last_error = message

    def to_dict(self, duration: int = 0) -> dict:
        """Return a JSON-serialisable snapshot of the counters."""
        with self._lock:
            return {
                "inserts": self.inserts,
                "updates": self.updates,
                "deletes": self.deletes,
                "selects": self.selects,
                "errors": self.errors,
                "elapsed": round(self.elapsed, 1),
                "running": self.running,
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

    def start(
        self,
        config: WorkloadConfig,
        progress_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """Begin the workload run.

        *progress_callback* is invoked every ~2 s with a status dict.
        """
        if self.is_running:
            raise RuntimeError("Workload already running.")

        self._config = config
        self._worker_count = max(1, min(config.thread_count, _MAX_PHYSICAL_WORKERS))
        self._stop_event.clear()
        self.status = WorkloadStatus(running=True)
        self.status.elapsed = 0.0
        self._start_time = 0.0

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
            increment=max(1, min(8, self._worker_count)),
        )

        # Seed data and identify hot rows
        self._prepare_seed_data()

        # Start the measured workload window only after startup preparation completes.
        self._start_time = time.monotonic()

        # Choose worker function based on contention mode
        mode = config.contention_mode.upper()
        if mode == "HAMMER":
            worker_fn = self._worker_hammer
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
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads.clear()
        if self._start_time:
            self.status.elapsed = time.monotonic() - self._start_time
        self.status.running = False

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
        base = self._hot_rows.get(table_idx, []) or self._all_rows.get(table_idx, [])
        if not base:
            return []
        target = max(minimum_rows, len(base))
        return base[:min(len(base), target)]

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

    def _worker(self) -> None:
        """Single worker thread: mixed DML loop until stop or duration."""
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config

        conn = None
        try:
            conn = self._pool.acquire()
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

                total_mix = max(1, cfg.insert_pct + cfg.update_pct + cfg.delete_pct + cfg.select_pct)
                roll = random.random()
                try:
                    insert_cutoff = cfg.insert_pct / total_mix
                    update_cutoff = (cfg.insert_pct + cfg.update_pct) / total_mix
                    delete_cutoff = (cfg.insert_pct + cfg.update_pct + cfg.delete_pct) / total_mix

                    if roll < insert_cutoff:
                        self._do_insert(cursor, tname, table_idx)
                        self.status.increment("inserts")
                    elif roll < update_cutoff:
                        self._do_update(cursor, tname, table_idx)
                        self.status.increment("updates")
                    elif roll < delete_cutoff:
                        self._do_delete_and_replace(cursor, tname, table_idx)
                        self.status.increment("deletes")
                    else:
                        self._do_select(cursor, tname, table_idx)
                        self.status.increment("selects")

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
        try:
            conn = self._pool.acquire()
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

        All threads hammer rows 1-5 in every table with rapid-fire
        plain UPDATEs (no ``FOR UPDATE`` lock step) and commit after
        every single operation.  This generates the highest possible
        rate of Cache Fusion block-transfer requests per second,
        designed to overflow the LMS receive-queue and reliably produce
        ``gc current block congested`` waits.

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

        # Keep pressure on hot areas, but widen the pool enough that sessions
        # collide on blocks/index leaves more often than on the exact same row.
        stress_rows: dict[int, list[int]] = {
            idx: self._contention_row_pool(idx, 256)
            for idx in range(1, cfg.table_count + 1)
        }
        slot = self._worker_slot()

        conn = None
        try:
            conn = self._pool.acquire()
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
                pk = random.choice(self._thread_row_window(rows, slot, 24))
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
            f"(order_ref, customer_name, status, order_date, amount, quantity, discount, payload) "
            f"VALUES (:ref, :cust, :st, SYSDATE - DBMS_RANDOM.VALUE(0, 365), "
            f":amt, :qty, :disc, :pay)",
            {
                "ref": _random_ref(),
                "cust": _random_name(),
                "st": _random_status(),
                "amt": round(random.uniform(10, 5000), 2),
                "qty": random.randint(1, 100),
                "disc": round(random.uniform(0, 50), 2),
                "pay": _random_payload(),
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

    # ---- seed data --------------------------------------------------------

    def _prepare_seed_data(self) -> None:
        """Ensure each table has seed rows and cache PKs + hot set."""
        assert self._config is not None
        assert self._pool is not None
        cfg = self._config

        conn = self._pool.acquire()
        try:
            cursor = conn.cursor()
            for table_idx in range(1, cfg.table_count + 1):
                tname = f"{cfg.table_prefix}_ORDER_{table_idx:02d}"

                # Count existing rows
                cursor.execute(f"SELECT COUNT(*) FROM {tname}")
                existing = int(cursor.fetchone()[0])

                # Seed if needed
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
                            ))
                        cursor.executemany(
                            f"INSERT INTO {tname} "
                            f"(order_ref, customer_name, status, amount, quantity, discount, payload) "
                            f"VALUES (:1, :2, :3, :4, :5, :6, :7)",
                            rows,
                        )
                    conn.commit()

                # Cache all PKs
                cursor.execute(
                    f"SELECT order_id FROM {tname} "
                    f"ORDER BY order_id FETCH FIRST :lim ROWS ONLY",
                    {"lim": cfg.seed_rows},
                )
                all_pks = [int(r[0]) for r in cursor.fetchall()]
                self._all_rows[table_idx] = all_pks

                # Identify hot rows. Keep a broader hot set so workloads
                # contend on blocks/index leaves without over-targeting one row.
                hot_count = max(32, len(all_pks) * cfg.hot_row_pct // 100)
                self._hot_rows[table_idx] = all_pks[:min(len(all_pks), hot_count)]

            cursor.close()
        finally:
            self._pool.release(conn)
