"""Repeated login/query/logout workload engine.

Each worker thread repeatedly:
1. opens a fresh Oracle connection
2. runs a simple SQL query
3. fetches one row
4. closes the connection

This is designed to simulate large numbers of short-lived application
sessions repeatedly logging on and off the database.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import oracledb
from oracle_session import DEFAULT_CALL_TIMEOUT_MS, apply_call_timeout

_MAX_LOGIN_WORKERS = 64


@dataclass
class LoginWorkloadConfig:
    """Parameters for a login/query/logout workload run."""

    dsn: str
    user: str
    password: str
    mode: str = "thin"
    sql_text: str = "select 1 from dual"
    thread_count: int = 20
    stop_mode: str = "CYCLES"        # CYCLES | DURATION | MANUAL
    iterations_per_thread: int = 1000  # 0 = run until stopped
    duration_seconds: int = 0
    think_time_ms: int = 0
    call_timeout_ms: int = DEFAULT_CALL_TIMEOUT_MS


@dataclass
class LoginWorkloadStatus:
    """Thread-safe live counters for login workload execution."""

    logons: int = 0
    queries: int = 0
    logouts: int = 0
    cycles: int = 0
    errors: int = 0
    active_connections: int = 0
    elapsed: float = 0.0
    running: bool = False
    phase: str = "IDLE"
    status_message: str = ""
    last_error: str = ""
    target_cycles: int = 0
    target_seconds: int = 0
    avg_cycle_ms: float = 0.0
    _total_cycle_ms: float = field(default=0.0, init=False, repr=False)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False,
    )

    def set_phase(self, phase: str, message: str = "") -> None:
        with self._lock:
            self.phase = phase
            self.status_message = message

    def record_logon(self) -> None:
        with self._lock:
            self.logons += 1

    def record_query(self) -> None:
        with self._lock:
            self.queries += 1

    def record_logout(self) -> None:
        with self._lock:
            self.logouts += 1

    def add_active(self, delta: int) -> None:
        with self._lock:
            self.active_connections = max(0, self.active_connections + delta)

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors += 1
            self.last_error = message

    def record_cycle(self, cycle_ms: float) -> None:
        with self._lock:
            self.cycles += 1
            self._total_cycle_ms += max(0.0, cycle_ms)
            if self.cycles > 0:
                self.avg_cycle_ms = self._total_cycle_ms / self.cycles

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "logons": self.logons,
                "queries": self.queries,
                "logouts": self.logouts,
                "cycles": self.cycles,
                "errors": self.errors,
                "active_connections": self.active_connections,
                "elapsed": round(self.elapsed, 1),
                "running": self.running,
                "phase": self.phase,
                "status_message": self.status_message,
                "last_error": self.last_error,
                "target_cycles": self.target_cycles,
                "target_seconds": self.target_seconds,
                "avg_cycle_ms": round(self.avg_cycle_ms, 2),
            }


class LoginWorkloadRunner:
    """Manages worker threads for repeated login/query/logout cycles."""

    def __init__(self) -> None:
        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._config: Optional[LoginWorkloadConfig] = None
        self._worker_count: int = 0
        self._start_time: float = 0.0
        self._remaining_workers: int = 0
        self._remaining_lock = threading.Lock()
        self.status = LoginWorkloadStatus()

    @property
    def is_running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    def start(
        self,
        config: LoginWorkloadConfig,
        progress_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """Start the login workload."""
        if self.is_running:
            raise RuntimeError("Login workload already running.")

        self._config = config
        self._worker_count = max(1, min(config.thread_count, _MAX_LOGIN_WORKERS))
        self._remaining_workers = self._worker_count
        self._stop_event.clear()
        self._start_time = time.monotonic()

        stop_mode = str(config.stop_mode or "CYCLES").upper()
        target_cycles = 0
        target_seconds = 0
        if stop_mode == "CYCLES" and config.iterations_per_thread > 0:
            target_cycles = self._worker_count * config.iterations_per_thread
        elif stop_mode == "DURATION" and config.duration_seconds > 0:
            target_seconds = config.duration_seconds

        self.status = LoginWorkloadStatus(
            running=True,
            phase="RUNNING",
            status_message="Login/query/logout workload running.",
            target_cycles=target_cycles,
            target_seconds=target_seconds,
        )

        if config.mode.lower() == "thick":
            try:
                if oracledb.is_thin_mode():
                    oracledb.init_oracle_client()
            except Exception:
                pass

        for idx in range(self._worker_count):
            thread = threading.Thread(
                target=self._worker,
                args=(idx,),
                name=f"login-sim-worker-{idx}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

        if progress_callback:
            reporter = threading.Thread(
                target=self._reporter,
                args=(progress_callback,),
                name="login-sim-reporter",
                daemon=True,
            )
            self._threads.append(reporter)
            reporter.start()

    def stop(self, timeout: float = 10.0) -> dict:
        """Signal the workers to stop and wait for them to finish."""
        if self.status.running:
            self.status.set_phase(
                "STOPPING",
                "Waiting for workers to finish current login/query/logout cycle...",
            )
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
        if self.status.phase not in ("COMPLETE", "STOPPED"):
            self.status.set_phase("STOPPED", "Login workload stopped.")
        return self.status.to_dict()

    def _reporter(self, callback: Callable[[dict], None]) -> None:
        """Emit periodic progress snapshots."""
        while not self._stop_event.is_set():
            if self._start_time:
                self.status.elapsed = time.monotonic() - self._start_time
            callback(self.status.to_dict())
            self._stop_event.wait(timeout=1.0)
        if self._start_time:
            self.status.elapsed = time.monotonic() - self._start_time
        callback(self.status.to_dict())

    def _worker_finished(self) -> None:
        """Track worker shutdown and mark the run complete when all exit."""
        with self._remaining_lock:
            self._remaining_workers = max(0, self._remaining_workers - 1)
            all_done = self._remaining_workers == 0

        if not all_done:
            return

        if self._start_time:
            self.status.elapsed = time.monotonic() - self._start_time
        self.status.running = False
        if self.status.phase not in ("STOPPING", "STOPPED"):
            self.status.set_phase("COMPLETE", "All login workload workers completed.")
        self._stop_event.set()

    def _worker(self, worker_idx: int) -> None:
        """Repeatedly connect, execute one query, and disconnect."""
        assert self._config is not None
        cfg = self._config
        iteration = 0

        try:
            while not self._stop_event.is_set():
                elapsed = time.monotonic() - self._start_time if self._start_time else 0.0
                stop_mode = str(cfg.stop_mode or "CYCLES").upper()
                if stop_mode == "DURATION" and cfg.duration_seconds > 0 and elapsed >= cfg.duration_seconds:
                    self._stop_event.set()
                    break
                if stop_mode == "CYCLES" and cfg.iterations_per_thread > 0 and iteration >= cfg.iterations_per_thread:
                    break
                iteration += 1

                conn = None
                cur = None
                cycle_start = time.perf_counter()

                try:
                    conn = oracledb.connect(
                        user=cfg.user,
                        password=cfg.password,
                        dsn=cfg.dsn,
                    )
                    apply_call_timeout(conn, cfg.call_timeout_ms)
                    self.status.add_active(1)
                    self.status.record_logon()

                    try:
                        conn.module = "LOGIN_WORKLOAD_SIM"
                        conn.action = f"worker-{worker_idx}"
                    except Exception:
                        pass

                    cur = conn.cursor()
                    cur.arraysize = 1
                    cur.execute(cfg.sql_text)
                    cur.fetchmany(1)
                    self.status.record_query()
                except Exception as exc:
                    self.status.record_error(str(exc))
                finally:
                    if cur is not None:
                        try:
                            cur.close()
                        except Exception:
                            pass
                    if conn is not None:
                        try:
                            conn.close()
                            self.status.record_logout()
                        except Exception as exc:
                            self.status.record_error(f"Close failed: {exc}")
                        finally:
                            self.status.add_active(-1)

                    cycle_ms = (time.perf_counter() - cycle_start) * 1000.0
                    self.status.record_cycle(cycle_ms)

                if cfg.think_time_ms > 0 and not self._stop_event.is_set():
                    if self._stop_event.wait(cfg.think_time_ms / 1000.0):
                        break
        finally:
            self._worker_finished()
