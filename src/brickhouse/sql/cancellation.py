"""Execution-scoped cancellation for local SQL statements."""

from __future__ import annotations

import time
from collections.abc import Callable
from threading import Event, Lock, Thread


class SqlStatementCancelledError(RuntimeError):
    """A SQL statement reached a safe terminal cancellation outcome."""


class SqlStatementControl:
    """Coordinate cancellation requests with native execution and table commits.

    A request never changes persisted statement state by itself. Active native
    work must report cancellation, or execution must reach a safe point before
    its first side effect, before the caller can persist ``CANCELED``.
    """

    def __init__(self, *, commit_guard: Callable[[], bool] | None = None) -> None:
        self._lock = Lock()
        self._phase = "queued"
        self._cancel_requested = False
        self._cancel_message = "SQL statement cancellation requested"
        self._cancel_active_actions: Callable[[], int] | None = None
        self._commit_guard = commit_guard
        self._action_done = Event()
        self._action_done.set()
        self._cancel_monitor: Thread | None = None

    @property
    def cancellation_requested(self) -> bool:
        with self._lock:
            return self._cancel_requested

    def mark_preparing(self) -> None:
        with self._lock:
            if self._cancel_requested:
                self._phase = "terminal"
                raise SqlStatementCancelledError(self._cancel_message)
            self._phase = "preparing"

    def request_cancel(self, message: str = "SQL statement cancellation requested") -> bool:
        """Request cancellation, without claiming that execution has stopped."""
        with self._lock:
            if self._phase in {"committing", "terminal"}:
                return False
            self._cancel_requested = True
            self._cancel_message = message
            if self._phase == "executing" and self._cancel_active_actions is not None:
                self._start_cancel_monitor_locked()
            return True

    def install_engine_canceller(self, callback: Callable[[], int]) -> None:
        """Install the native session's thread-safe, execution-scoped handle."""
        with self._lock:
            if self._phase in {"committing", "terminal"}:
                return
            self._cancel_active_actions = callback

    def begin_engine_action(self) -> None:
        with self._lock:
            if self._cancel_requested:
                self._phase = "terminal"
                raise SqlStatementCancelledError(self._cancel_message)
            self._phase = "executing"
            self._action_done.clear()

    def finish_engine_action(self, *, commit_follows: bool) -> None:
        with self._lock:
            self._action_done.set()
            self._cancel_active_actions = None
            self._phase = "precommit" if commit_follows else "terminal"

    def fail_engine_action(self) -> None:
        with self._lock:
            self._action_done.set()
            self._cancel_active_actions = None
            self._phase = "terminal"

    def begin_commit(self) -> None:
        """Fence cancellation before catalog/table side effects start."""
        guard = self._commit_guard
        if guard is not None and not guard():
            with self._lock:
                self._cancel_requested = True
                self._cancel_message = "SQL warehouse stopped before the statement commit began"
        with self._lock:
            if self._cancel_requested:
                self._phase = "terminal"
                raise SqlStatementCancelledError(self._cancel_message)
            self._phase = "committing"
            self._cancel_active_actions = None

    def mark_terminal(self) -> None:
        with self._lock:
            self._phase = "terminal"
            self._cancel_active_actions = None
            self._action_done.set()

    def _start_cancel_monitor_locked(self) -> None:
        monitor = self._cancel_monitor
        if monitor is not None and monitor.is_alive():
            return
        monitor = Thread(
            target=self._cancel_until_accepted_or_terminal,
            name="brickhouse-sql-cancel",
            daemon=True,
        )
        self._cancel_monitor = monitor
        monitor.start()

    def _cancel_until_accepted_or_terminal(self) -> None:
        while not self._action_done.wait(timeout=0.01):
            with self._lock:
                if self._phase != "executing" or not self._cancel_requested:
                    return
                callback = self._cancel_active_actions
            if callback is None:
                return
            try:
                accepted = callback()
            except Exception:
                # Keep the statement running and retain its lease. The worker's
                # actual terminal result decides whether cancellation succeeded.
                time.sleep(0.01)
                continue
            if type(accepted) is int and accepted > 0:
                return
