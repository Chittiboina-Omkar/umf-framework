"""Reading and advancing watermarks.

A watermark is the marker an incremental or CDC load uses to remember how far it
has already read (for example the maximum ``updated_at`` seen so far). The single
most important rule about watermarks is:

    A watermark is advanced only after the step has succeeded and
    reconciliation has passed.

If a watermark were advanced before the data was safely committed, a failure
would silently skip the un-committed rows on the next run, losing data. By
advancing only on success, a failed run simply re-reads the same window next
time, which is safe and idempotent.

This module defines the :class:`WatermarkStore` interface and a portable,
file-backed default. A table-backed implementation (writing to the ``watermark``
control table) is added in a later part; it will implement the same interface,
so nothing that uses a watermark store needs to change.

Every change is returned as a :class:`WatermarkChange` so the caller can record
it in the ``watermark_log`` for a full audit of when and why a watermark moved.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


@dataclass(frozen=True)
class WatermarkChange:
    """A record of a watermark moving from one value to another.

    Attributes:
        table_key: The table whose watermark changed.
        old_value: The previous value, or ``None`` if there was none.
        new_value: The new value.
        run_id: The run that advanced the watermark.
        changed_at: When the change happened, in UTC ISO format.
        reason: A short, non-sensitive explanation.
    """

    table_key: str
    old_value: Optional[str]
    new_value: str
    run_id: str
    changed_at: str
    reason: str


class WatermarkStore:
    """Interface for reading and advancing watermarks.

    Implementations must make :meth:`advance` safe to call repeatedly with the
    same inputs (idempotent) and must never move a watermark backwards unless
    explicitly reset.
    """

    def get(self, table_key: str) -> Optional[str]:
        """Return the current watermark for a table, or ``None`` if unset.

        Args:
            table_key: The table's stable key.

        Returns:
            The current watermark value as text, or ``None`` for a first run.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def advance(
        self,
        table_key: str,
        new_value: str,
        *,
        run_id: str,
        watermark_column: Optional[str] = None,
        reason: str = "step succeeded and reconciliation passed",
    ) -> WatermarkChange:
        """Advance a watermark, returning a record of the change.

        The caller must only invoke this after the step has succeeded and
        reconciliation has passed. The store refuses to move a watermark
        backwards (a smaller value than the current one) to protect against a
        late, out-of-order call; use :meth:`reset` for a deliberate rewind.

        Args:
            table_key: The table's stable key.
            new_value: The new watermark value, as text.
            run_id: The run advancing the watermark.
            watermark_column: The source column tracked, recorded for audit.
            reason: A short, non-sensitive reason for the change.

        Returns:
            A :class:`WatermarkChange` describing the move.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def reset(
        self, table_key: str, new_value: Optional[str], *, run_id: str, reason: str
    ) -> WatermarkChange:
        """Deliberately set (or clear) a watermark, bypassing the forward check.

        Resetting is a privileged operation (for example to re-baseline a table)
        and should require approval and be audited by the caller.

        Args:
            table_key: The table's stable key.
            new_value: The value to set, or ``None`` to clear the watermark.
            run_id: The run performing the reset.
            reason: A short, non-sensitive reason.

        Returns:
            A :class:`WatermarkChange` describing the reset.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class InMemoryWatermarkStore(WatermarkStore):
    """A watermark store kept only in memory. Intended for unit tests.

    It implements the full interface, including the no-backward-move rule, so
    tests exercise the real semantics without touching disk.
    """

    def __init__(self) -> None:
        """Create an empty in-memory store."""
        self._values: Dict[str, str] = {}
        self._lock = threading.Lock()

    def get(self, table_key: str) -> Optional[str]:
        """Return the current watermark or ``None``.

        Args:
            table_key: The table's stable key.

        Returns:
            The current value, or ``None`` if unset.
        """
        with self._lock:
            return self._values.get(table_key)

    def advance(
        self,
        table_key: str,
        new_value: str,
        *,
        run_id: str,
        watermark_column: Optional[str] = None,
        reason: str = "step succeeded and reconciliation passed",
    ) -> WatermarkChange:
        """Advance the watermark, refusing to move it backwards.

        Args:
            table_key: The table's stable key.
            new_value: The new value.
            run_id: The advancing run.
            watermark_column: The tracked column (unused here, recorded by the
                caller).
            reason: A short reason.

        Returns:
            The :class:`WatermarkChange`.
        """
        with self._lock:
            old = self._values.get(table_key)
            if old is not None and str(new_value) < str(old):
                # Comparing as text is correct for sortable formats (ISO
                # timestamps, zero-padded ids). For purely numeric watermarks the
                # caller should zero-pad or use a numeric store variant; the
                # safe direction here is to refuse a suspicious backward move.
                return WatermarkChange(
                    table_key=table_key,
                    old_value=old,
                    new_value=old,  # unchanged
                    run_id=run_id,
                    changed_at=_now(),
                    reason="refused backward move; watermark left unchanged",
                )
            self._values[table_key] = new_value
            return WatermarkChange(
                table_key=table_key,
                old_value=old,
                new_value=new_value,
                run_id=run_id,
                changed_at=_now(),
                reason=reason,
            )

    def reset(
        self, table_key: str, new_value: Optional[str], *, run_id: str, reason: str
    ) -> WatermarkChange:
        """Set or clear the watermark, bypassing the forward-only check.

        Args:
            table_key: The table's stable key.
            new_value: The value to set, or ``None`` to clear.
            run_id: The run performing the reset.
            reason: A short reason.

        Returns:
            The :class:`WatermarkChange`.
        """
        with self._lock:
            old = self._values.get(table_key)
            if new_value is None:
                self._values.pop(table_key, None)
            else:
                self._values[table_key] = new_value
            return WatermarkChange(
                table_key=table_key,
                old_value=old,
                new_value="" if new_value is None else new_value,
                run_id=run_id,
                changed_at=_now(),
                reason=reason,
            )


class FileWatermarkStore(InMemoryWatermarkStore):
    """A watermark store backed by a single JSON file.

    This is the portable default: it works on any platform and survives process
    restarts. It extends :class:`InMemoryWatermarkStore` so the forward-only and
    reset semantics are identical; it simply loads from and saves to disk around
    each operation.

    Concurrency note:
        File-level writes are serialised with a lock within a process. True
        multi-writer concurrency (two separate jobs) is handled by the
        table-backed store and the locking mechanism added later; this default
        targets single-writer use and development.

    Attributes:
        path: The JSON file that holds the watermarks.
    """

    def __init__(self, path: str) -> None:
        """Create the store and load any existing values.

        Args:
            path: The JSON file to use. Its parent directory is created if
                needed.
        """
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        """Load values from the file, tolerating a missing or empty file."""
        if self.path.exists():
            try:
                self._values = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                # A missing or corrupt file means "no watermarks yet", which is
                # the safe default: the next run re-reads from the beginning.
                self._values = {}

    def _save(self) -> None:
        """Persist values atomically by writing to a temp file then renaming."""
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._values, indent=2), encoding="utf-8")
        # ``replace`` is atomic on the same filesystem, so a crash never leaves a
        # half-written watermark file.
        tmp.replace(self.path)

    def advance(self, table_key: str, new_value: str, **kwargs: object) -> WatermarkChange:
        """Advance and persist the watermark.

        Args:
            table_key: The table's stable key.
            new_value: The new value.
            **kwargs: Passed through to the in-memory implementation.

        Returns:
            The :class:`WatermarkChange`.
        """
        change = super().advance(table_key, new_value, **kwargs)  # type: ignore[arg-type]
        self._save()
        return change

    def reset(self, table_key: str, new_value: Optional[str], **kwargs: object) -> WatermarkChange:
        """Reset and persist the watermark.

        Args:
            table_key: The table's stable key.
            new_value: The value to set, or ``None`` to clear.
            **kwargs: Passed through to the in-memory implementation.

        Returns:
            The :class:`WatermarkChange`.
        """
        change = super().reset(table_key, new_value, **kwargs)  # type: ignore[arg-type]
        self._save()
        return change


def _now() -> str:
    """Return the current UTC time as an ISO 8601 string.

    Returns:
        The current UTC time.
    """
    return datetime.now(timezone.utc).isoformat()
