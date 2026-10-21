"""Where structured log and control records are written.

Part 1 configured in-process logging (human-readable JSON lines on the console).
This module is about *persisting* structured records to a durable destination so
they can be queried later: the ``run_log``, ``step_log``, ``recon_results`` and
the other tables defined in :mod:`umf.control.table_defs`.

The design separates the act of emitting a record from the choice of where it
goes, via the :class:`LogSink` interface. This matters for two reasons:

    * Portability. On a platform with Spark and a catalog, the records go to
      Delta or Iceberg tables (that sink is added in a later part). With no such
      backend, or as a guaranteed fallback, the same records go to append-only
      JSON Lines files. The emitting code does not change.
    * Crash-safety. A sink write must never bring down the pipeline. If the
      primary sink (for example a table) fails, the failure itself is logged and
      a fallback sink (a local file) still captures the record, so the audit
      trail survives even a logging outage.

Everything here is standard library only, so the default file sink works on
every platform and in tests.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from umf.core.logging_setup import get_logger
from umf.core.masking import mask_structure

_log = get_logger("control.log_sink")


class LogSink:
    """Interface for a destination that persists structured records.

    A record is a flat dictionary whose keys match a table's columns (see
    :mod:`umf.control.records`). Implementations must be safe to call from the
    pipeline hot path: a write failure must be handled, not raised, so logging
    never breaks data processing.
    """

    def write(self, table: str, record: Dict[str, Any]) -> None:
        """Persist one record to the named table.

        Args:
            table: The logical table name (for example ``"run_log"``).
            record: A flat dictionary of column name to value.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Flush and release any resources held by the sink.

        The default does nothing; file and table sinks override it.
        """
        return None


class NullSink(LogSink):
    """A sink that discards everything. Useful in unit tests."""

    def write(self, table: str, record: Dict[str, Any]) -> None:
        """Discard the record.

        Args:
            table: Ignored.
            record: Ignored.
        """
        return None


class ConsoleSink(LogSink):
    """A sink that writes each record as a JSON line to the framework logger.

    This reuses the Part 1 logging backbone, so records show up in the platform
    driver / job logs. It is handy during development and as a secondary sink
    that is almost impossible to fail.
    """

    def write(self, table: str, record: Dict[str, Any]) -> None:
        """Log the record as a structured info line.

        Args:
            table: The logical table name, attached as a field.
            record: The record to log (masked before emission).
        """
        _log.info("record", extra={"table": table, "record": mask_structure(record)})


class JsonlFileSink(LogSink):
    """A sink that appends records to per-table JSON Lines files.

    Each table gets its own file, ``<root>/<table>.jsonl``, and each record is
    one JSON object on its own line. Appending a single line is about as
    crash-safe as file writes get: a crash mid-write can at worst leave one
    trailing partial line, and every prior record is intact and readable.

    This is the portable default and the fallback for the table-backed sink.
    Writes are guarded by a lock so concurrent threads do not interleave lines.

    Attributes:
        root: Directory that holds the per-table files.
    """

    def __init__(self, root: str) -> None:
        """Create the sink and ensure the target directory exists.

        Args:
            root: Directory to write the per-table files into. Created if it
                does not exist.
        """
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        # One lock per sink instance; file appends are short, so a single lock
        # is simpler and safe. Cross-process safety is provided by the fact that
        # appends of a single line are atomic on common filesystems for small
        # writes; the table-backed sink handles true concurrency later.
        self._lock = threading.Lock()

    def _path(self, table: str) -> Path:
        """Return the file path for a table.

        Args:
            table: The logical table name.

        Returns:
            The path of the table's JSON Lines file.
        """
        return self.root / f"{table}.jsonl"

    def write(self, table: str, record: Dict[str, Any]) -> None:
        """Append one record as a JSON line.

        The record is masked for secrets, stamped with a write time if it has no
        timestamp, and serialised with ``default=str`` so no value type can make
        the write fail.

        Args:
            table: The logical table name.
            record: The record to append.
        """
        safe = mask_structure(record)
        safe.setdefault("_written_at", datetime.now(timezone.utc).isoformat())
        line = json.dumps(safe, default=str, ensure_ascii=False)
        with self._lock:
            # Open in append mode per write. This is simple and robust: the OS
            # keeps the file consistent, and we are not writing at a rate where
            # keeping handles open would matter for these diagnostic tables.
            with self._path(table).open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())

    def read_all(self, table: str) -> List[Dict[str, Any]]:
        """Read every record from a table's file.

        Provided for tests and for small diagnostic reads. Not intended for
        large-scale querying, which belongs to the table-backed sink.

        Args:
            table: The logical table name.

        Returns:
            The list of records, oldest first. An empty list if the file does
            not exist. A trailing partial line (from a crash mid-write) is
            skipped rather than raising.
        """
        path = self._path(table)
        if not path.exists():
            return []
        records: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # A partial trailing line from an interrupted write. Every
                    # complete line before it is valid, so we stop cleanly.
                    break
        return records


class MultiSink(LogSink):
    """A sink that fans each record out to several sinks, with isolation.

    Used to send records to a primary sink (for example a table) and a fallback
    sink (a file) at the same time. A failure in one sink is logged and does not
    stop the others, so one destination going down never costs you the record in
    the others and never breaks the pipeline.

    Attributes:
        sinks: The underlying sinks, written to in order.
    """

    def __init__(self, sinks: List[LogSink]) -> None:
        """Create a multi-sink.

        Args:
            sinks: The sinks to fan out to.
        """
        self.sinks = list(sinks)

    def write(self, table: str, record: Dict[str, Any]) -> None:
        """Write the record to every underlying sink, isolating failures.

        Args:
            table: The logical table name.
            record: The record to write.
        """
        for sink in self.sinks:
            try:
                sink.write(table, record)
            except Exception as exc:  # isolation: one sink must not break others
                # Report the failure through the logger but never re-raise, so
                # that persistence problems cannot abort data processing.
                _log.warning(
                    "log sink write failed",
                    extra={
                        "table": table,
                        "sink": type(sink).__name__,
                        "error": repr(exc),
                    },
                )

    def close(self) -> None:
        """Close every underlying sink, isolating failures."""
        for sink in self.sinks:
            try:
                sink.close()
            except Exception as exc:  # pragma: no cover - defensive
                _log.warning(
                    "log sink close failed",
                    extra={"sink": type(sink).__name__, "error": repr(exc)},
                )
