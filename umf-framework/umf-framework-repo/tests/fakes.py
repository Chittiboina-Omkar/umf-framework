"""Shared test doubles.

These let the orchestration and staging logic be tested without Spark. The fake
table writer records the operations performed and reports whether the target was
published, which is how the "no partial data on recon fail" guarantee is checked.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

from umf.adapters.table_format.base import CommitMode, CommitResult, TableWriter
from umf.engine.identifiers import TableIdentifier


class FakeWriter(TableWriter):
    """A table writer that records operations instead of touching data.

    Attributes:
        ops: The sequence of operations performed, as (name, ...) tuples.
        staged_rows: The row count the staging table reports.
        tables: The set of tables that "exist".
        published: Whether the target was published.
    """

    table_format = "FAKE"

    def __init__(self, staged_rows: int = 0) -> None:
        """Create the fake writer.

        Args:
            staged_rows: The row count the staging table reports.
        """
        self.ops: List[Tuple[str, ...]] = []
        self.staged_rows = staged_rows
        self.tables: set = set()
        self.published = False

    def table_exists(self, ident: TableIdentifier) -> bool:
        return ident.qualified() in self.tables

    def create_table(self, ident, columns, *, partition_by=(), properties=None) -> None:
        self.ops.append(("create", ident.qualified()))
        self.tables.add(ident.qualified())

    def write_staging(self, df: Any, staging: TableIdentifier) -> None:
        self.ops.append(("stage", staging.qualified()))
        self.tables.add(staging.qualified())

    def row_count(self, ident: TableIdentifier) -> int:
        return self.staged_rows

    def publish(self, staging, target, mode, *, keys=(), ordering=None) -> CommitResult:
        self.ops.append(("PUBLISH", target.qualified(), str(mode)))
        self.published = True
        return CommitResult(mode=mode, rows_written=self.staged_rows)

    def drop(self, ident: TableIdentifier) -> None:
        self.ops.append(("drop", ident.qualified()))

    def op_names(self) -> List[str]:
        """Return just the operation names, for concise assertions."""
        return [o[0] for o in self.ops]
