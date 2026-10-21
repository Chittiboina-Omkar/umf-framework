"""The table writer interface, shared by Delta and Iceberg.

A :class:`TableWriter` is how the engine creates tables and gets data into them,
without knowing or caring whether the format is Delta or Iceberg. The staging
and commit orchestration (in :mod:`umf.engine.staging`) is written entirely
against this interface, so the same control flow (stage, reconcile, then publish
atomically, or discard) works for both formats and for any future format that
implements the interface.

Every method that changes a table is expected to be atomic at the level of a
single call, because both Delta and Iceberg commit a single write or merge as
one transaction. That property is what the orchestration relies on to guarantee
"no partial data": the publish is one transaction that either fully applies or
does not apply at all.

The interface itself is pure Python. Concrete writers import Spark lazily.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

from umf.core.constants import _StrEnum
from umf.engine.identifiers import TableIdentifier


class CommitMode(_StrEnum):
    """How staged data is published into the target table.

    OVERWRITE  replace the target's contents entirely (a full snapshot).
    APPEND     add the staged rows to the target (raw append, facts).
    MERGE      upsert the staged rows into the target on a key (curated upsert).
    """

    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"


@dataclass(frozen=True)
class CommitResult:
    """The outcome of publishing staged data into a target.

    Attributes:
        mode: The commit mode used.
        rows_written: Total rows written, when the writer can report it.
        rows_inserted: Rows inserted by a merge, when applicable.
        rows_updated: Rows updated by a merge, when applicable.
        rows_deleted: Rows deleted, when applicable.
    """

    mode: CommitMode
    rows_written: Optional[int] = None
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None


class TableWriter:
    """Interface for creating tables and writing data in a specific format.

    Implementations must make :meth:`publish` atomic: after a successful call the
    target reflects the staged data fully; after a failed call the target is
    unchanged. The orchestration depends on this to guarantee no partial writes.
    """

    #: The format this writer handles, for diagnostics.
    table_format: str = ""

    def table_exists(self, ident: TableIdentifier) -> bool:
        """Return whether the target table already exists.

        Args:
            ident: The table identifier.

        Returns:
            True if the table exists.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def create_table(
        self,
        ident: TableIdentifier,
        columns: Sequence[Tuple[str, str]],
        *,
        partition_by: Sequence[str] = (),
        properties: Optional[dict] = None,
    ) -> None:
        """Create the target table from a resolved column list.

        Args:
            ident: The table identifier.
            columns: The columns as ``(name, type_string)`` pairs, as produced
                by the type mapping engine.
            partition_by: Columns to partition by.
            properties: Optional table properties.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def write_staging(self, df: Any, staging: TableIdentifier) -> None:
        """Write a DataFrame into a staging table, replacing any prior content.

        Staging is where incoming data lands before reconciliation. Writing it
        as its own table means the target is never touched until the final
        publish.

        Args:
            df: The Spark DataFrame to stage.
            staging: The staging table identifier.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def row_count(self, ident: TableIdentifier) -> int:
        """Return the number of rows in a table.

        Used by reconciliation to compare counts.

        Args:
            ident: The table identifier.

        Returns:
            The row count.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def publish(
        self,
        staging: TableIdentifier,
        target: TableIdentifier,
        mode: CommitMode,
        *,
        keys: Sequence[str] = (),
        ordering: Optional[str] = None,
    ) -> CommitResult:
        """Publish staged data into the target atomically.

        Args:
            staging: The staging table holding the validated data.
            target: The target table to publish into.
            mode: The :class:`CommitMode`.
            keys: The merge keys, required for ``MERGE``.
            ordering: The ordering column that decides the latest row per key,
                used by ``MERGE`` to keep the newest.

        Returns:
            A :class:`CommitResult` describing what was written.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def drop(self, ident: TableIdentifier) -> None:
        """Drop a table if it exists (used to clean up staging).

        Args:
            ident: The table identifier.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError
