"""Delta Lake implementation of the table writer.

Implements :class:`~umf.adapters.table_format.base.TableWriter` for Delta. All
PySpark use is lazy so the module imports without Spark. The implementation
leans on Spark SQL where possible, because SQL statements (``CREATE TABLE``,
``INSERT OVERWRITE``, ``MERGE INTO``) are the most portable way to express
atomic Delta operations and are easy to audit.

Atomicity:
    Each of overwrite, append and merge is a single Delta transaction, so the
    target either reflects the operation fully or not at all. The staging table
    is a normal Delta table that holds the validated data until the publish.

This code is written carefully but cannot be executed without a Spark runtime,
so it must be validated in your environment before production use, especially
the merge path, whose exact SQL depends on your Delta and Spark versions.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence, Tuple

from umf.adapters.table_format.base import CommitMode, CommitResult, TableWriter
from umf.core.exceptions import UmfError
from umf.core.logging_setup import get_logger
from umf.engine.identifiers import TableIdentifier

_log = get_logger("adapters.table_format.delta")


class DeltaTableWriter(TableWriter):
    """Writes data into Delta tables."""

    table_format = "DELTA"

    def __init__(self, spark: Optional[Any] = None) -> None:
        """Create the writer.

        Args:
            spark: An existing Spark session to use. If ``None``, the session is
                obtained lazily from the L0 provider on first use, which is the
                normal path on a platform.
        """
        self._spark = spark

    def _session(self) -> Any:
        """Return the Spark session, obtaining it lazily if needed."""
        if self._spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            self._spark = get_or_create_spark()
        return self._spark

    def table_exists(self, ident: TableIdentifier) -> bool:
        """Return whether the Delta table exists.

        Args:
            ident: The table identifier.

        Returns:
            True if the catalog knows the table.
        """
        return bool(self._session().catalog.tableExists(ident.qualified()))

    def create_table(
        self,
        ident: TableIdentifier,
        columns: Sequence[Tuple[str, str]],
        *,
        partition_by: Sequence[str] = (),
        properties: Optional[dict] = None,
    ) -> None:
        """Create a Delta table from a resolved column list.

        Args:
            ident: The table identifier.
            columns: ``(name, type_string)`` pairs from the type engine.
            partition_by: Columns to partition by.
            properties: Optional table properties.
        """
        cols_sql = ", ".join(f"{name} {type_string}" for name, type_string in columns)
        clauses = [f"CREATE TABLE IF NOT EXISTS {ident.qualified()} ({cols_sql})",
                   "USING DELTA"]
        if partition_by:
            clauses.append(f"PARTITIONED BY ({', '.join(partition_by)})")
        if properties:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
            clauses.append(f"TBLPROPERTIES ({props})")
        sql = " ".join(clauses)
        _log.info("create delta table", extra={"table": ident.qualified()})
        self._session().sql(sql)

    def write_staging(self, df: Any, staging: TableIdentifier) -> None:
        """Overwrite a Delta staging table with the DataFrame's contents.

        Args:
            df: The Spark DataFrame to stage.
            staging: The staging table identifier.
        """
        _log.info("write delta staging", extra={"table": staging.qualified()})
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(staging.qualified())
        )

    def row_count(self, ident: TableIdentifier) -> int:
        """Return the row count of a Delta table.

        Args:
            ident: The table identifier.

        Returns:
            The number of rows.
        """
        return int(self._session().table(ident.qualified()).count())

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
            staging: The validated staging table.
            target: The target table.
            mode: The commit mode.
            keys: Merge keys (required for ``MERGE``).
            ordering: Ordering column that selects the newest row per key.

        Returns:
            A :class:`CommitResult`.

        Raises:
            UmfError: If ``MERGE`` is requested without keys.
        """
        spark = self._session()
        staged = staging.qualified()
        tgt = target.qualified()

        if mode == CommitMode.OVERWRITE:
            # A single atomic replace of the target's contents.
            spark.sql(f"INSERT OVERWRITE TABLE {tgt} SELECT * FROM {staged}")
            return CommitResult(mode=mode, rows_written=self.row_count(target))

        if mode == CommitMode.APPEND:
            spark.sql(f"INSERT INTO {tgt} SELECT * FROM {staged}")
            return CommitResult(mode=mode, rows_written=self.row_count(staging))

        if mode == CommitMode.MERGE:
            if not keys:
                raise UmfError(
                    "MERGE publish requires one or more keys.",
                    context={"target": tgt},
                )
            # De-duplicate the staged rows to the newest per key before merging,
            # so an input batch with several versions of a key applies the
            # latest one. The ordering column decides "newest".
            source = self._dedup_sql(staged, keys, ordering)
            on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
            merge_sql = (
                f"MERGE INTO {tgt} AS t USING ({source}) AS s ON {on} "
                "WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            )
            spark.sql(merge_sql)
            return CommitResult(mode=mode)

        raise UmfError(f"Unsupported commit mode '{mode}'.", context={"target": tgt})

    def _dedup_sql(self, staged: str, keys: Sequence[str], ordering: Optional[str]) -> str:
        """Build a SQL sub-query that keeps the newest staged row per key.

        Args:
            staged: The qualified staging table name.
            keys: The key columns.
            ordering: The ordering column, newest last. If ``None``, the staged
                rows are used as-is (assumed already unique per key).

        Returns:
            A SQL query string selecting one row per key.
        """
        if not ordering:
            return f"SELECT * FROM {staged}"
        partition = ", ".join(keys)
        # ROW_NUMBER keeps the newest row (ordering descending) per key.
        return (
            f"SELECT * EXCEPT (_umf_rn) FROM ("
            f"  SELECT *, ROW_NUMBER() OVER ("
            f"    PARTITION BY {partition} ORDER BY {ordering} DESC"
            f"  ) AS _umf_rn FROM {staged}"
            f") WHERE _umf_rn = 1"
        )

    def drop(self, ident: TableIdentifier) -> None:
        """Drop a Delta table if it exists.

        Args:
            ident: The table identifier.
        """
        self._session().sql(f"DROP TABLE IF EXISTS {ident.qualified()}")
