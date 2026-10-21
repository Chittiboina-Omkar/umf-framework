"""Audit columns added to the raw layer.

The raw layer is an exact mirror of the source: original column names, order and
types, with nothing transformed. The only thing the framework adds is a small,
fixed set of audit columns that record how each row arrived. This module decides
what those columns are and, crucially, names them safely so they never clash
with a real source column.

Why conflict-safe naming matters:
    A source could legitimately have a column called ``ingestion_time`` or
    ``run_id``. If the framework blindly added a column of the same name, it
    would either overwrite the source column (data loss) or fail. To avoid this,
    the audit columns use a configurable prefix (default ``_umf_``), and this
    module still checks the resolved names against the source columns and, if a
    collision somehow remains, appends a numeric suffix until the name is free.

The DataFrame-level injection (actually adding the columns with their values) is
a small Spark helper at the bottom, imported lazily. The naming and
specification logic above it is pure Python and fully testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence


@dataclass(frozen=True)
class AuditColumn:
    """One audit column to add to the raw layer.

    Attributes:
        logical: The stable logical name, before prefixing (for example
            ``"ingestion_time"``). Used to look the column up in code.
        type_string: The target type for the column (format-neutral enough for
            both Delta and Iceberg, for example ``"TIMESTAMP"`` or ``"STRING"``).
        description: What the column records.
    """

    logical: str
    type_string: str
    description: str


# The fixed set of audit columns, in the order they are appended. These capture
# enough to trace any row back to its origin and run without storing any
# business data.
AUDIT_COLUMNS: List[AuditColumn] = [
    AuditColumn("ingestion_time", "TIMESTAMP", "When the row was ingested (UTC)."),
    AuditColumn("run_id", "STRING", "The run that ingested the row."),
    AuditColumn("batch_id", "STRING", "The batch within the run, if any."),
    AuditColumn("source_file_name", "STRING", "Source file the row came from."),
    AuditColumn("source_file_modified_time", "TIMESTAMP",
                "Source file modification time, if known."),
    AuditColumn("source_system", "STRING", "The source system identifier."),
    AuditColumn("row_hash", "STRING",
                "Optional hash of the business columns for change detection."),
]


def resolve_audit_names(
    source_columns: Sequence[str], *, prefix: str = "_umf_"
) -> Dict[str, str]:
    """Return a mapping of logical audit name to a collision-free physical name.

    Each audit column's physical name is ``<prefix><logical>``. If that name
    still clashes with a source column (for example the source literally has a
    ``_umf_run_id`` column), a numeric suffix is added until the name is free,
    so an audit column can never overwrite source data.

    Args:
        source_columns: The source column names (the exact mirror), used to
            detect and avoid collisions.
        prefix: The prefix applied to every audit column name.

    Returns:
        A dictionary mapping each audit column's logical name to its final,
        collision-free physical name.
    """
    taken = {c.lower() for c in source_columns}
    resolved: Dict[str, str] = {}

    for col in AUDIT_COLUMNS:
        candidate = f"{prefix}{col.logical}"
        # Append a numeric suffix until the candidate is free, comparing
        # case-insensitively because several engines treat names that way.
        suffix = 1
        base = candidate
        while candidate.lower() in taken:
            candidate = f"{base}_{suffix}"
            suffix += 1
        resolved[col.logical] = candidate
        taken.add(candidate.lower())

    return resolved


def audit_column_ddl(
    source_columns: Sequence[str], *, prefix: str = "_umf_", include_row_hash: bool = False
) -> List[tuple]:
    """Return the audit columns as ``(name, type_string)`` DDL pairs.

    These are appended to the source columns to form the raw table's full column
    list.

    Args:
        source_columns: The source column names, for collision-free naming.
        prefix: The audit column prefix.
        include_row_hash: Whether to include the optional ``row_hash`` column.
            It is off by default because it is only needed when change detection
            uses a stored hash.

    Returns:
        A list of ``(physical_name, type_string)`` pairs in audit-column order.
    """
    names = resolve_audit_names(source_columns, prefix=prefix)
    pairs: List[tuple] = []
    for col in AUDIT_COLUMNS:
        if col.logical == "row_hash" and not include_row_hash:
            continue
        pairs.append((names[col.logical], col.type_string))
    return pairs


def add_audit_columns(
    df: Any,
    *,
    run_id: str,
    source_system: str,
    prefix: str = "_umf_",
    batch_id: str = "",
    source_file_column: str = "",
    include_row_hash: bool = False,
) -> Any:
    """Add the audit columns to a Spark DataFrame (lazy Spark import).

    This is the only Spark-touching function in the module. It appends the audit
    columns with their values; the ingestion time is set to the current UTC time
    at write, and the file name / modified time come from Spark's file metadata
    when available.

    Args:
        df: The source Spark DataFrame (the exact mirror).
        run_id: The current run identifier.
        source_system: The source system identifier.
        prefix: The audit column prefix.
        batch_id: The batch identifier within the run, if any.
        source_file_column: The name of an existing column that already holds
            the source file name (for example from Spark's ``_metadata``). Empty
            if not available.
        include_row_hash: Whether to add the optional row-hash column (left null
            here; it is populated by the change-detection step when enabled).

    Returns:
        A new DataFrame with the audit columns appended.
    """
    # Imported lazily so the pure-Python parts of this module need no Spark.
    from pyspark.sql import functions as F  # local import on purpose

    names = resolve_audit_names(df.columns, prefix=prefix)
    out = (
        df.withColumn(names["ingestion_time"], F.current_timestamp())
        .withColumn(names["run_id"], F.lit(run_id))
        .withColumn(names["batch_id"], F.lit(batch_id or None))
        .withColumn(names["source_system"], F.lit(source_system))
    )

    if source_file_column and source_file_column in df.columns:
        out = out.withColumn(names["source_file_name"], F.col(source_file_column))
    else:
        out = out.withColumn(names["source_file_name"], F.lit(None).cast("string"))

    out = out.withColumn(
        names["source_file_modified_time"], F.lit(None).cast("timestamp")
    )

    if include_row_hash:
        out = out.withColumn(names["row_hash"], F.lit(None).cast("string"))

    return out
