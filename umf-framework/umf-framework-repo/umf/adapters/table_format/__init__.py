"""Table format writers (Delta, Iceberg) behind one interface.

The engine writes through the :class:`~umf.adapters.table_format.base.TableWriter`
interface and never depends on a specific format. :func:`get_writer` returns the
writer for a configured format; adding a new format means adding a writer and a
line here.
"""

from __future__ import annotations

from typing import Any, Optional

from umf.adapters.table_format.base import CommitMode, CommitResult, TableWriter
from umf.core.constants import TableFormat
from umf.core.exceptions import ConfigError


def get_writer(table_format: TableFormat, spark: Optional[Any] = None) -> TableWriter:
    """Return a table writer for the given format.

    The concrete writer classes are imported lazily so this module (and anything
    that only needs the interface) imports without pulling in the Spark-backed
    implementations until a writer is actually requested.

    Args:
        table_format: The target format.
        spark: An optional Spark session to pass to the writer.

    Returns:
        A :class:`TableWriter` for the format.

    Raises:
        ConfigError: If the format is not supported.
    """
    if table_format == TableFormat.DELTA:
        from umf.adapters.table_format.delta import DeltaTableWriter

        return DeltaTableWriter(spark)
    if table_format == TableFormat.ICEBERG:
        from umf.adapters.table_format.iceberg import IcebergTableWriter

        return IcebergTableWriter(spark)
    raise ConfigError(f"Unsupported table format '{table_format}'.")


__all__ = [
    "TableWriter",
    "CommitMode",
    "CommitResult",
    "get_writer",
]
