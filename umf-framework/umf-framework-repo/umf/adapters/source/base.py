"""Layer L1: the source reader interface.

A source reader knows how to do two things for one kind of source:

    * ``describe_schema`` return the source's columns as
      :class:`~umf.typing.source_column.SourceColumn` objects, including (where
      the source provides it) precision, scale, length and nullability, plus
      where that schema came from; and
    * ``read`` return the data as a Spark DataFrame.

Separating "describe the schema" from "read the data" matters because the type
mapping engine (layer L2) works on the described schema alone, before a single
row is read. That is how the framework can validate types, detect unmappable
columns and stop early, without the cost or risk of reading data first.

The interface is pure Python; concrete readers (files here, databases later)
import Spark lazily so that this module and the type engine remain importable
and testable without PySpark.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Sequence

from umf.core.constants import _StrEnum
from umf.typing.source_column import SourceColumn


class SchemaOrigin(_StrEnum):
    """Where a source schema came from, recorded for trust and auditing.

    The origin matters because it says how much to trust the types. Metadata
    from a database catalog is authoritative; an inferred schema from a CSV is a
    best guess and, in strict mode, is not allowed to proceed silently.
    """

    SOURCE_METADATA = "SOURCE_METADATA"  # from a database / warehouse catalog
    EMBEDDED = "EMBEDDED"                # from the file itself (Parquet / Avro / ORC)
    SIDECAR = "SIDECAR"                  # from a schema file shipped with the data
    CONTRACT = "CONTRACT"                # from a user-supplied schema contract
    INFERRED = "INFERRED"               # guessed by sampling (for example a CSV)


@dataclass(frozen=True)
class SourceSchema:
    """A described source schema.

    Attributes:
        columns: The source columns, in order.
        origin: Where the schema came from.
        note: Optional diagnostic note (for example which file was sampled).
    """

    columns: Sequence[SourceColumn]
    origin: SchemaOrigin
    note: str = ""


class SourceReader:
    """Interface every source reader implements.

    Attributes:
        source_name: The source family (for example ``"files"`` or
            ``"oracle"``), used to select the matching type mapper.
    """

    source_name: str = "generic"

    def describe_schema(self) -> SourceSchema:
        """Return the source's columns and where the schema came from.

        Returns:
            A :class:`SourceSchema`.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def read(self) -> Any:
        """Return the source data as a Spark DataFrame.

        Returns:
            A ``pyspark.sql.DataFrame`` (typed as ``Any`` so this module imports
            without PySpark).

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError
