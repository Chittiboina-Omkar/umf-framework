"""Canonical column types used to describe control and log tables.

The framework targets more than one table engine (Delta and Iceberg) on more
than one platform. If we wrote table definitions directly as engine-specific
DDL, every table would have to be duplicated per engine. Instead, every table is
described once using the small, engine-neutral type vocabulary in this module,
and an engine adapter (added in a later part) translates a canonical type into
the concrete type for Delta or Iceberg.

Keeping the vocabulary small and obvious is deliberate: log and control tables
do not need the full richness of source data types, and a small set is easy to
map correctly to every engine.

Note on JSON:
    Nested or free-form structured values are described as ``JSON`` and stored
    as a serialised JSON string. A string column is supported identically by
    every engine and every query tool, which keeps the log tables portable and
    easy to inspect. The small cost is that querying inside the JSON requires a
    parse step, which is acceptable for diagnostic tables.
"""

from __future__ import annotations

from umf.core.constants import _StrEnum


class ColumnType(_StrEnum):
    """Engine-neutral column types for control and log tables.

    Each value is translated to a concrete Delta or Iceberg type by an engine
    adapter. The mapping is intentionally simple and lossless for these
    diagnostic tables.
    """

    STRING = "STRING"        # variable-length text
    INT = "INT"              # 32-bit signed integer
    LONG = "LONG"            # 64-bit signed integer (counts, durations in ms)
    DOUBLE = "DOUBLE"        # 64-bit floating point
    DECIMAL = "DECIMAL"      # exact decimal (precision/scale set per column)
    BOOLEAN = "BOOLEAN"      # true / false
    TIMESTAMP = "TIMESTAMP"  # point in time, stored in UTC
    DATE = "DATE"            # calendar date
    JSON = "JSON"            # nested / free-form, stored as a JSON string
