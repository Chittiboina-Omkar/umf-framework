"""The data model for a reconciliation profile.

Reconciliation works by measuring the same set of metrics on two sides (the
source and the staged target) and comparing them. A :class:`ReconProfile` is one
side's measurements: the row count, plus per-column metrics chosen to catch the
ways data can go wrong in a migration.

The metrics are chosen deliberately:

    * ``null_count``   catches values that silently became null (for example an
      overflow on a bad cast).
    * ``numeric_sum``  is the single most powerful check for precision and scale
      loss: if a decimal column's scale is reduced, every row shifts slightly
      and the sum no longer matches, even though the row count is unchanged.
    * ``min_value`` / ``max_value`` catch range problems and date shifts.
    * ``max_length``  catches string truncation (the target keeping fewer
      characters than the source held).
    * ``distinct_count`` on key columns catches duplicates or lost keys.

Separating the measured profile (this module, plus the Spark measurer) from the
comparison (in :mod:`umf.recon.compare`) is what lets the correctness-critical
comparison and tolerance logic be written and tested in plain Python, with no
Spark and no live data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Optional

from umf.core.constants import _StrEnum


class MetricKind(_StrEnum):
    """The measurement family of a column, which decides its tolerance.

    The distinction between ``DECIMAL`` / ``INTEGER`` (compared exactly) and
    ``FLOAT`` (compared with a small tolerance) is central: money and counts
    must match to the last digit, while floating point carries unavoidable
    representation noise that a tiny tolerance absorbs.
    """

    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    STRING = "STRING"
    DATE = "DATE"
    OTHER = "OTHER"


@dataclass(frozen=True)
class ColumnProfile:
    """Measured metrics for one column.

    Attributes:
        name: The column name.
        kind: The :class:`MetricKind`, which decides how values are compared.
        null_count: Number of null values in the column.
        numeric_sum: Sum of the column for numeric kinds, as a
            :class:`decimal.Decimal` for exact kinds or a ``float`` for floating
            kinds. ``None`` for non-numeric columns.
        min_value: The minimum value, as text, for numeric and date kinds.
        max_value: The maximum value, as text, for numeric and date kinds.
        max_length: The maximum character length, for string kinds.
        distinct_count: The number of distinct values, measured for key columns.
    """

    name: str
    kind: MetricKind
    null_count: Optional[int] = None
    numeric_sum: Optional[object] = None  # Decimal for exact, float for floating
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    max_length: Optional[int] = None
    distinct_count: Optional[int] = None


@dataclass
class ReconProfile:
    """All measured metrics for one side (source or target) of a table.

    Attributes:
        row_count: The total number of rows.
        columns: A mapping of column name to its :class:`ColumnProfile`.
        row_hash: An optional whole-table hash, used only at recon level L3 when
            both sides are measured the same way. ``None`` when not computed.
    """

    row_count: int
    columns: Dict[str, ColumnProfile] = field(default_factory=dict)
    row_hash: Optional[str] = None
