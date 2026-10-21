"""Measuring a reconciliation profile from data.

The comparison logic needs a :class:`~umf.recon.profile.ReconProfile` from each
side. This module measures one. The interface is :class:`ReconMeasurer`; the
Spark implementation computes the metrics with a small number of aggregate
queries, which is far cheaper than scanning rows individually and scales to very
large tables.

Scaling note:
    For most tables the measurer computes exact aggregates in a single pass
    (count, per-column sum, null count, min, max, max length). The expensive
    metric is distinct count on a key, so for very large tables the measurer can
    use an approximate distinct count instead; the comparison still works, and
    the approximation error is far smaller than any real duplication problem.
    Row-level hashing (recon level L3) is only done when requested and is left
    to a dedicated path because cross-engine hashing needs careful canonical
    formatting.

PySpark is imported lazily, so this module imports without Spark and the
comparison logic that depends on profiles stays testable with hand-built
profiles.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, List, Optional, Sequence

from umf.core.logging_setup import get_logger
from umf.recon.profile import ColumnProfile, MetricKind, ReconProfile

_log = get_logger("recon.measurer")


class ReconMeasurer:
    """Interface for measuring a reconciliation profile from a data handle."""

    def profile(self, handle: Any, *, key_columns: Sequence[str] = ()) -> ReconProfile:
        """Measure and return the profile for a data handle.

        Args:
            handle: The thing to measure (for the Spark measurer, a DataFrame).
            key_columns: Columns to compute distinct counts for.

        Returns:
            The measured :class:`ReconProfile`.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class SparkReconMeasurer(ReconMeasurer):
    """Measures a profile from a Spark DataFrame using aggregate queries.

    Attributes:
        approx_distinct_threshold: Row counts above which the distinct-count
            metric uses an approximate (cheaper) computation.
    """

    def __init__(self, *, approx_distinct_threshold: int = 50_000_000) -> None:
        """Create the measurer.

        Args:
            approx_distinct_threshold: The row count above which distinct counts
                are approximated rather than computed exactly, to keep very large
                tables affordable.
        """
        self.approx_distinct_threshold = approx_distinct_threshold

    def profile(self, handle: Any, *, key_columns: Sequence[str] = ()) -> ReconProfile:
        """Measure a Spark DataFrame.

        Args:
            handle: A ``pyspark.sql.DataFrame``.
            key_columns: Columns to compute distinct counts for.

        Returns:
            The measured :class:`ReconProfile`.
        """
        from pyspark.sql import functions as F  # lazy import
        from pyspark.sql.types import (  # lazy import
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            ByteType,
            StringType,
            DateType,
            TimestampType,
        )

        df = handle
        row_count = df.count()

        # Build one aggregate expression list covering every column, so the whole
        # profile (except distinct counts) is computed in a single pass.
        agg_exprs: List[Any] = []
        plan: List[tuple] = []  # (column name, MetricKind)

        for field in df.schema.fields:
            name = field.name
            dtype = field.dataType
            kind = self._kind_of(
                dtype,
                DecimalType,
                (FloatType, DoubleType),
                (ByteType, ShortType, IntegerType, LongType),
                StringType,
                DateType,
                TimestampType,
            )
            plan.append((name, kind))

            # Null count for every column.
            agg_exprs.append(
                F.count(F.when(F.col(name).isNull(), True)).alias(f"nulls__{name}")
            )

            if kind in (MetricKind.INTEGER, MetricKind.DECIMAL, MetricKind.FLOAT):
                agg_exprs.append(F.sum(F.col(name)).alias(f"sum__{name}"))
                agg_exprs.append(F.min(F.col(name)).alias(f"min__{name}"))
                agg_exprs.append(F.max(F.col(name)).alias(f"max__{name}"))
            elif kind == MetricKind.DATE:
                agg_exprs.append(F.min(F.col(name)).alias(f"min__{name}"))
                agg_exprs.append(F.max(F.col(name)).alias(f"max__{name}"))
            elif kind == MetricKind.STRING:
                agg_exprs.append(
                    F.max(F.length(F.col(name))).alias(f"maxlen__{name}")
                )

        row = df.agg(*agg_exprs).collect()[0] if agg_exprs else None

        # Distinct counts for key columns, exact or approximate by size.
        distinct: dict = {}
        keys_lower = {k.lower() for k in key_columns}
        key_cols = [f.name for f in df.schema.fields if f.name.lower() in keys_lower]
        if key_cols:
            use_approx = row_count > self.approx_distinct_threshold
            dc_exprs = [
                (F.approx_count_distinct(F.col(c)) if use_approx
                 else F.countDistinct(F.col(c))).alias(f"dc__{c}")
                for c in key_cols
            ]
            dc_row = df.agg(*dc_exprs).collect()[0]
            for c in key_cols:
                distinct[c] = int(dc_row[f"dc__{c}"])

        # Assemble the column profiles from the aggregate row.
        columns = {}
        for name, kind in plan:
            columns[name] = ColumnProfile(
                name=name,
                kind=kind,
                null_count=int(row[f"nulls__{name}"]) if row is not None else None,
                numeric_sum=self._as_sum(row, name, kind) if row is not None else None,
                min_value=self._as_text(row, f"min__{name}") if row is not None else None,
                max_value=self._as_text(row, f"max__{name}") if row is not None else None,
                max_length=(int(row[f"maxlen__{name}"])
                            if row is not None and f"maxlen__{name}" in row.asDict()
                            and row[f"maxlen__{name}"] is not None else None),
                distinct_count=distinct.get(name),
            )

        return ReconProfile(row_count=int(row_count), columns=columns)

    @staticmethod
    def _kind_of(dtype, decimal_t, float_ts, int_ts, string_t, date_t, ts_t) -> MetricKind:
        """Classify a Spark data type into a :class:`MetricKind`.

        Args:
            dtype: The Spark ``DataType``.
            decimal_t: The DecimalType class.
            float_ts: The floating-point type classes.
            int_ts: The integer type classes.
            string_t: The StringType class.
            date_t: The DateType class.
            ts_t: The TimestampType class.

        Returns:
            The metric kind, used to decide which aggregates and tolerance apply.
        """
        if isinstance(dtype, decimal_t):
            return MetricKind.DECIMAL
        if isinstance(dtype, float_ts):
            return MetricKind.FLOAT
        if isinstance(dtype, int_ts):
            return MetricKind.INTEGER
        if isinstance(dtype, string_t):
            return MetricKind.STRING
        if isinstance(dtype, (date_t, ts_t)):
            return MetricKind.DATE
        return MetricKind.OTHER

    @staticmethod
    def _as_sum(row: Any, name: str, kind: MetricKind):
        """Extract a column sum from the aggregate row, typed by kind.

        Args:
            row: The collected aggregate row.
            name: The column name.
            kind: The metric kind.

        Returns:
            A :class:`decimal.Decimal` for exact kinds, a ``float`` for floating
            kinds, or ``None`` if the column has no sum.
        """
        key = f"sum__{name}"
        if key not in row.asDict() or row[key] is None:
            return None
        value = row[key]
        if kind == MetricKind.FLOAT:
            return float(value)
        return Decimal(str(value))

    @staticmethod
    def _as_text(row: Any, key: str) -> Optional[str]:
        """Extract a value from the aggregate row as text, or ``None``.

        Args:
            row: The collected aggregate row.
            key: The aggregate alias to read.

        Returns:
            The value as a string, or ``None`` if absent or null.
        """
        if key not in row.asDict() or row[key] is None:
            return None
        return str(row[key])
