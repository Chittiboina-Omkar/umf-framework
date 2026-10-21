"""Planning how to split a large table extraction into chunks.

A table of hundreds of gigabytes or more cannot be read in one query without
overwhelming the source and the cluster. The answer is to split the read into
chunks along a column, each chunk a bounded slice, so the work is parallel,
bounded in memory, and resumable: if a chunk fails, only that chunk is retried,
not the whole table.

This module plans the chunks. It is pure arithmetic over the column bounds, so
it is fully testable without a source or Spark. It produces predicates (as SQL
text) that the JDBC reader turns into parallel partitions or sequential,
checkpointed reads.

It also builds the incremental predicate: for an incremental load, only rows
past the stored watermark (and up to a high bound) are read, which is how daily
increments stay small after the initial full load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Sequence

from umf.core.constants import _StrEnum
from umf.core.exceptions import ConfigError


class ChunkStrategy(_StrEnum):
    """How a table is split for extraction."""

    NONE = "NONE"                  # a single chunk (small tables)
    NUMERIC_RANGE = "NUMERIC_RANGE"  # split a numeric / id column into ranges
    DATE_RANGE = "DATE_RANGE"      # split a date / timestamp column into windows


@dataclass(frozen=True)
class Chunk:
    """One slice of a table to extract.

    Attributes:
        chunk_id: A stable identifier, used for checkpointing and logging so a
            resumed extract knows which chunks are already done.
        predicate: The SQL predicate selecting this chunk's rows, or an empty
            string for a single full chunk.
        lower: The lower bound (as text), for diagnostics.
        upper: The upper bound (as text), for diagnostics.
    """

    chunk_id: str
    predicate: str
    lower: Optional[str] = None
    upper: Optional[str] = None


def plan_numeric_chunks(
    *, column: str, min_value: int, max_value: int, num_chunks: int
) -> List[Chunk]:
    """Split a numeric column's range into contiguous chunks.

    Each chunk covers a half-open range ``[lo, hi)`` except the last, which
    includes the maximum. The chunks together cover every value from the minimum
    to the maximum with no gaps and no overlaps, so every row is read exactly
    once.

    Args:
        column: The numeric column to split on (for example an id).
        min_value: The smallest value in the column.
        max_value: The largest value in the column.
        num_chunks: How many chunks to produce. Values below 1 are treated as 1.

    Returns:
        The list of chunks.

    Raises:
        ConfigError: If the maximum is less than the minimum.
    """
    if max_value < min_value:
        raise ConfigError(
            f"max_value ({max_value}) is less than min_value ({min_value}) for "
            f"column '{column}'."
        )
    n = max(1, num_chunks)
    total = max_value - min_value + 1
    if total <= n:
        # Fewer values than requested chunks: one chunk per value is pointless,
        # so fall back to a single chunk covering the whole range.
        return [
            Chunk(
                chunk_id=f"{column}_0",
                predicate=f"{column} >= {min_value} AND {column} <= {max_value}",
                lower=str(min_value),
                upper=str(max_value),
            )
        ]

    step = total // n
    chunks: List[Chunk] = []
    lo = min_value
    for i in range(n):
        if i == n - 1:
            hi = max_value
            predicate = f"{column} >= {lo} AND {column} <= {hi}"
        else:
            hi = lo + step
            predicate = f"{column} >= {lo} AND {column} < {hi}"
        chunks.append(
            Chunk(chunk_id=f"{column}_{i}", predicate=predicate, lower=str(lo), upper=str(hi))
        )
        lo = hi
    return chunks


def plan_date_chunks(
    *, column: str, start: date, end: date, window_days: int = 30
) -> List[Chunk]:
    """Split a date column's range into fixed-width windows.

    Args:
        column: The date or timestamp column to split on.
        start: The earliest date (inclusive).
        end: The latest date (inclusive).
        window_days: The width of each window in days.

    Returns:
        The list of chunks, each covering ``[lo, hi)`` except the last which
        includes ``end``.

    Raises:
        ConfigError: If ``end`` is before ``start`` or the window is not
            positive.
    """
    if end < start:
        raise ConfigError(f"end ({end}) is before start ({start}) for '{column}'.")
    if window_days <= 0:
        raise ConfigError("window_days must be positive.")

    chunks: List[Chunk] = []
    lo = start
    index = 0
    while lo <= end:
        hi = lo + timedelta(days=window_days)
        if hi > end:
            # Final window includes the end date.
            predicate = f"{column} >= DATE '{lo.isoformat()}' AND {column} <= DATE '{end.isoformat()}'"
            upper = end.isoformat()
        else:
            predicate = f"{column} >= DATE '{lo.isoformat()}' AND {column} < DATE '{hi.isoformat()}'"
            upper = hi.isoformat()
        chunks.append(
            Chunk(chunk_id=f"{column}_{index}", predicate=predicate, lower=lo.isoformat(), upper=upper)
        )
        lo = hi
        index += 1
    return chunks


def single_chunk() -> List[Chunk]:
    """Return a single full chunk (no predicate), for small tables.

    Returns:
        A list with one chunk whose predicate is empty.
    """
    return [Chunk(chunk_id="full", predicate="")]


def incremental_predicate(
    *, column: str, watermark: Optional[str], high: Optional[str] = None,
    quote: bool = True,
) -> str:
    """Build the predicate selecting only rows past the watermark.

    For the first run there is no watermark, so the predicate is empty (read
    everything up to the high bound if given). On later runs only rows strictly
    greater than the stored watermark are read, which keeps each increment small
    and never re-reads committed rows.

    Args:
        column: The watermark column.
        watermark: The current watermark value, or ``None`` for the first run.
        high: An optional upper bound (for example "now"), so a run reads a
            closed window and a row arriving mid-run is picked up next time.
        quote: Whether to wrap the values in single quotes (needed for
            timestamps and strings; set ``False`` for bare numeric watermarks).

    Returns:
        The SQL predicate, or an empty string when there is nothing to bound.
    """
    def lit(v: str) -> str:
        return f"'{v}'" if quote else str(v)

    parts: List[str] = []
    if watermark is not None:
        parts.append(f"{column} > {lit(watermark)}")
    if high is not None:
        parts.append(f"{column} <= {lit(high)}")
    return " AND ".join(parts)


def plan_chunks_from_config(
    *,
    volume_class: str,
    column: Optional[str],
    bounds: Optional[Sequence] = None,
    num_chunks: int = 8,
    window_days: int = 30,
) -> List[Chunk]:
    """Choose a chunking plan from the volume class and available bounds.

    Small and medium tables read in a single chunk. Large and very large tables
    chunk along the configured column, using numeric ranges when the bounds are
    numbers and date windows when they are dates.

    Args:
        volume_class: One of ``small``, ``medium``, ``large``, ``very_large``.
        column: The column to chunk on, required for large volumes.
        bounds: The ``(min, max)`` bounds of the chunk column, required for large
            volumes.
        num_chunks: Number of numeric chunks for large volumes.
        window_days: Window size for date chunking.

    Returns:
        The list of chunks.

    Raises:
        ConfigError: If a large volume is requested without a chunk column and
            bounds.
    """
    vc = volume_class.strip().lower()
    if vc in ("small", "medium"):
        return single_chunk()

    if not column or not bounds or len(bounds) != 2:
        raise ConfigError(
            f"volume_class '{volume_class}' needs a chunk column and (min, max) "
            "bounds to split the extraction."
        )

    low, high = bounds
    if isinstance(low, (date, datetime)) and isinstance(high, (date, datetime)):
        return plan_date_chunks(
            column=column,
            start=low if isinstance(low, date) and not isinstance(low, datetime) else low.date() if isinstance(low, datetime) else low,
            end=high if isinstance(high, date) and not isinstance(high, datetime) else high.date() if isinstance(high, datetime) else high,
            window_days=window_days,
        )
    return plan_numeric_chunks(
        column=column, min_value=int(low), max_value=int(high), num_chunks=num_chunks
    )
