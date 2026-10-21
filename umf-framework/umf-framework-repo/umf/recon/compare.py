"""Comparing two reconciliation profiles and deciding pass or fail.

This is the correctness heart of reconciliation, and it is pure logic with no
Spark, so it can be tested exhaustively. Given the source profile and the staged
target profile, it produces one result per check, each ready to write to the
``recon_results`` log table, and it decides pass or fail using tolerances that
reflect the design:

    * Row count, null counts, string lengths and distinct key counts are
      compared **exactly** (tolerance zero). Any difference is a failure.
    * Decimal and integer sums, min and max are compared **exactly** by default
      (``decimal_tolerance`` is zero), because that is what catches precision
      and scale loss to the last digit.
    * Floating-point sums, min and max are compared with a small
      ``float_tolerance`` to absorb the representation noise that floating point
      unavoidably carries. Without this a harmless 1e-12 difference would be a
      false alarm; with it, a real loss still fails because it is far larger than
      the tolerance.
    * String ``max_length`` is checked for truncation: if the target's maximum
      length is shorter than the source's, characters were lost and the check
      fails.

The recon level controls the depth: L1 is the row count and schema (schema is
handled in :mod:`umf.recon.schema_recon`); L2 adds the aggregate checks here;
L3 adds a whole-table row hash where available.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional, Sequence

from umf.core.constants import ReconLevel
from umf.recon.profile import ColumnProfile, MetricKind, ReconProfile


@dataclass
class ReconConfig:
    """Configuration controlling how profiles are compared.

    Attributes:
        level: The reconciliation depth (L1, L2 or L3).
        decimal_tolerance: Allowed absolute difference for decimal and integer
            comparisons. Zero means exact, which is the default and the safe
            value for money and counts.
        float_tolerance: Allowed absolute difference for floating-point
            comparisons, to absorb representation noise.
        key_columns: Columns treated as business keys, for the distinct-count
            check.
    """

    level: ReconLevel = ReconLevel.L2
    decimal_tolerance: Decimal = Decimal("0")
    float_tolerance: float = 1e-6
    key_columns: Sequence[str] = ()


def _result(
    check_name: str,
    level: ReconLevel,
    source_value: object,
    target_value: object,
    tolerance: object,
    passed: bool,
) -> dict:
    """Build one recon result dictionary, shaped for the recon_results table.

    Args:
        check_name: The check performed (for example ``"sum:amount"``).
        level: The recon level the check belongs to.
        source_value: The value measured on the source side.
        target_value: The value measured on the target side.
        tolerance: The tolerance applied.
        passed: Whether the check passed.

    Returns:
        A plain dictionary with the standard recon result fields.
    """
    try:
        difference = _difference(source_value, target_value)
    except Exception:
        difference = "n/a"
    return {
        "check_name": check_name,
        "level": str(level),
        "source_value": "null" if source_value is None else str(source_value),
        "target_value": "null" if target_value is None else str(target_value),
        "difference": str(difference),
        "tolerance": str(tolerance),
        "status": "pass" if passed else "fail",
    }


def _difference(a: object, b: object) -> object:
    """Return a readable difference between two values, when both are numeric.

    Args:
        a: The source value.
        b: The target value.

    Returns:
        The numeric difference ``target - source`` when both are numbers,
        otherwise the text ``"n/a"``.
    """
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return b - a
    return "n/a"


def compare_profiles(
    source: ReconProfile, target: ReconProfile, config: ReconConfig
) -> List[dict]:
    """Compare two profiles and return the list of check results.

    Args:
        source: The source side profile.
        target: The staged target side profile.
        config: The comparison configuration (level and tolerances).

    Returns:
        A list of recon result dictionaries. Each has a ``status`` of ``"pass"``
        or ``"fail"``; the caller treats any failure as a failed gate.
    """
    results: List[dict] = []

    # --- L1: row count (always, exact) ------------------------------------ #
    results.append(
        _result(
            "count",
            ReconLevel.L1,
            source.row_count,
            target.row_count,
            0,
            source.row_count == target.row_count,
        )
    )

    # L1 stops at the count (plus schema, handled separately).
    if config.level == ReconLevel.L1:
        return results

    # --- L2: per-column aggregate checks ---------------------------------- #
    keys = {k.lower() for k in config.key_columns}
    for name, src_col in source.columns.items():
        tgt_col = target.columns.get(name)
        if tgt_col is None:
            # A column present on the source but not the target is a schema
            # problem; record it as a failed check so it is visible here too.
            results.append(
                _result(f"column_present:{name}", ReconLevel.L2, "present",
                        "missing", 0, False)
            )
            continue

        results.extend(self_column_checks(name, src_col, tgt_col, config, keys))

    # --- L3: whole-table row hash (only if both sides provided one) ------- #
    if config.level == ReconLevel.L3 and source.row_hash is not None:
        results.append(
            _result(
                "row_hash",
                ReconLevel.L3,
                source.row_hash,
                target.row_hash,
                0,
                source.row_hash == target.row_hash,
            )
        )

    return results


def self_column_checks(
    name: str,
    src: ColumnProfile,
    tgt: ColumnProfile,
    config: ReconConfig,
    keys: set,
) -> List[dict]:
    """Produce the L2 checks for a single column.

    Args:
        name: The column name.
        src: The source column profile.
        tgt: The target column profile.
        config: The comparison configuration.
        keys: The set of key column names (lower-cased).

    Returns:
        The list of recon result dictionaries for this column.
    """
    out: List[dict] = []

    # Null counts: exact. A change means values appeared or vanished.
    if src.null_count is not None and tgt.null_count is not None:
        out.append(
            _result(
                f"null_count:{name}",
                ReconLevel.L2,
                src.null_count,
                tgt.null_count,
                0,
                src.null_count == tgt.null_count,
            )
        )

    # Numeric sum: the key precision / scale check.
    if src.numeric_sum is not None and tgt.numeric_sum is not None:
        out.append(_sum_check(name, src, tgt, config))
        out.append(_minmax_check(name, "min", src.min_value, tgt.min_value, src, config))
        out.append(_minmax_check(name, "max", src.max_value, tgt.max_value, src, config))

    # Date min / max: exact (dates compared as text in a sortable format).
    elif src.kind == MetricKind.DATE:
        if src.min_value is not None and tgt.min_value is not None:
            out.append(
                _result(f"min:{name}", ReconLevel.L2, src.min_value, tgt.min_value,
                        0, src.min_value == tgt.min_value)
            )
        if src.max_value is not None and tgt.max_value is not None:
            out.append(
                _result(f"max:{name}", ReconLevel.L2, src.max_value, tgt.max_value,
                        0, src.max_value == tgt.max_value)
            )

    # String max length: truncation check.
    if src.kind == MetricKind.STRING and src.max_length is not None and tgt.max_length is not None:
        # Truncation means the target kept fewer characters than the source had.
        passed = tgt.max_length >= src.max_length
        out.append(
            _result(
                f"max_length:{name}",
                ReconLevel.L2,
                src.max_length,
                tgt.max_length,
                0,
                passed,
            )
        )

    # Distinct count on key columns: exact.
    if name.lower() in keys and src.distinct_count is not None and tgt.distinct_count is not None:
        out.append(
            _result(
                f"distinct:{name}",
                ReconLevel.L2,
                src.distinct_count,
                tgt.distinct_count,
                0,
                src.distinct_count == tgt.distinct_count,
            )
        )

    return out


def _sum_check(name: str, src: ColumnProfile, tgt: ColumnProfile, config: ReconConfig) -> dict:
    """Compare a numeric sum with the tolerance appropriate to its kind.

    Decimal and integer sums use ``decimal_tolerance`` (zero by default, exact);
    floating-point sums use ``float_tolerance`` to absorb representation noise.

    Args:
        name: The column name.
        src: The source column profile.
        tgt: The target column profile.
        config: The comparison configuration.

    Returns:
        A recon result dictionary for the sum check.
    """
    source_sum = src.numeric_sum
    target_sum = tgt.numeric_sum

    if src.kind == MetricKind.FLOAT:
        tolerance = config.float_tolerance
        passed = abs(float(target_sum) - float(source_sum)) <= tolerance
    else:
        # Decimal and integer: exact comparison within decimal_tolerance (0).
        tolerance = config.decimal_tolerance
        diff = abs(Decimal(str(target_sum)) - Decimal(str(source_sum)))
        passed = diff <= Decimal(str(tolerance))

    return _result(f"sum:{name}", ReconLevel.L2, source_sum, target_sum, tolerance, passed)


def _minmax_check(
    name: str,
    which: str,
    source_value: Optional[str],
    target_value: Optional[str],
    src: ColumnProfile,
    config: ReconConfig,
) -> dict:
    """Compare a numeric min or max, with the kind's tolerance.

    Args:
        name: The column name.
        which: ``"min"`` or ``"max"``.
        source_value: The source min or max, as text.
        target_value: The target min or max, as text.
        src: The source column profile (for its kind).
        config: The comparison configuration.

    Returns:
        A recon result dictionary.
    """
    if source_value is None or target_value is None:
        return _result(f"{which}:{name}", ReconLevel.L2, source_value, target_value,
                       0, source_value == target_value)

    if src.kind == MetricKind.FLOAT:
        tolerance = config.float_tolerance
        passed = abs(float(target_value) - float(source_value)) <= tolerance
    else:
        tolerance = config.decimal_tolerance
        diff = abs(Decimal(target_value) - Decimal(source_value))
        passed = diff <= Decimal(str(tolerance))

    return _result(f"{which}:{name}", ReconLevel.L2, source_value, target_value,
                   tolerance, passed)
