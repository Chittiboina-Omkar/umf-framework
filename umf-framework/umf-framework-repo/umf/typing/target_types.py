"""Resolving a canonical type into a concrete Delta or Iceberg target type.

This is the second hop of type mapping: canonical type to target type. Its job
is not only to produce a target type string, but to tell the truth about the
mapping:

    * ``exact``           Is the value preserved with no loss at all?
    * ``fallback_used``   Did we have to change the type family (for example a
                          narrow integer widened, or an exotic type stored as a
                          string)? A fallback can still be exact in value.
    * ``data_loss_risk``  Could information be lost (for example a decimal whose
                          precision exceeds the target maximum, or a timestamp
                          with more fractional digits than the target keeps)?
    * ``needs_decision``  Is this a case the framework refuses to resolve on its
                          own, because any automatic choice could lose data? Such
                          a column is handed to the unmappable policy, which uses
                          the user's configured decision or stops and asks.

The guiding rule is "exact first, never silent": the resolver maps exactly
wherever it can, and wherever it cannot it flags the column rather than quietly
degrading it.
"""

from __future__ import annotations

from dataclasses import dataclass

from umf.core.constants import TableFormat
from umf.typing.canonical import (
    MAX_DECIMAL_PRECISION,
    MAX_TIMESTAMP_FRACTIONAL_DIGITS,
    CanonicalKind,
    CanonicalType,
)


@dataclass(frozen=True)
class TargetType:
    """The outcome of resolving a canonical type to a target type.

    Attributes:
        type_string: The target type as it will appear in DDL (for example
            ``"DECIMAL(10,2)"`` or ``"STRING"``), or empty when
            ``needs_decision`` is true and no safe string could be produced.
        exact: True if the value is preserved with no loss.
        fallback_used: True if the type family changed (widening, or storing an
            exotic type as a string).
        data_loss_risk: True if information could be lost.
        needs_decision: True if the framework refuses to resolve this
            automatically and the unmappable policy must decide.
        reason: A short, human-readable explanation, shown in the type-mapping
            log and in any decision prompt.
    """

    type_string: str
    exact: bool
    fallback_used: bool
    data_loss_risk: bool
    needs_decision: bool
    reason: str


# Timestamp target type names per format. Delta uses TIMESTAMP_NTZ for a
# timestamp without a zone (a table feature) and TIMESTAMP for one normalised to
# UTC; Iceberg uses timestamp and timestamptz.
_TS_NTZ = {TableFormat.DELTA: "TIMESTAMP_NTZ", TableFormat.ICEBERG: "timestamp"}
_TS_TZ = {TableFormat.DELTA: "TIMESTAMP", TableFormat.ICEBERG: "timestamptz"}


def resolve_target(
    canonical: CanonicalType,
    fmt: TableFormat,
    *,
    iceberg_has_narrow_ints: bool = False,
) -> TargetType:
    """Resolve a canonical type to a target type for the given format.

    Args:
        canonical: The canonical type to resolve.
        fmt: The target table format (:class:`~umf.core.constants.TableFormat`).
        iceberg_has_narrow_ints: Iceberg has no byte or short integer type, so a
            narrow integer is widened to ``int``. This flag exists only for
            forward-compatibility if a future Iceberg version adds them; it
            defaults to ``False`` which is correct today.

    Returns:
        A :class:`TargetType` describing the resolution and its fidelity.
    """
    kind = canonical.kind

    # --- straightforward, exact kinds ------------------------------------- #
    simple_exact = {
        CanonicalKind.BOOLEAN: _name(fmt, "BOOLEAN", "boolean"),
        CanonicalKind.INT: _name(fmt, "INT", "int"),
        CanonicalKind.LONG: _name(fmt, "BIGINT", "long"),
        CanonicalKind.FLOAT: _name(fmt, "FLOAT", "float"),
        CanonicalKind.DOUBLE: _name(fmt, "DOUBLE", "double"),
        CanonicalKind.STRING: _name(fmt, "STRING", "string"),
        CanonicalKind.BINARY: _name(fmt, "BINARY", "binary"),
        CanonicalKind.DATE: _name(fmt, "DATE", "date"),
    }
    if kind in simple_exact:
        return TargetType(
            type_string=simple_exact[kind],
            exact=True,
            fallback_used=False,
            data_loss_risk=False,
            needs_decision=False,
            reason="exact mapping",
        )

    # --- narrow integers (byte / short) ----------------------------------- #
    if kind == CanonicalKind.BYTE:
        return _narrow_int(fmt, "TINYINT", iceberg_has_narrow_ints)
    if kind == CanonicalKind.SHORT:
        return _narrow_int(fmt, "SMALLINT", iceberg_has_narrow_ints)

    # --- decimal: exact only within the precision limit ------------------- #
    if kind == CanonicalKind.DECIMAL:
        return _decimal(canonical, fmt)

    # --- timestamps: exact only within the fractional-second limit -------- #
    if kind in (CanonicalKind.TIMESTAMP, CanonicalKind.TIMESTAMP_TZ):
        return _timestamp(canonical, fmt)

    # --- types with no native target: store as string (a fallback) -------- #
    string_fallback = {
        CanonicalKind.TIME: "time of day",
        CanonicalKind.INTERVAL: "interval",
        CanonicalKind.UUID: "uuid",
        CanonicalKind.JSON: "json / variant",
        CanonicalKind.GEOGRAPHY: "geography",
    }
    if kind in string_fallback:
        return TargetType(
            type_string=_name(fmt, "STRING", "string"),
            exact=True,  # the value is preserved as text, just not the type
            fallback_used=True,
            data_loss_risk=False,
            needs_decision=False,
            reason=f"{string_fallback[kind]} stored as string (no native target)",
        )

    # --- complex and unknown: refuse to guess ----------------------------- #
    if kind in (CanonicalKind.ARRAY, CanonicalKind.STRUCT, CanonicalKind.MAP):
        # Complex types are supported natively by both formats, but mapping their
        # element / field types correctly is handled where the full nested
        # schema is available (a later part). For now, flag for a decision rather
        # than produce a half-correct nested type.
        return _needs_decision(
            "complex type (array / struct / map) mapping is resolved with the "
            "full nested schema"
        )

    return _needs_decision(
        canonical.note or f"unknown canonical kind '{kind}' has no target"
    )


def _name(fmt: TableFormat, delta: str, iceberg: str) -> str:
    """Return the format-appropriate type name.

    Args:
        fmt: The target format.
        delta: The Delta type name.
        iceberg: The Iceberg type name.

    Returns:
        The name for the given format.
    """
    return delta if fmt == TableFormat.DELTA else iceberg


def _narrow_int(fmt: TableFormat, delta_name: str, iceberg_narrow: bool) -> TargetType:
    """Resolve a narrow integer, widening to int on Iceberg.

    Args:
        fmt: The target format.
        delta_name: The Delta narrow-integer type name (TINYINT or SMALLINT).
        iceberg_narrow: Whether Iceberg supports narrow integers (currently no).

    Returns:
        The :class:`TargetType`. Widening never loses a value, so it is exact,
        but it is recorded as a fallback because the type family changed.
    """
    if fmt == TableFormat.DELTA:
        return TargetType(
            type_string=delta_name,
            exact=True,
            fallback_used=False,
            data_loss_risk=False,
            needs_decision=False,
            reason="exact mapping",
        )
    # Iceberg: widen to int (lossless).
    if iceberg_narrow:
        return TargetType("int", True, False, False, False, "exact mapping")
    return TargetType(
        type_string="int",
        exact=True,
        fallback_used=True,
        data_loss_risk=False,
        needs_decision=False,
        reason="widened to int (Iceberg has no narrow integer type); no value lost",
    )


def _decimal(canonical: CanonicalType, fmt: TableFormat) -> TargetType:
    """Resolve a decimal, flagging a precision that exceeds the target limit.

    Args:
        canonical: The canonical decimal type.
        fmt: The target format.

    Returns:
        An exact ``DECIMAL(p,s)`` when precision is known and within the limit;
        otherwise a :class:`TargetType` marked ``needs_decision`` so the
        unmappable policy chooses how to handle the overflow.
    """
    precision = canonical.precision
    scale = canonical.scale if canonical.scale is not None else 0

    if precision is None:
        return _needs_decision(
            "decimal has no declared precision (for example Oracle NUMBER); "
            "a target precision must be chosen or the value stored as string"
        )
    if precision > MAX_DECIMAL_PRECISION:
        return _needs_decision(
            f"decimal precision {precision} exceeds the target maximum of "
            f"{MAX_DECIMAL_PRECISION}; it cannot be represented exactly"
        )
    type_string = _name(fmt, f"DECIMAL({precision},{scale})", f"decimal({precision},{scale})")
    return TargetType(
        type_string=type_string,
        exact=True,
        fallback_used=False,
        data_loss_risk=False,
        needs_decision=False,
        reason="exact decimal within precision limit",
    )


def _timestamp(canonical: CanonicalType, fmt: TableFormat) -> TargetType:
    """Resolve a timestamp, flagging fractional precision beyond the target.

    Args:
        canonical: The canonical timestamp type.
        fmt: The target format.

    Returns:
        An exact timestamp when the fractional precision is within the target's
        microsecond limit; otherwise ``needs_decision`` so the policy chooses
        between truncating with a warning or storing as a string.
    """
    frac = canonical.precision
    target = (_TS_TZ if canonical.kind == CanonicalKind.TIMESTAMP_TZ else _TS_NTZ)[fmt]

    if frac is not None and frac > MAX_TIMESTAMP_FRACTIONAL_DIGITS:
        return _needs_decision(
            f"timestamp has {frac} fractional-second digits, more than the "
            f"target keeps ({MAX_TIMESTAMP_FRACTIONAL_DIGITS}); precision would "
            "be lost"
        )
    return TargetType(
        type_string=target,
        exact=True,
        fallback_used=False,
        data_loss_risk=False,
        needs_decision=False,
        reason="exact timestamp within fractional-second limit",
    )


def _needs_decision(reason: str) -> TargetType:
    """Build a TargetType that defers to the unmappable policy.

    Args:
        reason: Why an automatic mapping is refused.

    Returns:
        A :class:`TargetType` with ``needs_decision`` set and no type string.
    """
    return TargetType(
        type_string="",
        exact=False,
        fallback_used=False,
        data_loss_risk=True,
        needs_decision=True,
        reason=reason,
    )
