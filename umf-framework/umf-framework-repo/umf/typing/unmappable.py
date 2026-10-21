"""Deciding what to do with a type that cannot be mapped exactly.

When :mod:`umf.typing.target_types` marks a column as ``needs_decision`` (a
decimal whose precision exceeds the target, a nanosecond timestamp, an unknown
type), the framework will not guess. This module applies the rule you set:

    * By default, the column **fails** and the full details are surfaced
      (column, source type, target limit, and why), so a human can see exactly
      what is wrong.
    * The human makes a decision once and records it in the config under
      ``data_type_handling`` (a global default and optional per-column
      overrides). On the next run the decision is applied automatically, and no
      question is asked at run time.

The supported decisions are intentionally few and explicit, because each one is
a conscious trade-off a person should make, not a default the framework should
assume:

    FAIL            keep failing until a real decision is made (the default)
    TO_STRING       store the value as text, preserving it without the type
    DECIMAL_38      cap a too-large decimal at the target maximum precision,
                    keeping the configured scale (the user accepts the cap)
    TRUNCATE_MICROS truncate a sub-microsecond timestamp to microseconds
    NULL_AND_LOG    replace an unrepresentable value with null and log it

Applying a decision still records, in the type-mapping log, that a fallback was
used and whether there is a residual data-loss risk, so the trail always shows
what happened and why.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from umf.core.constants import TableFormat, _StrEnum
from umf.core.exceptions import SchemaError
from umf.typing.canonical import MAX_DECIMAL_PRECISION, CanonicalKind, CanonicalType
from umf.typing.target_types import TargetType


class TypeDecision(_StrEnum):
    """A configured decision for an otherwise unmappable column."""

    FAIL = "FAIL"
    TO_STRING = "TO_STRING"
    DECIMAL_38 = "DECIMAL_38"
    TRUNCATE_MICROS = "TRUNCATE_MICROS"
    NULL_AND_LOG = "NULL_AND_LOG"


@dataclass(frozen=True)
class HandlingConfig:
    """The ``data_type_handling`` configuration, resolved for lookups.

    Attributes:
        default: The decision to use when a column has no specific override. If
            ``FAIL`` (the default), any unmappable column stops its table until a
            real decision is set.
        per_column: A mapping of ``"table.column"`` (lower-cased) to a specific
            decision, so one awkward column can be handled without changing the
            default for the rest.
    """

    default: TypeDecision = TypeDecision.FAIL
    per_column: Dict[str, TypeDecision] = None  # type: ignore[assignment]

    @staticmethod
    def from_config(raw: Optional[Dict[str, Any]]) -> "HandlingConfig":
        """Build a :class:`HandlingConfig` from the raw config section.

        Expected raw shape (all optional)::

            {
              "default": "FAIL",
              "columns": {
                "accounts.balance": "DECIMAL_38",
                "orders.legacy_id": "TO_STRING"
              }
            }

        Args:
            raw: The ``data_type_handling`` section of the config, or ``None``.

        Returns:
            The resolved :class:`HandlingConfig`. An absent section means the
            safe default: fail on anything unmappable.

        Raises:
            SchemaError: If a decision value is not one of the allowed options.
        """
        raw = raw or {}
        try:
            default = TypeDecision.coerce(raw.get("default", "FAIL"))
        except ValueError as exc:
            raise SchemaError(str(exc)) from exc

        per_column: Dict[str, TypeDecision] = {}
        for key, value in (raw.get("columns") or {}).items():
            try:
                per_column[str(key).strip().lower()] = TypeDecision.coerce(value)
            except ValueError as exc:
                raise SchemaError(
                    f"Invalid data_type_handling for '{key}': {exc}"
                ) from exc

        return HandlingConfig(default=default, per_column=per_column)

    def decision_for(self, table: str, column: str) -> TypeDecision:
        """Return the decision for a specific column.

        Args:
            table: The table name.
            column: The column name.

        Returns:
            The per-column decision if one is set, otherwise the default.
        """
        key = f"{table}.{column}".lower()
        return (self.per_column or {}).get(key, self.default)


@dataclass(frozen=True)
class ResolvedMapping:
    """The final target type after any decision has been applied.

    Attributes:
        type_string: The resolved target type string.
        fallback_used: Whether a fallback or decision changed the type.
        data_loss_risk: Whether information may still be lost after the decision.
        decision: The decision that was applied, if any.
        reason: A short explanation of the outcome.
    """

    type_string: str
    fallback_used: bool
    data_loss_risk: bool
    decision: Optional[TypeDecision]
    reason: str


def apply_decision(
    *,
    table: str,
    column: str,
    canonical: CanonicalType,
    target: TargetType,
    fmt: TableFormat,
    handling: HandlingConfig,
) -> ResolvedMapping:
    """Resolve a ``needs_decision`` column using the configured handling.

    If the target resolution did not need a decision, this returns it unchanged.
    Otherwise it looks up the decision for the column and applies it, or raises
    :class:`SchemaError` with full, non-sensitive detail when the decision is
    ``FAIL`` (the default) so the problem is surfaced for a human to resolve.

    Args:
        table: The table name (for per-column lookup and messages).
        column: The column name.
        canonical: The canonical type that could not be mapped exactly.
        target: The target resolution, expected to have ``needs_decision`` true.
        fmt: The target format, used when a decision produces a concrete type.
        handling: The resolved ``data_type_handling`` configuration.

    Returns:
        A :class:`ResolvedMapping` with the final type string.

    Raises:
        SchemaError: When the applicable decision is ``FAIL`` (no real decision
            configured), carrying the details needed to make one.
    """
    # Not actually a decision case: pass the target through unchanged.
    if not target.needs_decision:
        return ResolvedMapping(
            type_string=target.type_string,
            fallback_used=target.fallback_used,
            data_loss_risk=target.data_loss_risk,
            decision=None,
            reason=target.reason,
        )

    decision = handling.decision_for(table, column)

    if decision == TypeDecision.FAIL:
        # Surface everything a person needs to decide, with no data values.
        raise SchemaError(
            f"Column '{table}.{column}' cannot be mapped exactly and no "
            "data_type_handling decision is configured for it. "
            f"Source type: {canonical.describe()}. Reason: {target.reason}. "
            "Set a decision (TO_STRING, DECIMAL_38, TRUNCATE_MICROS, "
            "NULL_AND_LOG) in data_type_handling to proceed.",
            context={
                "table": table,
                "column": column,
                "source_type": canonical.describe(),
                "reason": target.reason,
            },
        )

    string_type = "STRING" if fmt == TableFormat.DELTA else "string"

    if decision == TypeDecision.TO_STRING:
        return ResolvedMapping(
            type_string=string_type,
            fallback_used=True,
            data_loss_risk=False,  # text preserves the value
            decision=decision,
            reason="stored as string by configured decision",
        )

    if decision == TypeDecision.DECIMAL_38:
        # Cap precision at the maximum, keep the source scale if it fits.
        scale = canonical.scale if canonical.scale is not None else 0
        scale = min(scale, MAX_DECIMAL_PRECISION)
        type_string = (
            f"DECIMAL({MAX_DECIMAL_PRECISION},{scale})"
            if fmt == TableFormat.DELTA
            else f"decimal({MAX_DECIMAL_PRECISION},{scale})"
        )
        return ResolvedMapping(
            type_string=type_string,
            fallback_used=True,
            data_loss_risk=True,  # digits beyond 38 would overflow / error
            decision=decision,
            reason=f"capped at DECIMAL({MAX_DECIMAL_PRECISION},{scale}) by decision",
        )

    if decision == TypeDecision.TRUNCATE_MICROS:
        target_ts = "TIMESTAMP_NTZ" if fmt == TableFormat.DELTA else "timestamp"
        if canonical.kind == CanonicalKind.TIMESTAMP_TZ:
            target_ts = "TIMESTAMP" if fmt == TableFormat.DELTA else "timestamptz"
        return ResolvedMapping(
            type_string=target_ts,
            fallback_used=True,
            data_loss_risk=True,  # sub-microsecond digits are dropped
            decision=decision,
            reason="truncated to microseconds by decision",
        )

    if decision == TypeDecision.NULL_AND_LOG:
        return ResolvedMapping(
            type_string=string_type,
            fallback_used=True,
            data_loss_risk=True,  # unrepresentable values become null
            decision=decision,
            reason="unrepresentable values set to null by decision",
        )

    # Defensive: an unrecognised decision should never reach here because the
    # config is validated, but failing closed is the safe choice.
    raise SchemaError(
        f"Unsupported data_type_handling decision '{decision}' for "
        f"'{table}.{column}'.",
        context={"table": table, "column": column},
    )
