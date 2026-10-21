"""Oracle source type mapper.

Oracle has several types that do not map the way their names suggest, and
getting them wrong silently corrupts data. This mapper overrides only those
special cases and defers everything else to the generic ANSI rules.

The notable Oracle cases handled here:

    * ``NUMBER`` with no precision is Oracle's "floating" number and can hold
      more significant digits than a target decimal supports. It is mapped to a
      canonical ``DECIMAL`` with no precision, which the engine recognises as
      needing a decision (never a silent truncation).
    * ``NUMBER(p)`` with no scale is an integer; ``NUMBER(p,s)`` is a decimal.
    * Oracle ``DATE`` actually carries a time component, so it maps to a
      canonical timestamp, not a date, to avoid dropping the time.
    * ``TIMESTAMP(9)`` carries nanoseconds; the fractional digits are preserved
      in the canonical type so the engine can see that they exceed what the
      target supports and ask for a decision.
    * ``ROWID``, ``XMLTYPE`` and ``LONG`` have no faithful scalar target and are
      left for an explicit decision rather than guessed.
"""

from __future__ import annotations

from typing import Optional

from umf.typing.canonical import CanonicalKind, CanonicalType
from umf.typing.mappers.generic import GenericTypeMapper, base_type_name
from umf.typing.source_column import SourceColumn


class OracleTypeMapper(GenericTypeMapper):
    """Maps Oracle native types to the canonical model."""

    source_name = "oracle"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle Oracle-specific types, deferring the rest to the generic rules.

        Args:
            column: The source column.

        Returns:
            A canonical type for an Oracle-special type, or ``None`` to defer.
        """
        name = base_type_name(column.native_type)

        if name == "number":
            return self._oracle_number(column)

        if name == "date":
            # Oracle DATE includes a time component; map to timestamp so the time
            # is not silently dropped. No fractional seconds on a plain DATE.
            return CanonicalType(
                kind=CanonicalKind.TIMESTAMP,
                precision=0,
                nullable=column.nullable,
                note="oracle DATE carries a time component",
            )

        if name in ("rowid", "urowid", "xmltype", "long", "long raw"):
            # No faithful scalar target: leave for an explicit decision.
            return CanonicalType(
                kind=CanonicalKind.UNKNOWN,
                nullable=column.nullable,
                note=f"oracle type '{column.native_type}' has no exact target",
            )

        # Everything else (varchar2, timestamp, clob, ...) uses the generic rules.
        return None

    def _oracle_number(self, column: SourceColumn) -> CanonicalType:
        """Map Oracle ``NUMBER`` in its several forms.

        Args:
            column: The source column with its precision and scale.

        Returns:
            * An integer kind when scale is zero and precision fits a native
              integer width (so ``NUMBER(5,0)`` becomes a small integer, not a
              decimal with scale 0).
            * A ``DECIMAL`` carrying precision and scale otherwise.
            * A ``DECIMAL`` with no precision when Oracle reports none, which the
              engine treats as needing a decision.
        """
        precision = column.precision
        scale = column.scale

        # NUMBER with no precision: Oracle's floating number. Keep it decimal
        # with unknown precision so the engine flags it for a decision rather
        # than inventing a precision.
        if precision is None:
            return CanonicalType(
                kind=CanonicalKind.DECIMAL,
                precision=None,
                scale=scale,
                nullable=column.nullable,
                note="oracle NUMBER with no declared precision",
            )

        # NUMBER(p) or NUMBER(p,0): an integer. Choose the narrowest integer kind
        # that certainly holds p digits, so downstream widening is explicit.
        if (scale or 0) == 0:
            if precision <= 2:
                kind = CanonicalKind.BYTE
            elif precision <= 4:
                kind = CanonicalKind.SHORT
            elif precision <= 9:
                kind = CanonicalKind.INT
            elif precision <= 18:
                kind = CanonicalKind.LONG
            else:
                # Too many digits for a 64-bit integer: keep as an exact decimal.
                return CanonicalType(
                    kind=CanonicalKind.DECIMAL,
                    precision=precision,
                    scale=0,
                    nullable=column.nullable,
                )
            return CanonicalType(kind=kind, nullable=column.nullable)

        # NUMBER(p,s): an exact decimal.
        return CanonicalType(
            kind=CanonicalKind.DECIMAL,
            precision=precision,
            scale=scale,
            nullable=column.nullable,
        )
