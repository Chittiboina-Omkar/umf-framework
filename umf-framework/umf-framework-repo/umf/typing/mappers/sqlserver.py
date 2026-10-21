"""SQL Server source type mapper.

Overrides the SQL Server types whose correct mapping is not obvious from the
name, and defers the rest to the generic ANSI rules.

Handled special cases:
    * ``money`` and ``smallmoney`` are fixed-scale currency types; they map to
      exact decimals with the scales SQL Server uses (4 digits).
    * ``datetime`` has a coarse resolution (about 3.33 ms) and ``datetime2`` can
      carry up to 7 fractional digits; both map to a canonical timestamp, with
      ``datetime2``'s fractional digits preserved so the engine can see whether
      they exceed the target's microsecond limit.
    * ``datetimeoffset`` carries a time-zone offset, so it maps to a canonical
      timestamp-with-time-zone.
    * ``uniqueidentifier`` maps to the canonical UUID kind.
    * ``sql_variant`` and ``hierarchyid`` have no faithful scalar target and are
      left for an explicit decision.
"""

from __future__ import annotations

from typing import Optional

from umf.typing.canonical import CanonicalKind, CanonicalType
from umf.typing.mappers.generic import GenericTypeMapper, base_type_name
from umf.typing.source_column import SourceColumn


class SqlServerTypeMapper(GenericTypeMapper):
    """Maps SQL Server native types to the canonical model."""

    source_name = "sqlserver"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle SQL Server-specific types, deferring the rest.

        Args:
            column: The source column.

        Returns:
            A canonical type for a SQL Server-special type, or ``None`` to defer
            to the generic rules.
        """
        name = base_type_name(column.native_type)
        nullable = column.nullable

        if name == "money":
            # SQL Server money has scale 4 and fits in precision 19.
            return CanonicalType(
                CanonicalKind.DECIMAL, precision=19, scale=4, nullable=nullable
            )
        if name == "smallmoney":
            return CanonicalType(
                CanonicalKind.DECIMAL, precision=10, scale=4, nullable=nullable
            )
        if name == "datetime":
            # Coarse resolution; map to timestamp. Note recorded for diagnostics.
            return CanonicalType(
                CanonicalKind.TIMESTAMP,
                precision=3,
                nullable=nullable,
                note="sql server datetime has ~3.33ms resolution",
            )
        if name in ("datetime2", "smalldatetime"):
            return CanonicalType(
                CanonicalKind.TIMESTAMP,
                precision=column.precision if column.precision is not None else 7,
                nullable=nullable,
            )
        if name == "datetimeoffset":
            return CanonicalType(
                CanonicalKind.TIMESTAMP_TZ,
                precision=column.precision if column.precision is not None else 7,
                nullable=nullable,
                with_tz=True,
            )
        if name == "uniqueidentifier":
            return CanonicalType(CanonicalKind.UUID, nullable=nullable)
        if name in ("sql_variant", "hierarchyid", "geography", "geometry"):
            return CanonicalType(
                CanonicalKind.UNKNOWN,
                nullable=nullable,
                note=f"sql server type '{column.native_type}' has no exact target",
            )
        return None
