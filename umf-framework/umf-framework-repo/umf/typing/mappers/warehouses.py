"""Type mappers for more warehouses and databases.

Each class overrides only the types that are special to its system and defers
the rest to the generic ANSI rules. The special cases are the ones where a naive
mapping would lose data or type meaning, so they are handled explicitly and,
where no faithful target exists, mapped to ``UNKNOWN`` so the engine asks for a
decision rather than guessing.

All of these are pure Python and tested with synthetic columns.
"""

from __future__ import annotations

from typing import Optional

from umf.typing.canonical import CanonicalKind, CanonicalType
from umf.typing.mappers.generic import GenericTypeMapper, base_type_name
from umf.typing.source_column import SourceColumn


class SnowflakeTypeMapper(GenericTypeMapper):
    """Maps Snowflake native types."""

    source_name = "snowflake"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle Snowflake-specific types.

        Args:
            column: The source column.

        Returns:
            A canonical type for a Snowflake-special type, or ``None`` to defer.
        """
        name = base_type_name(column.native_type)
        n = column.nullable
        if name == "number":
            # Snowflake NUMBER defaults to (38,0); scale 0 is an integer-like
            # exact decimal. Carry precision / scale through.
            return CanonicalType(
                CanonicalKind.DECIMAL,
                precision=column.precision if column.precision is not None else 38,
                scale=column.scale if column.scale is not None else 0,
                nullable=n,
            )
        if name in ("timestamp_ntz", "timestamp_ltz", "timestamp_tz"):
            with_tz = name != "timestamp_ntz"
            return CanonicalType(
                CanonicalKind.TIMESTAMP_TZ if with_tz else CanonicalKind.TIMESTAMP,
                precision=column.precision if column.precision is not None else 9,
                nullable=n,
                with_tz=with_tz,
            )
        if name in ("variant", "object", "array"):
            return CanonicalType(CanonicalKind.JSON, nullable=n,
                                 note=f"snowflake {name}")
        if name == "geography" or name == "geometry":
            return CanonicalType(CanonicalKind.UNKNOWN, nullable=n,
                                 note=f"snowflake {name} has no exact target")
        return None


class BigQueryTypeMapper(GenericTypeMapper):
    """Maps BigQuery native types."""

    source_name = "bigquery"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle BigQuery-specific types.

        Args:
            column: The source column.

        Returns:
            A canonical type, or ``None`` to defer.
        """
        name = base_type_name(column.native_type)
        n = column.nullable
        if name == "bignumeric":
            # Up to 76 digits: cannot be represented exactly, so no precision is
            # carried and the engine flags it for a decision.
            return CanonicalType(CanonicalKind.DECIMAL, precision=None,
                                 scale=column.scale, nullable=n,
                                 note="bigquery BIGNUMERIC exceeds target precision")
        if name == "numeric":
            return CanonicalType(
                CanonicalKind.DECIMAL,
                precision=column.precision if column.precision is not None else 38,
                scale=column.scale if column.scale is not None else 9,
                nullable=n,
            )
        if name == "datetime":
            return CanonicalType(CanonicalKind.TIMESTAMP, nullable=n)
        if name == "timestamp":
            return CanonicalType(CanonicalKind.TIMESTAMP_TZ, nullable=n, with_tz=True)
        if name == "json":
            return CanonicalType(CanonicalKind.JSON, nullable=n)
        if name == "geography":
            return CanonicalType(CanonicalKind.UNKNOWN, nullable=n,
                                 note="bigquery GEOGRAPHY has no exact target")
        if name == "bytes":
            return CanonicalType(CanonicalKind.BINARY, nullable=n)
        return None


class MySqlTypeMapper(GenericTypeMapper):
    """Maps MySQL / MariaDB native types, including unsigned integers."""

    source_name = "mysql"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle MySQL-specific types.

        Unsigned integers are widened to the next size up, because an unsigned
        value can exceed the signed maximum of the same width; ``unsigned
        bigint`` has no larger integer and becomes an exact decimal.

        Args:
            column: The source column. The ``raw`` metadata may carry an
                ``unsigned`` flag, or the native type text may contain
                "unsigned".

        Returns:
            A canonical type, or ``None`` to defer.
        """
        # MySQL integer types carry modifiers such as "unsigned" and "zerofill"
        # after the type name (for example "bigint unsigned"). Match on the first
        # token so the modifier does not defeat the lookup, and read the modifier
        # separately.
        name = base_type_name(column.native_type).split()[0] if column.native_type.strip() else ""
        n = column.nullable
        unsigned = "unsigned" in column.native_type.lower() or bool(
            column.raw.get("unsigned")
        )

        if name == "tinyint":
            # tinyint(1) is MySQL's boolean idiom.
            if column.raw.get("display_width") == 1 or column.length == 1:
                return CanonicalType(CanonicalKind.BOOLEAN, nullable=n)
            return CanonicalType(
                CanonicalKind.SHORT if unsigned else CanonicalKind.BYTE, nullable=n
            )
        if name == "smallint":
            return CanonicalType(
                CanonicalKind.INT if unsigned else CanonicalKind.SHORT, nullable=n
            )
        if name in ("int", "integer", "mediumint"):
            return CanonicalType(
                CanonicalKind.LONG if unsigned else CanonicalKind.INT, nullable=n
            )
        if name == "bigint":
            if unsigned:
                # No larger integer; keep every digit with an exact decimal.
                return CanonicalType(CanonicalKind.DECIMAL, precision=20, scale=0, nullable=n)
            return CanonicalType(CanonicalKind.LONG, nullable=n)
        if name == "year":
            return CanonicalType(CanonicalKind.SHORT, nullable=n)
        if name in ("enum", "set"):
            return CanonicalType(CanonicalKind.STRING, nullable=n)
        if name == "json":
            return CanonicalType(CanonicalKind.JSON, nullable=n)
        if name in ("datetime", "timestamp"):
            return CanonicalType(CanonicalKind.TIMESTAMP,
                                 precision=column.precision, nullable=n)
        return None


class PostgresTypeMapper(GenericTypeMapper):
    """Maps PostgreSQL native types."""

    source_name = "postgresql"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle PostgreSQL-specific types.

        Args:
            column: The source column.

        Returns:
            A canonical type, or ``None`` to defer.
        """
        name = base_type_name(column.native_type)
        n = column.nullable
        if name == "numeric" and column.precision is None:
            # Unbounded numeric: no precision to map exactly.
            return CanonicalType(CanonicalKind.DECIMAL, precision=None,
                                 scale=column.scale, nullable=n,
                                 note="postgres unbounded numeric")
        if name == "money":
            return CanonicalType(CanonicalKind.DECIMAL, precision=19, scale=2, nullable=n)
        if name == "jsonb":
            return CanonicalType(CanonicalKind.JSON, nullable=n)
        if name == "uuid":
            return CanonicalType(CanonicalKind.UUID, nullable=n)
        if name in ("timetz", "time with time zone"):
            return CanonicalType(CanonicalKind.TIME, nullable=n)
        if name in ("timestamptz", "timestamp with time zone"):
            return CanonicalType(CanonicalKind.TIMESTAMP_TZ, nullable=n, with_tz=True)
        if name in ("inet", "cidr", "macaddr"):
            return CanonicalType(CanonicalKind.STRING, nullable=n,
                                 note=f"postgres {name} stored as string")
        if name == "bytea":
            return CanonicalType(CanonicalKind.BINARY, nullable=n)
        return None


class TeradataTypeMapper(GenericTypeMapper):
    """Maps Teradata native types."""

    source_name = "teradata"

    def special_case(self, column: SourceColumn) -> Optional[CanonicalType]:
        """Handle Teradata-specific types.

        Args:
            column: The source column.

        Returns:
            A canonical type, or ``None`` to defer.
        """
        name = base_type_name(column.native_type)
        n = column.nullable
        if name == "byteint":
            return CanonicalType(CanonicalKind.BYTE, nullable=n)
        if name == "number":
            return CanonicalType(
                CanonicalKind.DECIMAL,
                precision=column.precision,
                scale=column.scale if column.scale is not None else 0,
                nullable=n,
            )
        if name.startswith("period"):
            # A PERIOD has a begin and end; it has no single scalar target.
            return CanonicalType(CanonicalKind.UNKNOWN, nullable=n,
                                 note="teradata PERIOD needs two columns or a decision")
        if name == "timestamp":
            return CanonicalType(CanonicalKind.TIMESTAMP,
                                 precision=column.precision if column.precision is not None else 6,
                                 nullable=n)
        return None
