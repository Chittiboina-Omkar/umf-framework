"""Source type mappers: native source types to the canonical model.

A :class:`SourceTypeMapper` converts a :class:`~umf.typing.source_column.
SourceColumn` into a :class:`~umf.typing.canonical.CanonicalType`. There is one
mapper per source family. The :class:`GenericTypeMapper` here understands the
common ANSI / JDBC type names and is the base other mappers extend, overriding
only the handful of types that are special to their source (for example Oracle's
unbounded ``NUMBER``).

Design choices:
    * Matching is on the lower-cased native type name, with the parenthesised
      arguments (``varchar(50)``) stripped off, because precision, scale and
      length arrive as separate fields on the column, not embedded in the name.
    * When a type is genuinely unknown to a mapper, it returns a canonical type
      of kind ``UNKNOWN`` rather than guessing. The mapping engine then stops
      and asks for a decision. This is the "never silent" rule applied at the
      very first hop.
"""

from __future__ import annotations

import re
from typing import Callable, Dict

from umf.typing.canonical import CanonicalKind, CanonicalType
from umf.typing.source_column import SourceColumn


class SourceTypeMapper:
    """Interface for mapping a source column to a canonical type.

    Attributes:
        source_name: A short identifier for the source family (for example
            ``"oracle"``), used in diagnostics.
    """

    source_name: str = "generic"

    def to_canonical(self, column: SourceColumn) -> CanonicalType:
        """Map a source column to its canonical type.

        Args:
            column: The source column to map.

        Returns:
            The canonical type.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


# A base type name is the native type with any parenthesised argument removed,
# for example "varchar(50)" -> "varchar". We strip arguments because precision,
# scale and length are supplied as structured fields on the column.
_ARGS = re.compile(r"\(.*\)")


def base_type_name(native_type: str) -> str:
    """Return the native type name without its parenthesised arguments.

    Args:
        native_type: The native type string (possibly with arguments).

    Returns:
        The lower-cased base name, for example ``"number"`` from
        ``"number(10,2)"``.
    """
    return _ARGS.sub("", native_type).strip().lower()


class GenericTypeMapper(SourceTypeMapper):
    """Maps common ANSI / JDBC type names to the canonical model.

    This covers the type names shared by most relational databases. Source
    specific mappers subclass this and override :meth:`special_case` for the
    few types unique to their system.
    """

    source_name = "generic"

    def to_canonical(self, column: SourceColumn) -> CanonicalType:
        """Map a column, trying source-specific rules first, then common rules.

        Args:
            column: The source column.

        Returns:
            The canonical type. Kind is :attr:`CanonicalKind.UNKNOWN` if no rule
            matched, which signals the engine to ask for a decision.
        """
        # Let a subclass handle its special types first.
        special = self.special_case(column)
        if special is not None:
            return special
        return self._common(column)

    def special_case(self, column: SourceColumn):
        """Hook for source-specific types. Returns ``None`` by default.

        Subclasses override this to handle types unique to their source, and
        return ``None`` for everything else so the common rules apply.

        Args:
            column: The source column.

        Returns:
            A :class:`CanonicalType`, or ``None`` to defer to the common rules.
        """
        return None

    def _common(self, column: SourceColumn) -> CanonicalType:
        """Apply the common ANSI / JDBC mapping rules.

        Args:
            column: The source column.

        Returns:
            The canonical type, or an ``UNKNOWN`` canonical type if unrecognised.
        """
        name = base_type_name(column.native_type)
        nullable = column.nullable

        handler = self._COMMON.get(name)
        if handler is not None:
            return handler(self, column)

        # Unrecognised: do not guess. Record the native name for diagnostics.
        return CanonicalType(
            kind=CanonicalKind.UNKNOWN,
            nullable=nullable,
            note=f"unrecognised source type '{column.native_type}'",
        )

    # --- helpers for the common handlers ---------------------------------- #

    def _decimal(self, column: SourceColumn) -> CanonicalType:
        """Map a decimal / numeric column, carrying precision and scale."""
        return CanonicalType(
            kind=CanonicalKind.DECIMAL,
            precision=column.precision,
            scale=column.scale if column.scale is not None else 0,
            nullable=column.nullable,
        )

    def _string(self, column: SourceColumn) -> CanonicalType:
        """Map a character column, carrying its declared length."""
        return CanonicalType(
            kind=CanonicalKind.STRING,
            length=column.length,
            nullable=column.nullable,
        )

    def _simple(self, kind: CanonicalKind) -> Callable[["GenericTypeMapper", SourceColumn], CanonicalType]:
        """Build a handler that maps to a simple kind with no parameters.

        Args:
            kind: The canonical kind to map to.

        Returns:
            A handler function.
        """
        def handler(self_: "GenericTypeMapper", column: SourceColumn) -> CanonicalType:
            return CanonicalType(kind=kind, nullable=column.nullable)

        return handler

    def _timestamp(self, with_tz: bool) -> Callable[["GenericTypeMapper", SourceColumn], CanonicalType]:
        """Build a handler for a timestamp, carrying fractional-second digits.

        Args:
            with_tz: Whether the timestamp carries a time zone.

        Returns:
            A handler function.
        """
        def handler(self_: "GenericTypeMapper", column: SourceColumn) -> CanonicalType:
            return CanonicalType(
                kind=CanonicalKind.TIMESTAMP_TZ if with_tz else CanonicalKind.TIMESTAMP,
                precision=column.precision,  # fractional-second digits, if given
                nullable=column.nullable,
                with_tz=with_tz,
            )

        return handler

    # Mapping from a base type name to a handler. Defined as a class attribute so
    # it is built once. Handlers take (self, column) and return a CanonicalType.
    _COMMON: Dict[str, Callable[["GenericTypeMapper", SourceColumn], CanonicalType]] = {
        # booleans
        "boolean": lambda self, c: self._simple(CanonicalKind.BOOLEAN)(self, c),
        "bool": lambda self, c: self._simple(CanonicalKind.BOOLEAN)(self, c),
        "bit": lambda self, c: self._simple(CanonicalKind.BOOLEAN)(self, c),
        # integers
        "tinyint": lambda self, c: self._simple(CanonicalKind.BYTE)(self, c),
        "smallint": lambda self, c: self._simple(CanonicalKind.SHORT)(self, c),
        "int": lambda self, c: self._simple(CanonicalKind.INT)(self, c),
        "integer": lambda self, c: self._simple(CanonicalKind.INT)(self, c),
        "bigint": lambda self, c: self._simple(CanonicalKind.LONG)(self, c),
        # floating point
        "real": lambda self, c: self._simple(CanonicalKind.FLOAT)(self, c),
        "float": lambda self, c: self._simple(CanonicalKind.DOUBLE)(self, c),
        "double": lambda self, c: self._simple(CanonicalKind.DOUBLE)(self, c),
        "double precision": lambda self, c: self._simple(CanonicalKind.DOUBLE)(self, c),
        # exact decimals
        "decimal": lambda self, c: self._decimal(c),
        "numeric": lambda self, c: self._decimal(c),
        "number": lambda self, c: self._decimal(c),  # generic; Oracle overrides
        # character
        "char": lambda self, c: self._string(c),
        "nchar": lambda self, c: self._string(c),
        "varchar": lambda self, c: self._string(c),
        "nvarchar": lambda self, c: self._string(c),
        "varchar2": lambda self, c: self._string(c),
        "text": lambda self, c: self._string(c),
        "clob": lambda self, c: self._string(c),
        "string": lambda self, c: self._string(c),
        # binary
        "binary": lambda self, c: self._simple(CanonicalKind.BINARY)(self, c),
        "varbinary": lambda self, c: self._simple(CanonicalKind.BINARY)(self, c),
        "blob": lambda self, c: self._simple(CanonicalKind.BINARY)(self, c),
        "bytes": lambda self, c: self._simple(CanonicalKind.BINARY)(self, c),
        # temporal
        "date": lambda self, c: self._simple(CanonicalKind.DATE)(self, c),
        "time": lambda self, c: self._simple(CanonicalKind.TIME)(self, c),
        "timestamp": lambda self, c: self._timestamp(False)(self, c),
        "datetime": lambda self, c: self._timestamp(False)(self, c),
        "timestamptz": lambda self, c: self._timestamp(True)(self, c),
        "timestamp with time zone": lambda self, c: self._timestamp(True)(self, c),
        # structured and special
        "json": lambda self, c: self._simple(CanonicalKind.JSON)(self, c),
        "jsonb": lambda self, c: self._simple(CanonicalKind.JSON)(self, c),
        "uuid": lambda self, c: self._simple(CanonicalKind.UUID)(self, c),
        "interval": lambda self, c: self._simple(CanonicalKind.INTERVAL)(self, c),
    }
