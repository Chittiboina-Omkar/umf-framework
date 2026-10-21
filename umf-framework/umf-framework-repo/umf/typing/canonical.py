"""The canonical type model: a neutral middle between source and target types.

Every source system names its types differently (Oracle ``NUMBER``, SQL Server
``money``, Snowflake ``VARIANT``), and every target format (Delta, Iceberg) has
its own type system too. Mapping every source directly to every target would be
an N by M problem. Instead we map in two hops through a single neutral model:

    source native type  ->  CanonicalType  ->  target type

This turns the problem into N source mappers plus M target mappers, and it gives
one obvious place to reason about precision, scale, length and nullability,
which is where correctness lives.

The canonical model deliberately captures the facts that determine whether a
mapping can be exact:

    * ``kind``       the broad type family,
    * ``precision``  total significant digits for a decimal, or fractional
                     second digits for a timestamp,
    * ``scale``      digits after the decimal point for a decimal,
    * ``length``     declared length for a character type,
    * ``nullable``   whether nulls are allowed,
    * ``with_tz``    whether a timestamp carries a time zone.

If a source type cannot be represented in the canonical model without losing one
of these facts, that is recorded as :attr:`CanonicalKind.UNKNOWN` so the mapping
engine stops and asks for a decision rather than guessing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from umf.core.constants import _StrEnum


class CanonicalKind(_StrEnum):
    """The broad family of a canonical type.

    Integer kinds are kept distinct (``BYTE``, ``SHORT``, ``INT``, ``LONG``) so a
    target that lacks a narrow integer type can widen it knowingly, recording
    that a widening happened even though no value is lost.
    """

    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"            # 8-bit integer (for example SQL Server tinyint)
    SHORT = "SHORT"          # 16-bit integer
    INT = "INT"             # 32-bit integer
    LONG = "LONG"            # 64-bit integer
    FLOAT = "FLOAT"          # 32-bit floating point
    DOUBLE = "DOUBLE"        # 64-bit floating point
    DECIMAL = "DECIMAL"      # exact decimal with precision and scale
    STRING = "STRING"        # variable-length text
    BINARY = "BINARY"        # raw bytes
    DATE = "DATE"            # calendar date, no time
    TIME = "TIME"            # time of day, no date
    TIMESTAMP = "TIMESTAMP"  # date and time, no time zone
    TIMESTAMP_TZ = "TIMESTAMP_TZ"  # date and time with a time zone / offset
    INTERVAL = "INTERVAL"    # a span of time
    JSON = "JSON"            # structured document / variant
    GEOGRAPHY = "GEOGRAPHY"  # geospatial value
    UUID = "UUID"            # universally unique identifier
    ARRAY = "ARRAY"          # list of a single element type
    STRUCT = "STRUCT"        # record with named fields
    MAP = "MAP"              # key / value map
    UNKNOWN = "UNKNOWN"      # could not be represented without losing facts


# Kinds that are integers, used by target mappers to decide widening.
INTEGER_KINDS = frozenset(
    {CanonicalKind.BYTE, CanonicalKind.SHORT, CanonicalKind.INT, CanonicalKind.LONG}
)

# Relative width of each integer kind, so a target can widen (never narrow).
INTEGER_WIDTH = {
    CanonicalKind.BYTE: 1,
    CanonicalKind.SHORT: 2,
    CanonicalKind.INT: 4,
    CanonicalKind.LONG: 8,
}

# The maximum decimal precision supported by Spark, Delta and Iceberg. A source
# decimal whose precision exceeds this cannot be represented exactly and must be
# handled by an explicit decision, never silently truncated.
MAX_DECIMAL_PRECISION = 38

# The maximum fractional-second precision (digits) representable in Spark / Delta
# / Iceberg timestamps, which is microseconds. A source timestamp with more
# fractional digits (for example Oracle's 9) loses precision and needs a
# decision.
MAX_TIMESTAMP_FRACTIONAL_DIGITS = 6


@dataclass(frozen=True)
class CanonicalType:
    """A source type expressed in the neutral canonical model.

    Attributes:
        kind: The broad :class:`CanonicalKind`.
        precision: For ``DECIMAL``, the total number of significant digits. For
            ``TIMESTAMP`` / ``TIMESTAMP_TZ``, the number of fractional-second
            digits. ``None`` when not applicable.
        scale: For ``DECIMAL``, the number of digits after the decimal point.
            ``None`` when not applicable.
        length: For ``STRING`` / ``BINARY``, the declared length. ``None`` means
            unbounded or unknown.
        nullable: Whether nulls are allowed.
        with_tz: For timestamps, whether a time zone is carried. (Captured by the
            kind too, but kept explicit for clarity.)
        note: Optional free-form note from the source mapper, for diagnostics.
    """

    kind: CanonicalKind
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    nullable: bool = True
    with_tz: bool = False
    note: str = ""

    def describe(self) -> str:
        """Return a short, human-readable description of the type.

        Used in validation messages and the type-mapping log so a reader sees
        exactly what was detected.

        Returns:
            A compact description, for example ``"DECIMAL(42,4)"`` or
            ``"TIMESTAMP(9)"``.
        """
        if self.kind == CanonicalKind.DECIMAL:
            return f"DECIMAL({self.precision},{self.scale})"
        if self.kind in (CanonicalKind.TIMESTAMP, CanonicalKind.TIMESTAMP_TZ):
            frac = f"({self.precision})" if self.precision is not None else ""
            return f"{self.kind}{frac}"
        if self.kind in (CanonicalKind.STRING, CanonicalKind.BINARY) and self.length:
            return f"{self.kind}({self.length})"
        return str(self.kind)
