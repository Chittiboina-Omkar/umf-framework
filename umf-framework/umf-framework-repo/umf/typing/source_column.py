"""The native-type descriptor a source reader produces for each column.

Before the framework can map types, it needs the source's own description of
each column: its native type name and, where the source provides them, the
precision, scale, length and nullability. A :class:`SourceColumn` carries
exactly that. It is plain data (no Spark, no SDK), so schema-mapping logic can
be written and tested with hand-built columns, independent of any live source.

A source reader (for example the file reader, or a database reader added later)
is responsible for producing a list of :class:`SourceColumn` from the source's
metadata. The warehouse / database case is the rich one, because the source
catalog exposes precision, scale and length; the plain-CSV case is the poor one,
because a CSV carries no type information and the columns must come from a
contract or be inferred, which is recorded as the schema origin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SourceColumn:
    """One column as described by the source system.

    Attributes:
        name: The column name exactly as the source reports it.
        native_type: The source's own type name, lower-cased by convention (for
            example ``"number"``, ``"varchar2"``, ``"timestamp"``). Source
            mappers match on this.
        precision: Numeric precision, if the source provides it. For a decimal
            this is total digits; for a timestamp it may be fractional-second
            digits.
        scale: Numeric scale (digits after the point), if provided.
        length: Character or binary length, if provided.
        nullable: Whether the source allows nulls in this column. Defaults to
            ``True`` because most sources default to nullable.
        ordinal: The column's position in the table (0-based), used to preserve
            order and to help detect reorder or rename drift later.
        raw: Any extra source-specific metadata, kept for diagnostics (for
            example a source's full type string, or flags). Never used for
            mapping decisions directly.
    """

    name: str
    native_type: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    nullable: bool = True
    ordinal: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def normalised_type(self) -> str:
        """Return the native type name trimmed and lower-cased for matching.

        Source mappers compare against this so that ``"VARCHAR2"`` and
        ``" varchar2 "`` match the same rule.

        Returns:
            The cleaned native type name.
        """
        return self.native_type.strip().lower()
