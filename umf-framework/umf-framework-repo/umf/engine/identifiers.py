"""Table identifiers: naming a table consistently across platforms.

A target table is addressed by up to three parts: an optional catalog, a schema
(namespace) and the table name. Platforms spell this differently (Unity uses
``catalog.schema.table``; a plain Hive metastore uses ``schema.table``; a
path-based setup has no catalog at all). A :class:`TableIdentifier` captures the
parts once and builds the right qualified name, so the rest of the framework
never hand-assembles table names and never writes to the wrong place because of
a naming mistake.

The module also provides conservative name sanitisation. Writing to the wrong
table is destructive, so an identifier is validated up front: parts must be
non-empty and must not contain characters that could change which object is
addressed (dots, quotes, backticks, whitespace). This is a guardrail, not a
convenience; a suspicious name fails loudly rather than being silently rewritten.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from umf.core.exceptions import ConfigError

# A safe identifier part: letters, digits and underscore, not starting with a
# digit. This is the portable intersection that every target engine accepts
# without quoting. Anything outside it is refused rather than guessed at.
_SAFE_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_part(part: str, role: str) -> str:
    """Validate one identifier part, returning it unchanged if acceptable.

    Args:
        part: The catalog, schema or table name.
        role: Which part it is (for the error message).

    Returns:
        The validated part.

    Raises:
        ConfigError: If the part is empty or contains unsafe characters. Writing
            to a wrongly-named table is destructive, so this fails loudly.
    """
    if not part or not part.strip():
        raise ConfigError(f"Table {role} must not be empty.")
    cleaned = part.strip()
    if not _SAFE_PART.match(cleaned):
        raise ConfigError(
            f"Table {role} '{part}' contains characters that are not allowed "
            "(use letters, digits and underscore, not starting with a digit). "
            "This guard prevents accidentally addressing the wrong object."
        )
    return cleaned


@dataclass(frozen=True)
class TableIdentifier:
    """A fully specified target table name.

    Attributes:
        schema: The schema / namespace (required).
        table: The table name (required).
        catalog: The catalog, or ``None`` on path-based / catalog-less targets.
    """

    schema: str
    table: str
    catalog: Optional[str] = None

    @staticmethod
    def create(
        *, schema: str, table: str, catalog: Optional[str] = None
    ) -> "TableIdentifier":
        """Validate the parts and build an identifier.

        Args:
            schema: The schema / namespace.
            table: The table name.
            catalog: The catalog, or ``None`` if there is none.

        Returns:
            The validated :class:`TableIdentifier`.

        Raises:
            ConfigError: If any part is empty or unsafe.
        """
        return TableIdentifier(
            schema=_check_part(schema, "schema"),
            table=_check_part(table, "table"),
            catalog=_check_part(catalog, "catalog") if catalog else None,
        )

    def qualified(self) -> str:
        """Return the fully qualified name for use in SQL and table APIs.

        Returns:
            ``catalog.schema.table`` when a catalog is set, otherwise
            ``schema.table``.
        """
        if self.catalog:
            return f"{self.catalog}.{self.schema}.{self.table}"
        return f"{self.schema}.{self.table}"

    def staging(self, run_id: str) -> "TableIdentifier":
        """Return a staging identifier derived from this target and a run id.

        Staged data is written to a separate, per-run table so that the target
        is only touched by the final atomic publish. Including the run id keeps
        two concurrent runs from colliding on the same staging table.

        Args:
            run_id: The run identifier, used to make the staging name unique.

        Returns:
            A new :class:`TableIdentifier` in the same schema and catalog, named
            ``_stg_<table>_<short run id>``.
        """
        # Use a short, safe slice of the run id; run ids are hex, so they are
        # already safe characters.
        suffix = re.sub(r"[^A-Za-z0-9_]", "", run_id)[:12]
        return TableIdentifier(
            schema=self.schema,
            table=f"_stg_{self.table}_{suffix}",
            catalog=self.catalog,
        )
