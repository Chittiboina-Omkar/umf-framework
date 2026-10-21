"""Source type mappers and a registry to pick one by source name.

Each mapper converts a source's native types into the canonical model. The
registry lets the engine ask for "the mapper for Oracle" by name, and falls back
to the generic ANSI mapper when a source has no specialised mapper yet. New
sources are added by writing a mapper and registering it here; nothing else
changes.
"""

from __future__ import annotations

from typing import Dict

from umf.typing.mappers.generic import GenericTypeMapper, SourceTypeMapper
from umf.typing.mappers.oracle import OracleTypeMapper
from umf.typing.mappers.sqlserver import SqlServerTypeMapper
from umf.typing.mappers.warehouses import (
    BigQueryTypeMapper,
    MySqlTypeMapper,
    PostgresTypeMapper,
    SnowflakeTypeMapper,
    TeradataTypeMapper,
)

# Registry of source name to mapper instance. Mappers are stateless, so a single
# shared instance each is fine.
_REGISTRY: Dict[str, SourceTypeMapper] = {
    GenericTypeMapper.source_name: GenericTypeMapper(),
    OracleTypeMapper.source_name: OracleTypeMapper(),
    SqlServerTypeMapper.source_name: SqlServerTypeMapper(),
    SnowflakeTypeMapper.source_name: SnowflakeTypeMapper(),
    BigQueryTypeMapper.source_name: BigQueryTypeMapper(),
    MySqlTypeMapper.source_name: MySqlTypeMapper(),
    PostgresTypeMapper.source_name: PostgresTypeMapper(),
    TeradataTypeMapper.source_name: TeradataTypeMapper(),
}


def get_mapper(source_name: str) -> SourceTypeMapper:
    """Return the mapper for a source family, or the generic mapper.

    Args:
        source_name: The source family name (for example ``"oracle"``). Matched
            case-insensitively.

    Returns:
        The matching :class:`SourceTypeMapper`, or the generic mapper when no
        specialised mapper is registered for the source. Falling back to generic
        is safe because the generic mapper returns ``UNKNOWN`` for anything it
        does not recognise, which makes the engine ask for a decision rather
        than guess.
    """
    return _REGISTRY.get(source_name.strip().lower(), _REGISTRY[GenericTypeMapper.source_name])


def register_mapper(mapper: SourceTypeMapper) -> None:
    """Register (or replace) a mapper by its source name.

    Args:
        mapper: The mapper to register. Its ``source_name`` is the key.
    """
    _REGISTRY[mapper.source_name.strip().lower()] = mapper


__all__ = [
    "SourceTypeMapper",
    "GenericTypeMapper",
    "OracleTypeMapper",
    "SqlServerTypeMapper",
    "SnowflakeTypeMapper",
    "BigQueryTypeMapper",
    "MySqlTypeMapper",
    "PostgresTypeMapper",
    "TeradataTypeMapper",
    "get_mapper",
    "register_mapper",
]
