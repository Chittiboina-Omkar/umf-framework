"""Type mapping (layer L2): exact first, never silent.

This sub-package maps a source's native column types to Delta or Iceberg target
types through a neutral canonical model, and it tells the truth about every
mapping: whether it is exact, whether a fallback was used, and whether any
information could be lost. When a type cannot be mapped exactly, it is not
guessed; the column fails and the details are surfaced, unless the config's
``data_type_handling`` records an explicit decision.

Public surface:
    CanonicalType, CanonicalKind   the neutral type model
    SourceColumn                   the native-type descriptor a reader produces
    get_mapper / register_mapper   source type mappers by family
    resolve_target                 canonical -> target type, with fidelity flags
    HandlingConfig, TypeDecision   the data_type_handling policy
    TypeMappingEngine, MappingPlan the engine that maps a whole table
"""

from umf.typing.canonical import CanonicalKind, CanonicalType
from umf.typing.engine import MappingEntry, MappingIssue, MappingPlan, TypeMappingEngine
from umf.typing.mappers import get_mapper, register_mapper
from umf.typing.source_column import SourceColumn
from umf.typing.target_types import TargetType, resolve_target
from umf.typing.unmappable import HandlingConfig, TypeDecision

__all__ = [
    "CanonicalType",
    "CanonicalKind",
    "SourceColumn",
    "get_mapper",
    "register_mapper",
    "resolve_target",
    "TargetType",
    "HandlingConfig",
    "TypeDecision",
    "TypeMappingEngine",
    "MappingPlan",
    "MappingEntry",
    "MappingIssue",
]
