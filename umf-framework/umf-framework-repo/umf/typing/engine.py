"""The type mapping engine: turn source columns into a target mapping plan.

This is where the three pieces come together for a whole table:

    source columns  --(source mapper)-->  canonical types
    canonical types --(target resolver)-->  target types
    needs-decision  --(unmappable policy)-->  final type or a blocking issue

The engine produces a :class:`MappingPlan`. If every column resolved, the plan
is ready to build a table from. If any column could not be resolved and the
configured decision is to fail, the plan lists those as blocking issues; the
caller then fails that table (status ``NEEDS_DECISION``), surfaces the issues,
and the run continues with the other tables. This is exactly the "fail that
table, never partial, others continue, show the details" behaviour agreed in the
design.

The engine also builds ``type_mapping_log`` records (one per column) using the
Part 2 record builders, so every mapping decision, exact or fallback, is
recorded as evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from umf.control import records
from umf.core.constants import TableFormat
from umf.core.exceptions import SchemaError
from umf.typing.canonical import CanonicalKind, CanonicalType
from umf.typing.mappers import SourceTypeMapper, get_mapper
from umf.typing.source_column import SourceColumn
from umf.typing.target_types import resolve_target
from umf.typing.unmappable import HandlingConfig, ResolvedMapping, apply_decision


@dataclass(frozen=True)
class MappingEntry:
    """The resolved mapping for one column.

    Attributes:
        name: The column name.
        source_type: The native source type, as reported.
        canonical: The canonical type it mapped to.
        target_type: The final target type string.
        exact: Whether the mapping is exact (no loss).
        fallback_used: Whether a fallback or configured decision was applied.
        data_loss_risk: Whether information could be lost.
        decision: The configured decision applied, if any (as text).
        reason: A short explanation of the outcome.
    """

    name: str
    source_type: str
    canonical: CanonicalType
    target_type: str
    exact: bool
    fallback_used: bool
    data_loss_risk: bool
    decision: Optional[str]
    reason: str


@dataclass(frozen=True)
class MappingIssue:
    """A column that could not be resolved and is blocking its table.

    Attributes:
        name: The column name.
        source_type: The native source type.
        detail: A full, non-sensitive explanation of why it is blocked and what
            decision would unblock it.
    """

    name: str
    source_type: str
    detail: str


@dataclass
class MappingPlan:
    """The outcome of mapping a whole table's columns.

    Attributes:
        table: The table name.
        entries: The resolved mappings, in column order.
        issues: The columns that are blocked pending a decision. When non-empty,
            the table must not be built; it fails with ``NEEDS_DECISION``.
    """

    table: str
    entries: List[MappingEntry] = field(default_factory=list)
    issues: List[MappingIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether every column resolved (no blocking issues).

        Returns:
            True if the plan has no issues and can be used to build the table.
        """
        return not self.issues

    def ddl_columns(self) -> List[str]:
        """Return the resolved columns as ``"name type"`` DDL fragments.

        This is a convenience for the table-creation code. It is only meaningful
        when :attr:`ok` is true.

        Returns:
            A list like ``["order_id BIGINT", "amount DECIMAL(10,2)"]``.
        """
        return [f"{e.name} {e.target_type}" for e in self.entries]

    def ddl_columns_pairs(self) -> List[tuple]:
        """Return the resolved columns as ``(name, type_string)`` pairs.

        This is the shape the table writer's ``create_table`` expects. It is
        only meaningful when :attr:`ok` is true.

        Returns:
            A list like ``[("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")]``.
        """
        return [(e.name, e.target_type) for e in self.entries]


class TypeMappingEngine:
    """Maps a table's source columns to target types, honouring the decisions.

    Attributes:
        mapper: The source type mapper for the source family.
        fmt: The target table format.
        handling: The resolved ``data_type_handling`` configuration.
    """

    def __init__(
        self,
        *,
        source_name: str,
        table_format: TableFormat,
        handling: HandlingConfig,
    ) -> None:
        """Create an engine for one source family and target format.

        Args:
            source_name: The source family (for example ``"oracle"``); selects
                the source type mapper.
            table_format: The target table format.
            handling: The resolved ``data_type_handling`` configuration.
        """
        self.mapper: SourceTypeMapper = get_mapper(source_name)
        self.fmt = table_format
        self.handling = handling

    def plan_table(
        self, table: str, columns: Sequence[SourceColumn]
    ) -> MappingPlan:
        """Map every column of a table and return the plan.

        Args:
            table: The table name (used for per-column decisions and messages).
            columns: The source columns in order.

        Returns:
            A :class:`MappingPlan`. If any column is blocked, the plan's
            ``issues`` list is non-empty and its ``entries`` cover only the
            columns that did resolve.
        """
        plan = MappingPlan(table=table)
        for column in columns:
            canonical = self.mapper.to_canonical(column)
            target = resolve_target(canonical, self.fmt)

            try:
                resolved = apply_decision(
                    table=table,
                    column=column.name,
                    canonical=canonical,
                    target=target,
                    fmt=self.fmt,
                    handling=self.handling,
                )
            except SchemaError as exc:
                # A blocking column: record it as an issue and keep mapping the
                # rest, so the user sees every problem in one pass.
                plan.issues.append(
                    MappingIssue(
                        name=column.name,
                        source_type=column.native_type,
                        detail=exc.message,
                    )
                )
                continue

            plan.entries.append(
                MappingEntry(
                    name=column.name,
                    source_type=column.native_type,
                    canonical=canonical,
                    target_type=resolved.type_string,
                    exact=target.exact and resolved.decision is None,
                    fallback_used=resolved.fallback_used,
                    data_loss_risk=resolved.data_loss_risk,
                    decision=None if resolved.decision is None else str(resolved.decision),
                    reason=resolved.reason,
                )
            )
        return plan

    def log_records(self, run_id: str, table: str, plan: MappingPlan) -> List[dict]:
        """Build ``type_mapping_log`` records for a plan.

        One record per resolved column, capturing the source and target types,
        precision / scale / length, exactness, whether a fallback was used, the
        data-loss risk and any decision. This is the evidence trail behind every
        mapping.

        Args:
            run_id: The run the mapping belongs to.
            table: The table name.
            plan: The mapping plan to record.

        Returns:
            A list of validated ``type_mapping_log`` records, ready to write to
            a log sink.
        """
        out: List[dict] = []
        for entry in plan.entries:
            canonical = entry.canonical
            out.append(
                records.type_mapping_record(
                    run_id=run_id,
                    table_name=table,
                    column_name=entry.name,
                    source_type=entry.source_type,
                    target_type=entry.target_type,
                    exact=entry.exact,
                    precision=canonical.precision
                    if canonical.kind == CanonicalKind.DECIMAL
                    else None,
                    scale=canonical.scale
                    if canonical.kind == CanonicalKind.DECIMAL
                    else None,
                    length=canonical.length,
                    fallback_used=entry.fallback_used,
                    data_loss_risk=entry.data_loss_risk,
                    decision=entry.decision,
                )
            )
        return out
