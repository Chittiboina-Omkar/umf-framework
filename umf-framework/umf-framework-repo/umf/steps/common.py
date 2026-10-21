"""Shared helpers used by the step executors.

The step executors are mostly glue that strings together the pieces built in
earlier parts. A few decisions inside them are pure logic, and those are
factored out here so they can be tested without Spark:

    * turning a type mapping plan into either an approved plan or a
      ``NEEDS_DECISION`` outcome (the "fail that table, show the issues, continue
      others" behaviour, decided before any data is read);
    * separating business columns from the framework's audit columns, so
      reconciliation compares only the real data; and
    * building the reconciliation configuration from a table's config.

Keeping these here means the executors stay thin and the important choices are
covered by tests.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple

from umf.core.constants import ReconLevel, StepStatus
from umf.core.run_context import StepOutcome
from umf.recon.compare import ReconConfig
from umf.typing.engine import MappingPlan, TypeMappingEngine
from umf.typing.source_column import SourceColumn


def plan_or_block(
    engine: TypeMappingEngine,
    *,
    step: str,
    table: str,
    source_columns: Sequence[SourceColumn],
) -> Tuple[MappingPlan, Optional[StepOutcome]]:
    """Map the columns, returning either the plan or a blocking outcome.

    This runs before any data is read. If every column mapped, the plan is
    returned with no outcome. If any column is unmappable and no decision is
    configured, a ``NEEDS_DECISION`` outcome is returned describing the issues,
    and the caller returns it immediately so the table fails cleanly (never
    partially) while other tables continue.

    Args:
        engine: The type mapping engine.
        step: The step name, for the outcome.
        table: The table name.
        source_columns: The source columns to map.

    Returns:
        A tuple of (mapping plan, optional blocking outcome). The outcome is
        ``None`` when the plan is clean.
    """
    plan = engine.plan_table(table, source_columns)
    if plan.ok:
        return plan, None

    # Build a concise, non-sensitive summary of every blocked column.
    detail = "; ".join(f"{issue.name}: {issue.detail}" for issue in plan.issues)
    outcome = StepOutcome(
        step=step,
        table=table,
        status=StepStatus.NEEDS_DECISION,
        detail=(
            f"{len(plan.issues)} column(s) need a data_type_handling decision: "
            + detail
        )[:1000],
    )
    return plan, outcome


def business_columns(columns: Sequence[str], *, audit_prefix: str = "_umf_") -> List[str]:
    """Return the data columns, excluding the framework's audit columns.

    Reconciliation compares the source to the staged target on the real data, so
    it must ignore the audit columns the framework adds (which exist only on the
    target). This selects the business columns by excluding anything whose name
    starts with the audit prefix.

    Args:
        columns: All column names.
        audit_prefix: The prefix used for audit columns.

    Returns:
        The business column names, in their original order.
    """
    prefix = audit_prefix.lower()
    return [c for c in columns if not c.lower().startswith(prefix)]


def build_recon_config(table_config: Dict) -> ReconConfig:
    """Build the reconciliation configuration from a table's config.

    Args:
        table_config: The per-table config, whose ``recon`` and ``curated``
            sections supply the level, tolerances and key columns.

    Returns:
        The :class:`~umf.recon.compare.ReconConfig`.
    """
    recon = table_config.get("recon") or {}
    curated = table_config.get("curated") or {}
    try:
        level = ReconLevel.coerce(recon.get("level", "L2"))
    except ValueError:
        level = ReconLevel.L2
    return ReconConfig(
        level=level,
        decimal_tolerance=Decimal(str(recon.get("decimal_tolerance", 0))),
        float_tolerance=float(recon.get("float_tolerance", 1e-6)),
        key_columns=list(curated.get("key_columns", [])),
    )
