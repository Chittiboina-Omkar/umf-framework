"""Schema reconciliation: confirm the target schema preserves the source facts.

Data reconciliation (in :mod:`umf.recon.compare`) runs after the data is staged.
Schema reconciliation runs earlier, on the mapping plan alone, before any data
is read, and answers a different question: does the resolved target type keep
the source's precision, scale and length?

In the normal path the type mapping engine has already mapped every column
exactly, so these checks pass and simply record the evidence. The checks earn
their place in the cases where a configured ``data_type_handling`` decision
accepted a reduction (for example capping a large decimal at precision 38, or
truncating a nanosecond timestamp): schema recon turns that accepted reduction
into an explicit, logged result with a clear status, so the trail always shows
that a known, approved reduction happened rather than an accident.

This module is pure Python and fully testable; it reads the mapping plan
produced in Part 3.
"""

from __future__ import annotations

from typing import List

from umf.core.constants import ReconLevel
from umf.typing.canonical import CanonicalKind
from umf.typing.engine import MappingPlan


def schema_recon(plan: MappingPlan) -> List[dict]:
    """Produce schema reconciliation results from a mapping plan.

    For each resolved column it records whether the mapping was exact and, when
    not, flags the precision / scale / length situation so a reviewer sees it.

    Args:
        plan: The mapping plan from the type engine. Expected to have no blocking
            issues (those are handled before load); if issues remain they are
            recorded as failed schema checks too.

    Returns:
        A list of recon result dictionaries at level L1, one per column, plus one
        per unresolved issue.
    """
    results: List[dict] = []

    for entry in plan.entries:
        canonical = entry.canonical
        status = "pass" if entry.exact else "warn"
        # A warn status means the value is preserved but the type changed (a
        # fallback), or a known, approved reduction was applied. A reduction that
        # risks data loss is marked explicitly.
        detail = entry.reason
        if entry.data_loss_risk:
            status = "fail"
            detail = f"data-loss risk: {entry.reason}"

        results.append(
            {
                "check_name": f"schema:{entry.name}",
                "level": str(ReconLevel.L1),
                "source_value": f"{entry.source_type} ({canonical.describe()})",
                "target_value": entry.target_type,
                "difference": "exact" if entry.exact else detail,
                "tolerance": "n/a",
                "status": status,
            }
        )

    for issue in plan.issues:
        results.append(
            {
                "check_name": f"schema:{issue.name}",
                "level": str(ReconLevel.L1),
                "source_value": issue.source_type,
                "target_value": "unresolved",
                "difference": issue.detail,
                "tolerance": "n/a",
                "status": "fail",
            }
        )

    return results
