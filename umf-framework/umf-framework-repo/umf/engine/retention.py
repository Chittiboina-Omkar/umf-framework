"""Planning raw-layer retention: keep the last N successful runs.

When a table's raw layer is configured to keep only the last N runs (rather than
appending full history), something has to decide which runs stay and which are
removed. This module is that decision, kept as pure logic so it can be tested
exhaustively without touching any data.

The rules encode the decisions you made:

    * Only **successful** runs count toward the N. Failed runs are never counted
      and are always eligible for cleanup, so a string of failures cannot push a
      good run out of the retained window.
    * Older runs beyond the window are **archived**, not deleted, when archiving
      is enabled (the safe default); deletion is only chosen when archiving is
      explicitly turned off.
    * The newest N successful runs are always kept.

The planner takes a description of the runs present in raw and returns a
:class:`RetentionPlan` listing what to keep, archive and delete. The actual
archiving or deletion of data is carried out by the caller against the table
writer; the planner only decides, which is what makes it testable and keeps the
policy in one clear place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Sequence

from umf.core.constants import RunStatus


@dataclass(frozen=True)
class RunInfo:
    """A run present in the raw layer, as seen by the retention planner.

    Attributes:
        run_id: The run identifier.
        status: The run's recorded status. Only ``SUCCESS`` runs count toward
            the retained window.
        ingested_at: An ISO 8601 timestamp used to order runs newest-first.
    """

    run_id: str
    status: RunStatus
    ingested_at: str


@dataclass
class RetentionPlan:
    """The decision about which runs to keep, archive or delete.

    Attributes:
        keep: Run ids to retain.
        archive: Run ids to move to archive storage (when archiving is enabled).
        delete: Run ids to delete outright (when archiving is disabled).
    """

    keep: List[str] = field(default_factory=list)
    archive: List[str] = field(default_factory=list)
    delete: List[str] = field(default_factory=list)


def plan_retention(
    runs: Sequence[RunInfo],
    *,
    retention_runs: int,
    archive_enabled: bool = True,
) -> RetentionPlan:
    """Decide retention for a table's raw runs.

    Args:
        runs: All runs currently present in the raw layer for the table.
        retention_runs: How many successful runs to keep. Must be at least 1;
            a non-positive value is treated as 1 to avoid accidentally keeping
            nothing.
        archive_enabled: When true (the default), runs beyond the window are
            archived; when false, they are deleted.

    Returns:
        A :class:`RetentionPlan`. Failed runs are always placed in archive or
        delete (never kept), and the newest ``retention_runs`` successful runs
        are kept.
    """
    window = max(1, retention_runs)

    # Order newest first. ISO timestamps sort correctly as text; the run id is a
    # stable tie-breaker so the ordering is deterministic.
    ordered = sorted(runs, key=lambda r: (r.ingested_at, r.run_id), reverse=True)

    plan = RetentionPlan()
    kept_successful = 0

    for run in ordered:
        is_success = run.status == RunStatus.SUCCESS
        if is_success and kept_successful < window:
            plan.keep.append(run.run_id)
            kept_successful += 1
        else:
            # Either a failed run (never counts, always cleaned up) or a
            # successful run beyond the window.
            if archive_enabled:
                plan.archive.append(run.run_id)
            else:
                plan.delete.append(run.run_id)

    return plan
