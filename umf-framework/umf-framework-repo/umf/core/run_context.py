"""The run context: one object that tracks a single run from start to finish.

A :class:`RunContext` is created once at the start of a run and threaded through
every step. It holds the things every step needs to know and record:

    * a unique ``run_id`` so every log line, recon result and output can be tied
      back to this exact run,
    * the resolved, validated config (with secrets masked when snapshotted),
    * the resolved platform and environment,
    * the framework version that produced the run,
    * timing and overall status, computed from the per-table outcomes so that
      one table's failure yields a ``PARTIAL`` run, never a silent total
      failure or a false success.

Crash-safety:
    The context is designed so that marking a run finished is something you do
    in a ``finally`` block. Even if a step raises, the run is still closed out
    with a status and an end time, and that close-out is logged. Persisting the
    status to a database ``run_log`` table is layered on top in a later part via
    a sink; this object is the in-memory source of truth those sinks read.

Standard library only.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from umf.core.constants import Environment, Platform, RunStatus, StepStatus
from umf.core.logging_setup import get_logger
from umf.core.masking import mask_structure

_log = get_logger("core.run_context")


@dataclass
class StepOutcome:
    """The result of running one step for one table.

    Attributes:
        step: The step name (a :class:`~umf.core.constants.Step` value).
        table: The logical table this outcome refers to.
        status: The :class:`~umf.core.constants.StepStatus`.
        rows_read: Rows read from the source, if known.
        rows_written: Rows written to the target, if known.
        rows_rejected: Rows sent to the reject table, if known.
        detail: A short, non-sensitive note (for example why it was skipped).
        started_at: When this step started, in UTC.
        ended_at: When this step finished, in UTC.
    """

    step: str
    table: str
    status: StepStatus
    rows_read: Optional[int] = None
    rows_written: Optional[int] = None
    rows_rejected: Optional[int] = None
    detail: str = ""
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable view of the outcome, for logging.

        Returns:
            A dictionary of the outcome's fields with datetimes as ISO strings.
        """
        return {
            "step": str(self.step),
            "table": self.table,
            "status": str(self.status),
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_rejected": self.rows_rejected,
            "detail": self.detail,
            "started_at": _iso(self.started_at),
            "ended_at": _iso(self.ended_at),
        }


@dataclass
class RunContext:
    """State and identity for a single framework run.

    Create one with :meth:`start`. Record each step with :meth:`record_outcome`.
    Close the run with :meth:`finish`, ideally from a ``finally`` block so the
    run is always closed even on error.

    Attributes:
        run_id: A unique identifier for this run.
        config: The resolved, validated config for this run.
        platform: The resolved platform.
        environment: The resolved environment.
        framework_version: The framework build that produced this run.
        status: The current overall status.
        started_at: When the run started, in UTC.
        ended_at: When the run finished, in UTC, or ``None`` while running.
        outcomes: The per-step, per-table outcomes recorded so far.
    """

    run_id: str
    config: Dict[str, Any]
    platform: Platform
    environment: Environment
    framework_version: str
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: Optional[datetime] = None
    outcomes: List[StepOutcome] = field(default_factory=list)

    @classmethod
    def start(
        cls,
        *,
        config: Dict[str, Any],
        platform: Platform,
        environment: Environment,
        framework_version: str,
        run_id: Optional[str] = None,
    ) -> "RunContext":
        """Create and log the start of a new run.

        Args:
            config: The resolved, validated config.
            platform: The resolved platform.
            environment: The resolved environment.
            framework_version: The framework build identifier.
            run_id: An explicit run identifier. If omitted, a new UUID is
                generated. Supplying your own is useful when an external
                scheduler already has an id it wants to correlate on.

        Returns:
            The new :class:`RunContext`, already marked ``RUNNING`` and logged.
        """
        ctx = cls(
            run_id=run_id or uuid.uuid4().hex,
            config=config,
            platform=platform,
            environment=environment,
            framework_version=framework_version,
        )
        # Log the start with a masked snapshot of the config, so the exact
        # (secret-free) config that produced this run is recorded up front.
        _log.info(
            "run started",
            extra={
                "run_id": ctx.run_id,
                "platform": str(platform),
                "environment": str(environment),
                "framework_version": framework_version,
                "config_snapshot": mask_structure(config),
            },
        )
        return ctx

    def record_outcome(self, outcome: StepOutcome) -> None:
        """Record the outcome of one step for one table and log it.

        Args:
            outcome: The :class:`StepOutcome` to record.
        """
        self.outcomes.append(outcome)
        _log.info("step outcome", extra={"run_id": self.run_id, **outcome.to_dict()})

    def compute_status(self) -> RunStatus:
        """Derive the overall run status from the recorded outcomes.

        The rule enforces the design decision that one table's failure must not
        masquerade as success, and a mix of success and failure is reported
        honestly as ``PARTIAL``:

            * no outcomes at all              -> SUCCESS (nothing to do)
            * every outcome failed            -> FAILED
            * some failed, some succeeded     -> PARTIAL
            * a step is blocked on a decision -> PARTIAL (needs human input)
            * otherwise                       -> SUCCESS

        Returns:
            The computed :class:`RunStatus`. This does not change
            :attr:`status`; :meth:`finish` does that.
        """
        if not self.outcomes:
            return RunStatus.SUCCESS

        failed = sum(1 for o in self.outcomes if o.status == StepStatus.FAILED)
        needs_decision = sum(
            1 for o in self.outcomes if o.status == StepStatus.NEEDS_DECISION
        )
        succeeded = sum(
            1
            for o in self.outcomes
            if o.status in (StepStatus.SUCCESS, StepStatus.SKIPPED)
        )

        if failed and succeeded == 0 and needs_decision == 0:
            return RunStatus.FAILED
        if failed or needs_decision:
            return RunStatus.PARTIAL
        return RunStatus.SUCCESS

    def finish(self, status: Optional[RunStatus] = None) -> RunStatus:
        """Close out the run, set its final status and end time, and log it.

        Call this from a ``finally`` block so the run is always closed, even if a
        step raised. Calling it more than once is harmless; the first call wins
        for the end time.

        Args:
            status: An explicit final status to force (for example
                :attr:`RunStatus.ABORTED` when the run never really started
                because the config was invalid). If omitted, the status is
                derived from the recorded outcomes via :meth:`compute_status`.

        Returns:
            The final :class:`RunStatus`.
        """
        if self.ended_at is None:
            self.ended_at = datetime.now(timezone.utc)
        self.status = status or self.compute_status()

        duration = (self.ended_at - self.started_at).total_seconds()
        _log.info(
            "run finished",
            extra={
                "run_id": self.run_id,
                "status": str(self.status),
                "duration_seconds": round(duration, 3),
                "tables_total": len({o.table for o in self.outcomes}),
                "outcomes_total": len(self.outcomes),
            },
        )
        return self.status

    def summary(self) -> Dict[str, Any]:
        """Return a compact, JSON-serialisable summary of the run.

        Useful for a final report or for persisting to the ``run_log`` table.

        Returns:
            A dictionary with identity, status, timing and per-table counts.
        """
        return {
            "run_id": self.run_id,
            "framework_version": self.framework_version,
            "platform": str(self.platform),
            "environment": str(self.environment),
            "status": str(self.status),
            "started_at": _iso(self.started_at),
            "ended_at": _iso(self.ended_at),
            "outcomes": [o.to_dict() for o in self.outcomes],
        }


def _iso(value: Optional[datetime]) -> Optional[str]:
    """Format a datetime as an ISO 8601 string, or return ``None``.

    Args:
        value: The datetime to format, or ``None``.

    Returns:
        The ISO 8601 string, or ``None`` if the input was ``None``.
    """
    return None if value is None else value.isoformat()
