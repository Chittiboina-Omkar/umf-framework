"""The control plane: one facade over logging, watermarks and config storage.

A run needs three operational services working together:

    * a :class:`~umf.control.log_sink.LogSink` to persist structured records,
    * a :class:`~umf.control.watermark_store.WatermarkStore` to read and advance
      watermarks, and
    * a :class:`~umf.control.config_store.ConfigStore` for versioning and audit.

Rather than pass three objects everywhere, the engine (added in a later part)
is given one :class:`ControlPlane` that holds all three and offers a few
convenience methods for the most common emissions. This keeps call sites short
and consistent, and it is the single place to swap the portable file-based
defaults for the table-backed implementations later, without touching callers.

The convenience emitters are deliberately thin wrappers: they build a validated
record (via :mod:`umf.control.records`) and hand it to the sink. Because the
sink isolates its own failures, emitting a record can never break the pipeline.
"""

from __future__ import annotations

from typing import Optional

from umf.control.config_store import ConfigStore
from umf.control.log_sink import LogSink, NullSink
from umf.control import records
from umf.control.watermark_store import WatermarkStore, WatermarkChange
from umf.core.exceptions import UmfError
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext

_log = get_logger("control.control_plane")


class ControlPlane:
    """Holds the operational services and offers convenience emitters.

    Attributes:
        sink: Where structured records are persisted.
        watermarks: The watermark store, or ``None`` if the run uses no
            watermarks (for example a full load with no incremental state).
        configs: The config store, or ``None`` if config versioning is handled
            elsewhere for this run.
    """

    def __init__(
        self,
        *,
        sink: Optional[LogSink] = None,
        watermarks: Optional[WatermarkStore] = None,
        configs: Optional[ConfigStore] = None,
    ) -> None:
        """Create a control plane.

        Args:
            sink: The log sink. Defaults to a :class:`NullSink` so that code
                depending on a control plane works even before a real sink is
                configured (nothing is persisted, but nothing breaks).
            watermarks: The watermark store, if the run needs one.
            configs: The config store, if the run manages config versions.
        """
        self.sink: LogSink = sink or NullSink()
        self.watermarks = watermarks
        self.configs = configs

    # --- run and step logging --------------------------------------------- #

    def emit_run_start(self, ctx: RunContext, *, trigger: str = "manual") -> None:
        """Persist a ``run_log`` row marking the run as started.

        Args:
            ctx: The run context just created by
                :meth:`~umf.core.run_context.RunContext.start`.
            trigger: What started the run (for example ``"schedule"`` or
                ``"manual"``).
        """
        record = records.run_log_record(
            run_id=ctx.run_id,
            status=str(ctx.status),
            client_id=str(ctx.config.get("client_id", "")) or None,
            environment=str(ctx.environment),
            platform=str(ctx.platform),
            framework_version=ctx.framework_version,
            run_mode=str((ctx.config.get("operation") or {}).get("run_mode", ""))
            or None,
            trigger=trigger,
            started_at=ctx.started_at.isoformat(),
        )
        self.sink.write("run_log", record)

    def emit_run_finish(self, ctx: RunContext, *, error_summary: str = "") -> None:
        """Persist a ``run_log`` row marking the run as finished.

        A separate finish row (rather than updating the start row) keeps the log
        append-only: both the start and the final state are visible, which is
        what lets a crashed run be recognised as left in ``RUNNING``.

        Args:
            ctx: The run context after
                :meth:`~umf.core.run_context.RunContext.finish`.
            error_summary: A short, non-sensitive failure summary, if any.
        """
        duration = None
        if ctx.ended_at is not None:
            duration = round((ctx.ended_at - ctx.started_at).total_seconds(), 3)
        record = records.run_log_record(
            run_id=ctx.run_id,
            status=str(ctx.status),
            framework_version=ctx.framework_version,
            started_at=ctx.started_at.isoformat(),
            ended_at=ctx.ended_at.isoformat() if ctx.ended_at else None,
            duration_seconds=duration,
            tables_total=len({o.table for o in ctx.outcomes}),
            outcomes_total=len(ctx.outcomes),
            error_summary=error_summary or None,
        )
        self.sink.write("run_log", record)

    def emit_error(
        self,
        error: UmfError,
        *,
        run_id: Optional[str] = None,
        step: Optional[str] = None,
        table: Optional[str] = None,
        file_path: Optional[str] = None,
        attempt_no: Optional[int] = None,
    ) -> None:
        """Persist an ``error_log`` row from a framework error.

        Args:
            error: The error to record.
            run_id: Run identifier, if known.
            step: Step where the error occurred.
            table: Table involved.
            file_path: File involved.
            attempt_no: Retry attempt number.
        """
        record = records.error_log_record(
            error=error,
            run_id=run_id,
            step=step,
            table_name=table,
            file_path=file_path,
            attempt_no=attempt_no,
        )
        self.sink.write("error_log", record)

    def emit_recon(self, **kwargs: object) -> None:
        """Persist a ``recon_results`` row.

        Args:
            **kwargs: Passed straight to
                :func:`umf.control.records.recon_result_record`.
        """
        self.sink.write("recon_results", records.recon_result_record(**kwargs))  # type: ignore[arg-type]

    # --- watermark handling ----------------------------------------------- #

    def advance_watermark(
        self,
        table_key: str,
        new_value: str,
        *,
        run_id: str,
        watermark_column: Optional[str] = None,
        reason: str = "step succeeded and reconciliation passed",
    ) -> Optional[WatermarkChange]:
        """Advance a watermark and record the change to the watermark log.

        This must only be called after the step has succeeded and reconciliation
        has passed; the control plane does not itself check that precondition, it
        trusts the caller (the step) to honour it, which keeps the rule in one
        obvious place in the step code.

        Args:
            table_key: The table's stable key.
            new_value: The new watermark value.
            run_id: The run advancing the watermark.
            watermark_column: The tracked column, recorded for audit.
            reason: A short, non-sensitive reason.

        Returns:
            The :class:`WatermarkChange`, or ``None`` if no watermark store is
            configured for this run.
        """
        if self.watermarks is None:
            return None
        change = self.watermarks.advance(
            table_key,
            new_value,
            run_id=run_id,
            watermark_column=watermark_column,
            reason=reason,
        )
        # Record the move in the watermark log for a full audit.
        self.sink.write(
            "watermark_log",
            {
                "run_id": run_id,
                "table_key": change.table_key,
                "old_value": change.old_value,
                "new_value": change.new_value,
                "reason": change.reason,
                "changed_at": change.changed_at,
                "changed_by": run_id,
            },
        )
        return change
