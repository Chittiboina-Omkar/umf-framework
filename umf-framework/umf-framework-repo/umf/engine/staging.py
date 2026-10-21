"""Stage, reconcile, then commit atomically, or discard with the target intact.

This module holds the single most important guarantee in the framework, as code:

    Data is written to a staging table first. Reconciliation runs against the
    staged data. Only if it passes is the staged data published into the target,
    atomically. If it fails, the staged data is discarded and the target is
    never touched.

Together with the atomic publish provided by the table writers, this gives the
two guarantees you insisted on:

    * no bad data reaches the target (the recon gate stands between stage and
      publish), and
    * no partial data ever appears (the publish is a single transaction that
      either fully applies or does not apply at all).

The orchestrator is written entirely against the :class:`~umf.adapters.
table_format.base.TableWriter` interface and the :class:`~umf.engine.recon_gate.
ReconGate` interface, so it is independent of Delta, Iceberg and the specific
reconciliation checks, and it can be tested in pure Python with a fake writer
and a gate that is told to pass or fail.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

from umf.adapters.table_format.base import CommitMode, CommitResult, TableWriter
from umf.core.exceptions import ReconciliationError
from umf.core.logging_setup import get_logger
from umf.engine.identifiers import TableIdentifier
from umf.engine.recon_gate import ReconGate

_log = get_logger("engine.staging")


@dataclass
class StageAndCommitResult:
    """The outcome of a full stage-reconcile-commit cycle.

    Attributes:
        committed: Whether the data was published to the target.
        commit: The :class:`CommitResult` when committed, else ``None``.
        recon_results: The reconciliation check results, for logging, whether or
            not the commit happened.
        staged_count: The number of rows that were staged.
    """

    committed: bool
    commit: Optional[CommitResult]
    recon_results: List[dict]
    staged_count: int


def stage_reconcile_commit(
    *,
    writer: TableWriter,
    df: Any,
    target: TableIdentifier,
    run_id: str,
    mode: CommitMode,
    gate_factory,
    target_columns: Optional[Sequence[Tuple[str, str]]] = None,
    partition_by: Sequence[str] = (),
    keys: Sequence[str] = (),
    ordering: Optional[str] = None,
    properties: Optional[dict] = None,
) -> StageAndCommitResult:
    """Run the stage, reconcile, and atomic commit (or discard) cycle.

    The sequence is deliberately strict and always the same:

        1. Ensure the target table exists (create it from ``target_columns`` if
           it does not and a column list was supplied).
        2. Write the incoming DataFrame to a per-run staging table.
        3. Build and run the reconciliation gate against the staged data.
        4. If the gate fails, drop the staging table and raise, leaving the
           target completely untouched.
        5. If the gate passes, publish the staged data into the target
           atomically, then drop the staging table.

    Args:
        writer: The table writer (Delta or Iceberg).
        df: The incoming Spark DataFrame to stage and commit.
        target: The target table identifier.
        run_id: The current run identifier, used to name the staging table.
        mode: The commit mode (overwrite, append or merge).
        gate_factory: A callable ``(staged_count: int) -> ReconGate`` that builds
            the reconciliation gate once the staged row count is known. Taking a
            factory lets the gate compare the staged count (and, in the full
            engine, aggregates) against the source. Part 5 passes a factory that
            builds the complete gate; a count gate suffices here.
        target_columns: ``(name, type_string)`` pairs used to create the target
            table if it does not exist. If ``None``, the target is assumed to
            already exist.
        partition_by: Columns to partition a newly created target by.
        keys: Merge keys (required when ``mode`` is ``MERGE``).
        ordering: Ordering column that selects the newest row per key for a
            merge.
        properties: Optional table properties for a newly created target.

    Returns:
        A :class:`StageAndCommitResult` describing what happened.

    Raises:
        ReconciliationError: If the gate fails. The staging table is dropped
            first and the target is left unchanged, so a failed reconciliation
            never leaves bad or partial data behind.
    """
    staging = target.staging(run_id)

    # Step 1: ensure the target exists (create from the resolved schema if asked).
    if target_columns is not None and not writer.table_exists(target):
        _log.info("creating target table", extra={"table": target.qualified()})
        writer.create_table(
            target, target_columns, partition_by=partition_by, properties=properties
        )

    # Step 2: stage the incoming data. The target is still untouched.
    writer.write_staging(df, staging)
    staged_count = writer.row_count(staging)
    _log.info(
        "data staged",
        extra={"staging": staging.qualified(), "rows": staged_count},
    )

    # Step 3: reconcile the staged data.
    gate = gate_factory(staged_count)
    outcome = gate.check()

    # Step 4: on failure, discard staging and leave the target untouched.
    if not outcome.passed:
        _log.warning(
            "reconciliation failed; discarding staged data, target untouched",
            extra={"target": target.qualified(), "staging": staging.qualified()},
        )
        _safe_drop(writer, staging)
        raise ReconciliationError(
            "Reconciliation did not pass; the staged data was discarded and the "
            "target was not modified.",
            context={
                "target": target.qualified(),
                "staged_rows": staged_count,
                "checks": outcome.results,
            },
        )

    # Step 5: reconciliation passed. Publish atomically, then clean up staging.
    _log.info(
        "reconciliation passed; publishing atomically",
        extra={"target": target.qualified(), "mode": str(mode)},
    )
    commit = writer.publish(staging, target, mode, keys=keys, ordering=ordering)
    _safe_drop(writer, staging)

    return StageAndCommitResult(
        committed=True,
        commit=commit,
        recon_results=outcome.results,
        staged_count=staged_count,
    )


def _safe_drop(writer: TableWriter, staging: TableIdentifier) -> None:
    """Drop a staging table, never raising if the drop itself fails.

    Cleaning up staging is best effort: a leftover staging table is harmless
    (it is named per run and never read by anything else), so a failure to drop
    it must not turn into a failure of the whole step, nor mask the real outcome.

    Args:
        writer: The table writer.
        staging: The staging table to drop.
    """
    try:
        writer.drop(staging)
    except Exception as exc:  # best effort; a leftover staging table is harmless
        _log.warning(
            "could not drop staging table (harmless leftover)",
            extra={"staging": staging.qualified(), "error": repr(exc)},
        )
