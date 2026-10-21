"""Typed builders that turn framework events into table-shaped records.

A log record is ultimately a flat dictionary written to a :class:`~umf.control.
log_sink.LogSink`. Building those dictionaries by hand at each call site would
be error-prone: a typo in a key, or a key that does not exist on the target
table, would only show up much later when someone queries the table.

This module provides small builder functions, one per important table. Each:

    * accepts typed, named arguments so the caller cannot forget a field,
    * fills in a UTC timestamp consistently, and
    * validates the produced record against the table definition, so a record
      can never contain a column the table does not have.

The validation is cheap and runs in development and tests; it is the safety net
that keeps records and table schemas in lockstep.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from umf.control.table_defs import TableDef, get_table
from umf.core.exceptions import UmfError


def _now() -> str:
    """Return the current time as an ISO 8601 UTC string.

    Using UTC everywhere removes timezone ambiguity from the logs.

    Returns:
        The current UTC time in ISO 8601 format.
    """
    return datetime.now(timezone.utc).isoformat()


def _validate_against_table(table: TableDef, record: Dict[str, Any]) -> None:
    """Check that a record only uses columns the table defines.

    Missing columns are allowed (they are simply null in the table); unknown
    columns are not, because they indicate a mismatch between the record builder
    and the table definition that must be fixed.

    Args:
        table: The target table definition.
        record: The record to check.

    Raises:
        UmfError: If the record contains a key that is not a column of the
            table. This is an internal consistency error, not a user error.
    """
    allowed = set(table.column_names())
    # Keys beginning with underscore are sink bookkeeping (for example the
    # sink's own write timestamp) and are not table columns.
    unknown = {k for k in record if not k.startswith("_")} - allowed
    if unknown:
        raise UmfError(
            f"Record for table '{table.name}' has unknown columns: "
            f"{sorted(unknown)}",
            context={"table": table.name, "unknown_columns": sorted(unknown)},
        )


def build(table: str, **fields: Any) -> Dict[str, Any]:
    """Build and validate a record for a named table.

    This is the general builder. The named helpers below wrap it for the common
    tables so that call sites get argument checking and documentation.

    The parameter is named ``table`` (not ``table_name``) on purpose: several
    tables have a column literally called ``table_name``, and a record for such
    a table passes ``table_name=...`` as a field. Naming the positional
    parameter ``table`` keeps those two distinct and avoids an argument clash.

    Args:
        table: The logical table the record targets (for example ``"run_log"``).
        **fields: Column values. Any column not provided is treated as null.

    Returns:
        The validated record dictionary.

    Raises:
        UmfError: If a provided field is not a column of the table.
    """
    table_def = get_table(table)
    record = {key: value for key, value in fields.items() if value is not None}
    _validate_against_table(table_def, record)
    return record


def run_log_record(
    *,
    run_id: str,
    status: str,
    client_id: Optional[str] = None,
    environment: Optional[str] = None,
    platform: Optional[str] = None,
    framework_version: Optional[str] = None,
    config_version: Optional[int] = None,
    run_mode: Optional[str] = None,
    trigger: Optional[str] = None,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    tables_total: Optional[int] = None,
    outcomes_total: Optional[int] = None,
    error_summary: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a ``run_log`` record.

    Args:
        run_id: Unique run identifier.
        status: Overall run status.
        client_id: Client the run belongs to.
        environment: Environment the run executed in.
        platform: Resolved compute platform.
        framework_version: Framework build identifier.
        config_version: Config version used.
        run_mode: The run mode.
        trigger: What started the run.
        started_at: ISO start time.
        ended_at: ISO end time.
        duration_seconds: Duration in seconds.
        tables_total: Distinct tables touched.
        outcomes_total: Total outcomes recorded.
        error_summary: Short, non-sensitive failure summary.

    Returns:
        A validated ``run_log`` record.
    """
    return build(
        "run_log",
        run_id=run_id,
        status=status,
        client_id=client_id,
        environment=environment,
        platform=platform,
        framework_version=framework_version,
        config_version=config_version,
        run_mode=run_mode,
        trigger=trigger,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        tables_total=tables_total,
        outcomes_total=outcomes_total,
        error_summary=error_summary,
    )


def step_log_record(
    *,
    run_id: str,
    step: str,
    status: str,
    table_name: Optional[str] = None,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    detail: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a ``step_log`` record.

    Args:
        run_id: Run identifier.
        step: Step name.
        status: Step status.
        table_name: Table processed.
        started_at: ISO start time.
        ended_at: ISO end time.
        duration_seconds: Duration in seconds.
        detail: Short, non-sensitive note.

    Returns:
        A validated ``step_log`` record.
    """
    return build(
        "step_log",
        run_id=run_id,
        step=step,
        status=status,
        table_name=table_name,
        started_at=started_at or _now(),
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        detail=detail,
    )


def error_log_record(
    *,
    error: UmfError,
    run_id: Optional[str] = None,
    step: Optional[str] = None,
    table_name: Optional[str] = None,
    file_path: Optional[str] = None,
    attempt_no: Optional[int] = None,
) -> Dict[str, Any]:
    """Build an ``error_log`` record from a framework error.

    Reuses the structured fields the error already carries (category, severity,
    retry hint, context), so the error record is consistent with how the error
    was raised.

    Args:
        error: The :class:`~umf.core.exceptions.UmfError` to record.
        run_id: Run identifier, if known.
        step: Step where the error occurred.
        table_name: Table involved.
        file_path: File involved.
        attempt_no: Retry attempt number.

    Returns:
        A validated ``error_log`` record.
    """
    data = error.to_dict()
    return build(
        "error_log",
        run_id=run_id,
        step=step,
        table_name=table_name,
        file_path=file_path,
        error_type=data["error_type"],
        category=data["category"],
        severity=data["severity"],
        message=data["message"],
        retryable=data["retryable"],
        attempt_no=attempt_no,
        context=data["context"],
        ts=_now(),
    )


def recon_result_record(
    *,
    run_id: str,
    step: str,
    table_name: str,
    check_name: str,
    level: str,
    source_value: Any,
    target_value: Any,
    difference: Any,
    tolerance: Any,
    status: str,
) -> Dict[str, Any]:
    """Build a ``recon_results`` record.

    Values are stored as text so any comparable type (decimal, timestamp, count)
    can be recorded uniformly.

    Args:
        run_id: Run identifier.
        step: Step the check belongs to.
        table_name: Table checked.
        check_name: The check performed.
        level: Recon level (L1 / L2 / L3).
        source_value: Value measured on the source side.
        target_value: Value measured on the target side.
        difference: The difference between source and target.
        tolerance: The tolerance allowed for the check.
        status: ``"pass"`` or ``"fail"``.

    Returns:
        A validated ``recon_results`` record.
    """
    return build(
        "recon_results",
        run_id=run_id,
        step=step,
        table_name=table_name,
        check_name=check_name,
        level=level,
        source_value=str(source_value),
        target_value=str(target_value),
        difference=str(difference),
        tolerance=str(tolerance),
        status=status,
        ts=_now(),
    )


def type_mapping_record(
    *,
    run_id: str,
    table_name: str,
    column_name: str,
    source_type: str,
    target_type: str,
    exact: bool,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    length: Optional[int] = None,
    fallback_used: bool = False,
    data_loss_risk: bool = False,
    decision: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a ``type_mapping_log`` record.

    Args:
        run_id: Run identifier.
        table_name: Table.
        column_name: Column.
        source_type: Native source type.
        target_type: Resolved target type.
        exact: Whether the mapping is exact.
        precision: Numeric precision, if any.
        scale: Numeric scale, if any.
        length: String length, if any.
        fallback_used: Whether a fallback was applied.
        data_loss_risk: Whether the mapping risks losing information.
        decision: The configured handling decision, if any.

    Returns:
        A validated ``type_mapping_log`` record.
    """
    return build(
        "type_mapping_log",
        run_id=run_id,
        table_name=table_name,
        column_name=column_name,
        source_type=source_type,
        target_type=target_type,
        precision=precision,
        scale=scale,
        length=length,
        exact=exact,
        fallback_used=fallback_used,
        data_loss_risk=data_loss_risk,
        decision=decision,
        ts=_now(),
    )
