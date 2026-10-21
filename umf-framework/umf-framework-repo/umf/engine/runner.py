"""The runner: the single entry point that executes a run.

Everything built so far comes together here. Given a config, the runner:

    1. validates the config and, for a real (non-dry) run, refuses to proceed if
       any safe default is still unapproved;
    2. resolves the platform;
    3. opens a :class:`~umf.core.run_context.RunContext` and records the start;
    4. derives the steps and builds the work DAG across all tables;
    5. executes the nodes in order, isolating failures so one table's problem
       never stops another; and
    6. finishes the run, records the final status, and returns a summary.

The isolation rule is the heart of step 5: before running a node, the runner
checks whether any node it depends on failed or was skipped; if so, the node is
skipped too. This is how a failure in one table's raw step stops only that
table's later steps, while every other table runs to completion and the run
ends as ``PARTIAL`` rather than a misleading success or total failure.

The runner is driven through the executor interface, so it is tested end to end
here with a dry-run / fake executor, no Spark required.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from umf.core.constants import Environment, Platform, RunStatus, StepStatus
from umf.core.exceptions import UmfError, ValidationError
from umf.core.logging_setup import get_logger, init_logging
from umf.core.run_context import RunContext, StepOutcome
from umf.control.control_plane import ControlPlane
from umf.core.config_loader import validate_config
from umf.core.platform_detect import resolve_platform
from umf.engine.dag import Dag, Node, build_dag
from umf.engine.decision import resolve_steps
from umf.engine.executor import ExecutorRegistry, dry_run_registry

_log = get_logger("engine.runner")


def _tables_of(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the list of per-table configs for a run.

    A run may process many tables (each with its own settings) via a ``tables``
    list, or a single table described by the top-level ``target``. This
    normalises both into a list of per-table configs, each carrying at least a
    ``name``.

    Args:
        config: The resolved config.

    Returns:
        A list of per-table config dictionaries.
    """
    tables = config.get("tables")
    if isinstance(tables, list) and tables:
        result = []
        for entry in tables:
            # Each table entry inherits the top-level config and overrides it.
            merged = {**config, **entry}
            merged.setdefault("name", (entry.get("target") or {}).get("table")
                              or entry.get("name"))
            result.append(merged)
        return result

    # Single table from the top-level target.
    target = config.get("target") or {}
    name = target.get("table") or "table"
    single = {**config, "name": name}
    return [single]


def run(
    config: Dict[str, Any],
    *,
    framework_version: str,
    executor_registry: Optional[ExecutorRegistry] = None,
    control_plane: Optional[ControlPlane] = None,
    spark: Any = None,
    trigger: str = "manual",
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a run and return its summary.

    Args:
        config: The resolved config for the run.
        framework_version: The framework build identifier, recorded on the run.
        executor_registry: The registry mapping steps to executors. If ``None``,
            a dry-run registry is used, which validates the orchestration without
            processing data (equivalent to a validate / dry-run mode).
        control_plane: The control plane for logging and state. If ``None``, a
            no-op control plane is used (nothing is persisted, but the run still
            works and logs to the console).
        spark: An optional Spark session passed to platform resolution.
        trigger: What started the run (for example ``"schedule"``).
        run_id: An explicit run id, or ``None`` to generate one.

    Returns:
        The run summary dictionary from
        :meth:`~umf.core.run_context.RunContext.summary`.

    Raises:
        ValidationError: If the config is invalid. For a real (non-dry) run,
            unapproved safe defaults also make the config invalid, so a scheduled
            run never proceeds on assumptions the user has not seen.
    """
    init_logging()
    registry = executor_registry or dry_run_registry()
    control = control_plane or ControlPlane()
    is_dry_run = executor_registry is None

    # --- validate the config --------------------------------------------- #
    result = validate_config(config)
    if not is_dry_run and result.confirmations:
        # A real run must not rely on un-approved defaults.
        for conf in result.confirmations:
            result.errors.append("Unapproved default: " + conf.message())
    result.raise_if_invalid()
    resolved = result.resolved

    # --- resolve the platform -------------------------------------------- #
    configured_platform = Platform.coerce(resolved.get("platform", "AUTO"))
    platform = resolve_platform(configured_platform, spark)
    environment = Environment.coerce(resolved.get("environment", "DEV"))

    # --- open the run ---------------------------------------------------- #
    ctx = RunContext.start(
        config=resolved,
        platform=platform,
        environment=environment,
        framework_version=framework_version,
        run_id=run_id,
    )
    control.emit_run_start(ctx, trigger=trigger)

    error_summary = ""
    try:
        steps = resolve_steps(resolved)
        tables = _tables_of(resolved)
        table_names = [t["name"] for t in tables]
        table_by_name = {t["name"]: t for t in tables}

        cross = resolved.get("cross_table_deps") or {}
        dag = build_dag(steps=steps, tables=table_names, cross_table_deps=cross)

        _execute_dag(ctx, dag, registry, table_by_name, control)

    except UmfError as exc:
        # A failure in setting up the run (not in an individual node): record it
        # and let the run finish as failed. Node-level failures are handled
        # inside _execute_dag and do not reach here.
        control.emit_error(exc, run_id=ctx.run_id)
        error_summary = exc.message
        ctx.finish(RunStatus.FAILED)
        control.emit_run_finish(ctx, error_summary=error_summary)
        raise
    finally:
        # Ensure the run is always closed out, even on an unexpected error, so a
        # crashed run is never left silently open.
        if ctx.ended_at is None:
            ctx.finish()
            control.emit_run_finish(ctx, error_summary=error_summary)

    return ctx.summary()


def _execute_dag(
    ctx: RunContext,
    dag: Dag,
    registry: ExecutorRegistry,
    table_by_name: Dict[str, Dict[str, Any]],
    control: ControlPlane,
) -> None:
    """Execute the DAG nodes in order, applying the isolation rule.

    Args:
        ctx: The run context.
        dag: The work DAG.
        registry: The executor registry.
        table_by_name: The per-table configs, keyed by table name.
        control: The control plane, for per-step logging.
    """
    status_by_node: Dict[str, StepStatus] = {}

    for node in dag.topo_order():
        # Isolation: if any dependency failed or was skipped, skip this node.
        blocked = [
            dep
            for dep in node.depends_on
            if status_by_node.get(dep) in (StepStatus.FAILED, StepStatus.SKIPPED)
        ]
        if blocked:
            outcome = StepOutcome(
                step=str(node.step),
                table=node.table,
                status=StepStatus.SKIPPED,
                detail=f"skipped because an upstream step did not succeed: {blocked[0]}",
            )
            _record(ctx, control, outcome)
            status_by_node[node.id] = StepStatus.SKIPPED
            continue

        table_config = table_by_name.get(node.table, {})
        try:
            executor = registry.get(node.step)
            outcome = executor.execute(ctx, node, table_config)
        except UmfError as exc:
            # A node-level failure is isolated: record it and continue with other
            # independent nodes. The run's status becomes PARTIAL or FAILED.
            control.emit_error(exc, run_id=ctx.run_id, step=str(node.step), table=node.table)
            outcome = StepOutcome(
                step=str(node.step),
                table=node.table,
                status=StepStatus.FAILED,
                detail=exc.message,
            )

        _record(ctx, control, outcome)
        status_by_node[node.id] = outcome.status


def _record(ctx: RunContext, control: ControlPlane, outcome: StepOutcome) -> None:
    """Record a step outcome to the run context and the control plane.

    Args:
        ctx: The run context.
        control: The control plane.
        outcome: The outcome to record.
    """
    ctx.record_outcome(outcome)
    # Persist a step_log row. Done through the control plane's sink; failures to
    # persist are isolated inside the sink and never break the run.
    from umf.control import records  # local import to avoid a load-time cycle

    control.sink.write(
        "step_log",
        records.step_log_record(
            run_id=ctx.run_id,
            step=outcome.step,
            status=str(outcome.status),
            table_name=outcome.table,
            detail=outcome.detail,
        ),
    )
