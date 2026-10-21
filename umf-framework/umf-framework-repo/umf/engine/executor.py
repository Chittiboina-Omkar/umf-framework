"""Executing a single step for a single table.

The runner walks the DAG and asks an executor to carry out each node. What a
node actually does (read files into raw, build curated, and so on) depends on
the step, and most of those actions need Spark. By routing every node through a
:class:`StepExecutor` interface and an :class:`ExecutorRegistry`, the runner
stays free of step-specific and Spark-specific detail, and the whole
orchestration can be exercised without Spark by registering a dry-run executor.

This is what lets the runner be tested end to end here: a fake or dry-run
executor stands in for the real, Spark-backed steps, and the orchestration
(ordering, isolation, logging, run status) is verified in plain Python.

The real executors that wire together the source reader, type engine,
transforms, keys, data quality, reconciliation and the atomic commit are
registered on top of this interface; because each needs Spark, they are
validated in your environment.
"""

from __future__ import annotations

from typing import Any, Callable, Dict

from umf.core.constants import Step, StepStatus
from umf.core.exceptions import UmfError
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext, StepOutcome
from umf.engine.dag import Node

_log = get_logger("engine.executor")


class StepExecutor:
    """Interface for executing one step for one table."""

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Carry out the node's work and return its outcome.

        Implementations should let a :class:`~umf.core.exceptions.UmfError`
        propagate; the runner catches it, records a failed outcome and isolates
        the failure. Returning a :class:`StepOutcome` with a non-success status
        is also allowed (for example ``SKIPPED`` when there is nothing to do).

        Args:
            ctx: The run context.
            node: The DAG node (step and table).
            table_config: The per-table slice of the config.

        Returns:
            The :class:`StepOutcome`.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class DryRunExecutor(StepExecutor):
    """An executor that does no data work, for validate / dry-run modes.

    It confirms the orchestration (which steps, in what order, for which tables)
    without reading or writing any data, and reports each node as succeeded with
    a note that it was a dry run. This is how a run can be fully validated before
    it is allowed to touch data.
    """

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Return a successful, no-op outcome.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config (unused).

        Returns:
            A succeeded :class:`StepOutcome` marked as a dry run.
        """
        return StepOutcome(
            step=str(node.step),
            table=node.table,
            status=StepStatus.SUCCESS,
            detail="dry-run: validated, no data processed",
        )


class ExecutorRegistry:
    """Maps each step to the executor that runs it.

    A single registry is handed to the runner. For a dry run, every step maps to
    the :class:`DryRunExecutor`. For a real run, each step maps to its concrete,
    Spark-backed executor.
    """

    def __init__(self) -> None:
        """Create an empty registry."""
        self._executors: Dict[Step, StepExecutor] = {}

    def register(self, step: Step, executor: StepExecutor) -> None:
        """Register the executor for a step.

        Args:
            step: The step.
            executor: The executor to run it.
        """
        self._executors[step] = executor

    def register_all(self, executor: StepExecutor, steps=None) -> None:
        """Register one executor for many steps at once.

        Useful for a dry-run executor that handles every step identically.

        Args:
            executor: The executor to register.
            steps: The steps to register it for. Defaults to every known step.
        """
        for step in (steps if steps is not None else list(Step)):
            self._executors[step] = executor

    def get(self, step: Step) -> StepExecutor:
        """Return the executor for a step.

        Args:
            step: The step.

        Returns:
            The registered :class:`StepExecutor`.

        Raises:
            UmfError: If no executor is registered for the step. Raised as a
                framework error so the runner records it and isolates the node,
                rather than crashing the whole run.
        """
        executor = self._executors.get(step)
        if executor is None:
            raise UmfError(
                f"No executor is registered for step '{step}'.",
                context={"step": str(step)},
            )
        return executor


def dry_run_registry() -> ExecutorRegistry:
    """Build a registry whose every step is a dry run.

    Returns:
        An :class:`ExecutorRegistry` mapping all steps to a
        :class:`DryRunExecutor`.
    """
    registry = ExecutorRegistry()
    registry.register_all(DryRunExecutor())
    return registry
