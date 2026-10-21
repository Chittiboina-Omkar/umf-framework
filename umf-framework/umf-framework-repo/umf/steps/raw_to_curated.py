"""The Raw-to-Curated step executor.

This executor builds the curated layer from raw, applying the curated mode and
all the per-table rules, in order:

    1. read the raw table;
    2. apply the configured transforms (rename, cast, derived, mask, ...);
    3. build the curated plan from the mode and keys (Part 6), which also
       selects the commit mode (overwrite / append / merge);
    4. add the key hash when the key strategy calls for one;
    5. run the data-quality rules, routing quarantined rows out and failing the
       table if a fail-action rule is breached;
    6. profile the business columns for reconciliation;
    7. stage, reconcile, and commit atomically in the plan's commit mode, or
       discard if reconciliation fails.

As with the files-to-raw executor, the plan building and commit-mode selection
are pure and tested (Part 6); the data operations use Spark through the
collaborators and are validated in your environment.
"""

from __future__ import annotations

from typing import Any, Optional

from umf.adapters.table_format.base import TableWriter
from umf.core.constants import CuratedMode, KeyStrategy, StepStatus
from umf.core.exceptions import ConfigError
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext, StepOutcome
from umf.dq.registry import SparkDQEvaluator, build_rules
from umf.dq.rules import enforce
from umf.engine.curated import build_curated_plan, merge_keys
from umf.engine.dag import Node
from umf.engine.executor import StepExecutor
from umf.engine.identifiers import TableIdentifier
from umf.engine.keys import add_key_hash
from umf.engine.staging import stage_reconcile_commit
from umf.engine.transforms import apply_transforms
from umf.recon.gate import FullReconGate
from umf.recon.measurer import ReconMeasurer
from umf.steps.common import build_recon_config

_log = get_logger("steps.raw_to_curated")


class RawToCuratedExecutor(StepExecutor):
    """Executes the raw-to-curated step for one table.

    Attributes:
        writer: The table writer.
        measurer: The reconciliation measurer.
        dq_evaluator: The data-quality evaluator.
        spark: The Spark session (obtained lazily when needed).
    """

    def __init__(
        self,
        *,
        writer: TableWriter,
        measurer: ReconMeasurer,
        dq_evaluator: Optional[SparkDQEvaluator] = None,
        spark: Any = None,
    ) -> None:
        """Create the executor with its collaborators.

        Args:
            writer: The table writer.
            measurer: The reconciliation measurer.
            dq_evaluator: The data-quality evaluator; a default is used if
                ``None``.
            spark: The Spark session (optional).
        """
        self.writer = writer
        self.measurer = measurer
        self.dq_evaluator = dq_evaluator or SparkDQEvaluator()
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Run the raw-to-curated step for the node's table.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config.

        Returns:
            The step outcome.

        Raises:
            DataQualityError: If a fail-action data-quality rule is breached (the
                runner catches this and isolates the table).
            ConfigError: If the curated settings are inconsistent.
        """
        step = str(node.step)
        curated_cfg = table_config.get("curated") or {}
        raw_ident = _raw_identifier(table_config)
        curated_ident = _curated_identifier(table_config)

        # --- 1: read raw -------------------------------------------------- #
        raw_df = self._read_table(raw_ident)

        # --- 2: apply transforms ----------------------------------------- #
        transformed = apply_transforms(raw_df, _transform_plan(table_config))

        # --- 3: build the curated plan (mode -> commit mode, keys) -------- #
        mode = CuratedMode.coerce(curated_cfg.get("mode", "LATEST_SNAPSHOT"))
        key_strategy = (
            KeyStrategy.coerce(curated_cfg["key_strategy"])
            if curated_cfg.get("key_strategy")
            else None
        )
        plan = build_curated_plan(
            mode=mode,
            all_columns=list(transformed.columns),
            key_strategy=key_strategy,
            key_columns=curated_cfg.get("key_columns", []),
            ordering_column=curated_cfg.get("ordering_column"),
            delete_handling=curated_cfg.get("delete_handling"),
            transforms=[],  # already applied above; plan.transforms unused here
        )

        # --- 4: add the key hash when the strategy produces one ----------- #
        prepared = transformed
        if plan.key_plan is not None and plan.key_plan.produces_hash_key:
            prepared = add_key_hash(prepared, plan.key_plan)

        # --- 5: data quality (quarantine / fail / warn / flag) ------------ #
        rules = build_rules(curated_cfg.get("dq_rules", []))
        if rules:
            prepared, outcomes = self.dq_evaluator.evaluate(prepared, rules)
            for outcome in outcomes:
                # A fail-action breach raises; the runner isolates the table.
                enforce(outcome, curated_ident.table)

        # --- 6: profile business columns for reconciliation --------------- #
        recon_config = build_recon_config(table_config)
        source_profile = self.measurer.profile(
            prepared, key_columns=recon_config.key_columns
        )

        # --- 7: stage, reconcile, commit in the plan's mode --------------- #
        staging = curated_ident.staging(ctx.run_id)

        def gate_factory(_staged_count: int) -> FullReconGate:
            """Build the recon gate by profiling the staged table."""
            staged_df = self._read_table(staging)
            target_profile = self.measurer.profile(
                staged_df, key_columns=recon_config.key_columns
            )
            return FullReconGate(
                source_profile=source_profile,
                target_profile=target_profile,
                config=recon_config,
            )

        result = stage_reconcile_commit(
            writer=self.writer,
            df=prepared,
            target=curated_ident,
            run_id=ctx.run_id,
            mode=plan.commit_mode,
            gate_factory=gate_factory,
            keys=merge_keys(plan),
            ordering=plan.ordering_column,
        )

        return StepOutcome(
            step=step,
            table=curated_ident.table,
            status=StepStatus.SUCCESS,
            rows_written=result.staged_count,
            detail=f"curated built in mode {mode} ({plan.commit_mode})",
        )

    def _read_table(self, ident: TableIdentifier) -> Any:
        """Read a table into a DataFrame (lazy Spark)."""
        spark = self.spark
        if spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            spark = get_or_create_spark()
            self.spark = spark
        return spark.table(ident.qualified())


def _transform_plan(table_config: dict):
    """Build the transform plan from a table's curated config.

    Args:
        table_config: The per-table config.

    Returns:
        The ordered transform plan.
    """
    from umf.engine.transforms import build_transform_plan

    curated = table_config.get("curated") or {}
    return build_transform_plan(curated.get("transforms", []))


def _raw_identifier(table_config: dict) -> TableIdentifier:
    """Return the raw source table identifier.

    The raw table is taken from a ``raw`` target section if present, otherwise
    from the main target (the raw layer this table was written to).

    Args:
        table_config: The per-table config.

    Returns:
        The raw :class:`TableIdentifier`.
    """
    raw = table_config.get("raw_target") or table_config.get("target") or {}
    return TableIdentifier.create(
        schema=raw["schema"], table=raw["table"], catalog=raw.get("catalog")
    )


def _curated_identifier(table_config: dict) -> TableIdentifier:
    """Return the curated target table identifier.

    Args:
        table_config: The per-table config, whose ``curated_target`` section (or
            a derived default) names the curated table.

    Returns:
        The curated :class:`TableIdentifier`.

    Raises:
        ConfigError: If no curated target can be determined.
    """
    curated_target = table_config.get("curated_target")
    if curated_target:
        return TableIdentifier.create(
            schema=curated_target["schema"],
            table=curated_target["table"],
            catalog=curated_target.get("catalog"),
        )
    # Derive a sensible default: same table name in a curated schema if given.
    target = table_config.get("target") or {}
    schema = (table_config.get("curated") or {}).get("schema") or target.get("schema")
    if not schema or not target.get("table"):
        raise ConfigError(
            "Cannot determine the curated target; set curated_target "
            "(schema and table)."
        )
    return TableIdentifier.create(
        schema=schema, table=target["table"], catalog=target.get("catalog")
    )
