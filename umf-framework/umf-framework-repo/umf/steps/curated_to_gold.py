"""The Curated-to-Gold (consumption) step executor.

This executor builds a gold table from business logic and, when configured,
delivers it to the client. It:

    1. compiles and validates the gold logic to a read-only SQL query (the
       structured-to-SQL compilation and the read-only guard are pure and
       tested in :mod:`umf.engine.gold`);
    2. runs the query to produce the gold DataFrame;
    3. stages, reconciles against the query result, and commits atomically,
       overwriting or merging per the refresh mode; and
    4. delivers the result as files, a table or a view, per the delivery plan
       (built and tested in :mod:`umf.engine.delivery`).

Running the SQL and writing the delivery use Spark and are validated in your
environment; the compilation, validation and planning are pure.
"""

from __future__ import annotations

from typing import Any, Optional

from umf.adapters.table_format.base import CommitMode, TableWriter
from umf.core.constants import OutputType, StepStatus
from umf.core.exceptions import ConfigError
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext, StepOutcome
from umf.engine.dag import Node
from umf.engine.delivery import build_delivery_plan
from umf.engine.executor import StepExecutor
from umf.engine.gold import GoldLogic, GoldLogicType, GoldRefresh, build_sql
from umf.engine.identifiers import TableIdentifier
from umf.engine.staging import stage_reconcile_commit
from umf.recon.gate import FullReconGate
from umf.recon.measurer import ReconMeasurer
from umf.steps.common import build_recon_config

_log = get_logger("steps.curated_to_gold")


class CuratedToGoldExecutor(StepExecutor):
    """Builds a gold table from logic and optionally delivers it.

    Attributes:
        writer: The table writer.
        measurer: The reconciliation measurer.
        spark: The Spark session (obtained lazily when needed).
    """

    def __init__(
        self, *, writer: TableWriter, measurer: ReconMeasurer, spark: Any = None
    ) -> None:
        """Create the executor.

        Args:
            writer: The table writer.
            measurer: The reconciliation measurer.
            spark: The Spark session (optional).
        """
        self.writer = writer
        self.measurer = measurer
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Build the gold table and deliver it.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config, whose ``gold`` section carries
                the logic and ``delivery`` section the delivery settings.

        Returns:
            The step outcome.

        Raises:
            ConfigError: If the gold logic is missing or invalid.
        """
        step = str(node.step)
        gold_cfg = table_config.get("gold")
        if not gold_cfg:
            raise ConfigError(f"Step {step} needs a 'gold' config section.")

        logic = GoldLogic(
            name=gold_cfg.get("name", node.table),
            logic_type=GoldLogicType.coerce(gold_cfg.get("logic_type", "SQL")),
            sql=gold_cfg.get("sql"),
            definition=gold_cfg.get("definition"),
            dependencies=list(gold_cfg.get("dependencies", [])),
            refresh=GoldRefresh.coerce(gold_cfg.get("refresh", "FULL")),
        )

        # 1: compile + validate (read-only guard enforced here).
        sql = build_sql(logic)

        # 2: run the query.
        spark = self._session()
        gold_df = spark.sql(sql)

        # 3: stage, reconcile, commit.
        target = _gold_identifier(table_config)
        recon_config = build_recon_config(table_config)
        source_profile = self.measurer.profile(gold_df, key_columns=recon_config.key_columns)
        staging = target.staging(ctx.run_id)
        commit_mode = (
            CommitMode.MERGE if logic.refresh == GoldRefresh.INCREMENTAL else CommitMode.OVERWRITE
        )
        keys = list((table_config.get("gold") or {}).get("keys", []))

        def gate_factory(_n: int) -> FullReconGate:
            staged_df = self._session().table(staging.qualified())
            target_profile = self.measurer.profile(staged_df, key_columns=recon_config.key_columns)
            return FullReconGate(
                source_profile=source_profile, target_profile=target_profile,
                config=recon_config,
            )

        result = stage_reconcile_commit(
            writer=self.writer, df=gold_df, target=target, run_id=ctx.run_id,
            mode=commit_mode, gate_factory=gate_factory,
            keys=keys, ordering=(table_config.get("gold") or {}).get("ordering_column"),
            target_columns=None,  # gold table is created by the first overwrite
        )

        # 4: deliver, if a delivery section is present.
        delivered = ""
        if table_config.get("delivery"):
            delivered = self._deliver(target, table_config)

        return StepOutcome(
            step=step,
            table=target.table,
            status=StepStatus.SUCCESS,
            rows_written=result.staged_count,
            detail=f"gold built ({logic.refresh})" + (f"; {delivered}" if delivered else ""),
        )

    def _deliver(self, gold: TableIdentifier, table_config: dict) -> str:
        """Deliver the gold table per the delivery plan.

        Args:
            gold: The built gold table identifier.
            table_config: The per-table config.

        Returns:
            A short description of what was delivered, for the outcome detail.
        """
        plan = build_delivery_plan(table_config)
        spark = self._session()
        df = spark.table(gold.qualified())

        if plan.output_type in (OutputType.FILES, OutputType.BOTH):
            writer = df
            if plan.single_file:
                writer = df.coalesce(1)
            w = writer.write.format(plan.file_format).mode("overwrite")
            if plan.partition_by:
                w = w.partitionBy(*plan.partition_by)
            w.save(plan.destination)
            if plan.done_marker:
                # Write an empty _SUCCESS marker so a consumer knows it is done.
                self._write_done_marker(plan.destination)

        if plan.output_type in (OutputType.TABLE, OutputType.BOTH):
            df.write.mode("overwrite").saveAsTable(plan.destination)

        if plan.output_type == OutputType.VIEW:
            spark.sql(
                f"CREATE OR REPLACE VIEW {plan.destination} AS "
                f"SELECT * FROM {gold.qualified()}"
            )

        return f"delivered as {plan.output_type} to {plan.destination}"

    def _write_done_marker(self, destination: str) -> None:
        """Write a ``_SUCCESS`` marker file under a delivery destination.

        Args:
            destination: The delivery path.
        """
        spark = self._session()
        # An empty single-row write of nothing is awkward; instead use the
        # Hadoop FileSystem via the JVM to create a zero-byte marker. Kept small
        # and best-effort; a missing marker is not fatal.
        try:
            jvm = spark._jvm
            hadoop_conf = spark._jsc.hadoopConfiguration()
            path = jvm.org.apache.hadoop.fs.Path(f"{destination}/_SUCCESS")
            fs = path.getFileSystem(hadoop_conf)
            fs.create(path).close()
        except Exception as exc:  # best effort
            _log.warning("could not write done marker", extra={"error": repr(exc)})

    def _session(self) -> Any:
        """Return the Spark session (lazy)."""
        if self.spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            self.spark = get_or_create_spark()
        return self.spark


def _gold_identifier(table_config: dict) -> TableIdentifier:
    """Build the gold target identifier from a table's config.

    Args:
        table_config: The per-table config.

    Returns:
        The gold :class:`TableIdentifier`, from a ``gold_target`` section or the
        main target.

    Raises:
        ConfigError: If no gold target can be determined.
    """
    gold_target = table_config.get("gold_target") or table_config.get("target")
    if not gold_target or not gold_target.get("table"):
        raise ConfigError("Cannot determine the gold target (schema and table).")
    return TableIdentifier.create(
        schema=gold_target["schema"],
        table=gold_target["table"],
        catalog=gold_target.get("catalog"),
    )
