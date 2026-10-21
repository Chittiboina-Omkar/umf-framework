"""The Files-to-Raw step executor.

This executor carries a table from files into the raw layer, wiring together the
pieces built in earlier parts in the one correct order:

    1. describe the source schema (layer L1);
    2. map the types to the target, exactly or by configured decision (L2); if
       any column is unmappable with no decision, stop here with a
       ``NEEDS_DECISION`` outcome, before reading any data;
    3. read the data (L1);
    4. add the audit columns that make raw an exact mirror plus provenance;
    5. profile the business columns of the source for reconciliation;
    6. stage, reconcile against the source profile, and commit atomically, or
       discard if reconciliation fails (Part 4 and Part 5).

Everything up to and including step 2 is pure and does not touch Spark, which is
why the "fail the table before reading data" behaviour is tested here without a
Spark runtime. Steps 3 onward use Spark through the collaborators and are
validated in your environment.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from umf.adapters.source.base import SourceReader
from umf.adapters.table_format.base import CommitMode, TableWriter
from umf.core.constants import StepStatus, TableFormat
from umf.core.run_context import RunContext, StepOutcome
from umf.core.logging_setup import get_logger
from umf.engine.audit import add_audit_columns, audit_column_ddl
from umf.engine.dag import Node
from umf.engine.executor import StepExecutor
from umf.engine.identifiers import TableIdentifier
from umf.engine.staging import stage_reconcile_commit
from umf.recon.gate import FullReconGate
from umf.recon.measurer import ReconMeasurer
from umf.steps.common import build_recon_config, business_columns, plan_or_block
from umf.typing.engine import TypeMappingEngine
from umf.typing.unmappable import HandlingConfig

_log = get_logger("steps.files_to_raw")


class FilesToRawExecutor(StepExecutor):
    """Executes the files-to-raw step for one table.

    Attributes:
        writer: The table writer for the target format.
        measurer: The reconciliation measurer.
        reader_factory: A callable that builds the source reader from a table's
            config. Injected so tests can supply a reader that needs no Spark.
        spark: The Spark session, used to read a staging table for the recon
            gate. Not needed for the pre-read, type-planning path.
    """

    def __init__(
        self,
        *,
        writer: TableWriter,
        measurer: ReconMeasurer,
        reader_factory: Callable[[dict], SourceReader],
        spark: Any = None,
    ) -> None:
        """Create the executor with its collaborators.

        Args:
            writer: The table writer.
            measurer: The reconciliation measurer.
            reader_factory: Builds a source reader from a table's config.
            spark: The Spark session (optional; obtained lazily when needed).
        """
        self.writer = writer
        self.measurer = measurer
        self.reader_factory = reader_factory
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Run the files-to-raw step for the node's table.

        Args:
            ctx: The run context.
            node: The DAG node (step and table).
            table_config: The per-table config.

        Returns:
            The step outcome. ``NEEDS_DECISION`` if a type is unmappable with no
            configured decision; ``SUCCESS`` after a committed, reconciled write.
        """
        step = str(node.step)
        target = _target_identifier(table_config)
        table_format = _target_format(table_config)

        # --- 1 & 2: describe schema, plan types (no data read yet) -------- #
        reader = self.reader_factory(table_config)
        schema = reader.describe_schema()

        engine = TypeMappingEngine(
            source_name=reader.source_name,
            table_format=table_format,
            handling=HandlingConfig.from_config(table_config.get("data_type_handling")),
        )
        plan, block = plan_or_block(
            engine, step=step, table=target.table, source_columns=schema.columns
        )
        if block is not None:
            # Unmappable column(s) with no decision: fail this table cleanly.
            return block

        # --- 3: read the data --------------------------------------------- #
        df = reader.read()
        source_system = str(table_config.get("source", {}).get("system", reader.source_name))

        # --- 5 (part): profile the source business columns for recon ------ #
        recon_config = build_recon_config(table_config)
        source_profile = self.measurer.profile(df, key_columns=recon_config.key_columns)

        # --- 4: add the audit columns (exact mirror + provenance) --------- #
        audited = add_audit_columns(df, run_id=ctx.run_id, source_system=source_system)

        # Build the target column list: resolved business columns plus audit.
        target_columns = list(plan.ddl_columns_pairs()) + audit_column_ddl(
            [c.name for c in schema.columns]
        )
        partition_by = (table_config.get("target") or {}).get("partition_by", [])

        # --- 6: stage, reconcile against the source profile, commit ------- #
        staging = target.staging(ctx.run_id)

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
            df=audited,
            target=target,
            run_id=ctx.run_id,
            mode=CommitMode.APPEND,
            gate_factory=gate_factory,
            target_columns=target_columns,
            partition_by=partition_by,
        )

        return StepOutcome(
            step=step,
            table=target.table,
            status=StepStatus.SUCCESS,
            rows_read=result.staged_count,
            rows_written=result.staged_count,
            detail="files ingested into raw, reconciled and committed",
        )

    def _read_table(self, ident: TableIdentifier) -> Any:
        """Read a table into a DataFrame for measurement (lazy Spark)."""
        spark = self.spark
        if spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            spark = get_or_create_spark()
            self.spark = spark
        return spark.table(ident.qualified())


def _target_identifier(table_config: dict) -> TableIdentifier:
    """Build the target table identifier from a table's config.

    Args:
        table_config: The per-table config.

    Returns:
        The validated target :class:`TableIdentifier`.
    """
    target = table_config.get("target") or {}
    return TableIdentifier.create(
        schema=target["schema"],
        table=target["table"],
        catalog=target.get("catalog"),
    )


def _target_format(table_config: dict) -> TableFormat:
    """Return the target table format from a table's config.

    Args:
        table_config: The per-table config.

    Returns:
        The :class:`~umf.core.constants.TableFormat`.
    """
    fmt = (table_config.get("target") or {}).get("table_format", "DELTA")
    return TableFormat.coerce(fmt)
