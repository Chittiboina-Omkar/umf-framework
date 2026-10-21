"""The DWH-to-Table (raw) step executor.

This executor extracts a warehouse or database table straight into the raw
layer. It reuses the same safe pattern as files-to-raw (describe schema, map
types, fail before reading if a type is unmappable, then read, add audit
columns, stage, reconcile and commit atomically) and adds the two things a
warehouse extract needs:

    * incremental reads, so after the first full load only rows past the stored
      watermark are read; and
    * chunking, so a large table is split along a column and read in parallel,
      bounded and resumable.

The incremental predicate and the chunk plan come from the pure, tested
:mod:`umf.engine.chunking`. The rest is glue over the JDBC reader and the
staging orchestrator, and uses Spark, so it is validated in your environment.
"""

from __future__ import annotations

from typing import Any, Optional

from umf.adapters.source.jdbc import JdbcSourceConfig, JdbcSourceReader
from umf.adapters.table_format.base import CommitMode, TableWriter
from umf.core.constants import LoadType, StepStatus, TableFormat
from umf.core.exceptions import ConfigError
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext, StepOutcome
from umf.engine.audit import add_audit_columns, audit_column_ddl
from umf.engine.chunking import Chunk, incremental_predicate, plan_chunks_from_config, single_chunk
from umf.engine.dag import Node
from umf.engine.executor import StepExecutor
from umf.engine.identifiers import TableIdentifier
from umf.engine.staging import stage_reconcile_commit
from umf.recon.gate import FullReconGate
from umf.recon.measurer import ReconMeasurer
from umf.secrets.registry import SecretRegistry
from umf.steps.common import build_recon_config, plan_or_block
from umf.typing.engine import TypeMappingEngine
from umf.typing.unmappable import HandlingConfig

_log = get_logger("steps.dwh_to_table")


class DwhToTableExecutor(StepExecutor):
    """Extracts a warehouse / database table into the raw layer.

    Attributes:
        writer: The table writer.
        measurer: The reconciliation measurer.
        secrets: The secret registry, to resolve the connection password from a
            reference without ever storing it.
        spark: The Spark session (obtained lazily when needed).
    """

    def __init__(
        self,
        *,
        writer: TableWriter,
        measurer: ReconMeasurer,
        secrets: Optional[SecretRegistry] = None,
        spark: Any = None,
    ) -> None:
        """Create the executor.

        Args:
            writer: The table writer.
            measurer: The reconciliation measurer.
            secrets: The secret registry for resolving the password reference.
            spark: The Spark session (optional).
        """
        self.writer = writer
        self.measurer = measurer
        self.secrets = secrets or SecretRegistry()
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Extract the source table into raw for the node's table.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config, whose ``source`` section is a
                JDBC connection and whose ``load`` section drives incremental
                and chunking behaviour.

        Returns:
            The step outcome. ``NEEDS_DECISION`` for an unmappable type (before
            any read); ``SUCCESS`` after a committed, reconciled extract.
        """
        step = str(node.step)
        source = table_config.get("source") or {}
        load = table_config.get("load") or {}
        target = _target_identifier(table_config)
        table_format = _target_format(table_config)

        # Resolve the connection password from its reference (never stored).
        password = self.secrets.resolve_optional(source.get("password_ref"))

        # Build the chunk plan and the incremental predicate, then combine them.
        chunks = self._plan_chunks(source, load)
        chunks = self._apply_incremental(chunks, load, ctx, target)

        reader = JdbcSourceReader(
            JdbcSourceConfig(
                url=source["url"],
                driver=source["driver"],
                dwh_family=source.get("category_family", source.get("category", "generic")).lower(),
                table=source.get("table"),
                query=source.get("query"),
                user=source.get("user"),
                password=password,
                fetch_size=int(source.get("fetch_size", 10_000)),
                chunks=chunks,
                extra_options=dict(source.get("options", {})),
            ),
            spark=self.spark,
        )

        # Describe and plan types before reading any data.
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
            return block

        # Read, profile source, add audit columns, stage, reconcile, commit.
        df = reader.read()
        recon_config = build_recon_config(table_config)
        source_profile = self.measurer.profile(df, key_columns=recon_config.key_columns)
        audited = add_audit_columns(
            df, run_id=ctx.run_id, source_system=reader.source_name
        )

        target_columns = list(plan.ddl_columns_pairs()) + audit_column_ddl(
            [c.name for c in schema.columns]
        )
        partition_by = (table_config.get("target") or {}).get("partition_by", [])
        staging = target.staging(ctx.run_id)

        def gate_factory(_staged_count: int) -> FullReconGate:
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
            detail=f"extracted from {reader.source_name} into raw ({len(chunks)} chunk(s))",
        )

    def _plan_chunks(self, source: dict, load: dict) -> list:
        """Plan the extraction chunks from the volume class and bounds.

        Args:
            source: The source config section.
            load: The load config section.

        Returns:
            The list of chunks (a single full chunk for small / medium volumes).
        """
        volume_class = str(load.get("volume_class", "medium"))
        column = load.get("chunk_column")
        bounds = load.get("chunk_bounds")  # [min, max]
        num_chunks = int(load.get("num_chunks", 8))
        if str(volume_class).lower() in ("small", "medium"):
            return single_chunk()
        return plan_chunks_from_config(
            volume_class=volume_class,
            column=column,
            bounds=bounds,
            num_chunks=num_chunks,
            window_days=int(load.get("window_days", 30)),
        )

    def _apply_incremental(
        self, chunks: list, load: dict, ctx: RunContext, target: TableIdentifier
    ) -> list:
        """Combine the chunk predicates with the incremental watermark predicate.

        For an incremental load, the watermark predicate is intersected with each
        chunk predicate, so only new rows within each chunk are read. For a full
        load the chunks are returned unchanged.

        Args:
            chunks: The chunk plan.
            load: The load config section.
            ctx: The run context (for logging).
            target: The target identifier (for the watermark key).

        Returns:
            The possibly-narrowed chunk list.
        """
        load_type = str(load.get("type", "FULL")).upper()
        if load_type != LoadType.INCREMENTAL.value:
            return chunks

        column = load.get("watermark_column")
        if not column:
            raise ConfigError("Incremental load requires load.watermark_column.")

        # The watermark value would be read from the watermark store by the
        # runner / control plane and injected into the config for this run. Here
        # we read it from the load section where the caller placed it.
        watermark = load.get("watermark_value")
        quote = bool(load.get("watermark_quote", True))
        inc = incremental_predicate(column=column, watermark=watermark, quote=quote)
        if not inc:
            return chunks

        combined = []
        for ch in chunks:
            predicate = inc if not ch.predicate else f"({ch.predicate}) AND ({inc})"
            combined.append(Chunk(chunk_id=ch.chunk_id, predicate=predicate,
                                  lower=ch.lower, upper=ch.upper))
        return combined

    def _read_table(self, ident: TableIdentifier) -> Any:
        """Read a table for measurement (lazy Spark)."""
        spark = self.spark
        if spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            spark = get_or_create_spark()
            self.spark = spark
        return spark.table(ident.qualified())


def _target_identifier(table_config: dict) -> TableIdentifier:
    """Build the target identifier from a table's config."""
    target = table_config.get("target") or {}
    return TableIdentifier.create(
        schema=target["schema"], table=target["table"], catalog=target.get("catalog")
    )


def _target_format(table_config: dict) -> TableFormat:
    """Return the target format from a table's config."""
    return TableFormat.coerce((table_config.get("target") or {}).get("table_format", "DELTA"))
