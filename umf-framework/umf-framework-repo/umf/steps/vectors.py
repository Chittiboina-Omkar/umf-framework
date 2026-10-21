"""The Vectors and Volume/Table step executors.

These two executors cover the non-tabular and conversion operations:

    * VectorsExecutor builds or refreshes a vector store from a source,
      computing the delete-sync so the store stays faithful to the source
      (chunking, embedding and the store are pluggable and done through
      adapters; the delete-sync decision is the pure, tested part).
    * VolumeTableExecutor handles the conversions between volumes and tables:
      a file inventory table, a parsed content table, exporting a table to
      files, or copying files across storage.

The embedding model and the vector store are external services, so the vectors
executor is a thin, clearly pluggable skeleton: it resolves the plan and the
delete-sync, then calls the injected embedder and store. The volume/table
executor uses Spark for the data movement. Both are validated in your
environment; the planning they rely on is tested.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence

from umf.adapters.vector.base import VectorStore
from umf.core.constants import StepStatus
from umf.core.exceptions import ConfigError
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext, StepOutcome
from umf.engine.dag import Node
from umf.engine.delivery import (
    VolumeTableOp,
    delete_sync_plan,
    plan_vectors,
    resolve_volume_table_op,
)
from umf.engine.executor import StepExecutor

_log = get_logger("steps.vectors")


# An embedder turns a list of texts into a list of vectors. Injected so the
# embedding model is a plug-in, not a hard dependency.
Embedder = Callable[[Sequence[str]], List[List[float]]]


class VectorsExecutor(StepExecutor):
    """Builds or refreshes a vector store from a source.

    Attributes:
        store: The vector store backend.
        embedder: A callable that embeds a batch of texts.
        spark: The Spark session, used to read the source ids and text.
    """

    def __init__(
        self,
        *,
        store: VectorStore,
        embedder: Embedder,
        spark: Any = None,
    ) -> None:
        """Create the executor.

        Args:
            store: The vector store backend.
            embedder: The embedding function.
            spark: The Spark session (optional).
        """
        self.store = store
        self.embedder = embedder
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Build or refresh vectors for the node's source.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config with a ``vector`` section.

        Returns:
            The step outcome, reporting vectors written and removed.
        """
        plan = plan_vectors(table_config)
        spark = self._session()

        # Read the current source ids to compute what must change.
        source_df = spark.table(plan.source_ref) if "." in plan.source_ref else spark.read.load(plan.source_ref)
        source_ids = {
            str(row[plan.id_column])
            for row in source_df.select(plan.id_column).distinct().collect()
        }
        existing = set(self.store.list_ids(plan.store))

        sync = delete_sync_plan(source_ids, existing)

        # Delete vectors whose source is gone.
        deleted = self.store.delete(plan.store, sync.to_delete) if sync.to_delete else 0

        # Embed and upsert the new items. (Chunking and batching would be applied
        # here; kept straightforward in this skeleton and validated in your
        # environment with a real embedder and store.)
        written = 0
        if sync.to_add:
            rows = (
                source_df.filter(source_df[plan.id_column].isin(list(sync.to_add)))
                .select(plan.id_column, plan.text_column, *plan.metadata_columns)
                .collect()
            )
            texts = [str(r[plan.text_column]) for r in rows]
            vectors = self.embedder(texts) if texts else []
            items = [
                {
                    "id": str(r[plan.id_column]),
                    "vector": vec,
                    "metadata": {m: r[m] for m in plan.metadata_columns},
                }
                for r, vec in zip(rows, vectors)
            ]
            written = self.store.upsert(plan.store, items) if items else 0

        return StepOutcome(
            step=str(node.step),
            table=plan.source_ref,
            status=StepStatus.SUCCESS,
            rows_written=written,
            detail=f"vectors synced: +{written} / -{deleted} (store {plan.store})",
        )

    def _session(self) -> Any:
        """Return the Spark session (lazy)."""
        if self.spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            self.spark = get_or_create_spark()
        return self.spark


class VolumeTableExecutor(StepExecutor):
    """Converts between volumes and tables.

    Attributes:
        spark: The Spark session (obtained lazily).
    """

    def __init__(self, *, spark: Any = None) -> None:
        """Create the executor.

        Args:
            spark: The Spark session (optional).
        """
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Perform the configured volume/table operation.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config with a ``volume`` section.

        Returns:
            The step outcome.

        Raises:
            ConfigError: If the operation or its settings are invalid.
        """
        op = resolve_volume_table_op(table_config)
        vol = table_config.get("volume") or {}
        spark = self._session()

        if op == VolumeTableOp.VOLUME_TO_TABLE_INVENTORY:
            # Build a table of file metadata (path, size, modified time).
            from pyspark.sql import functions as F

            df = (
                spark.read.format("binaryFile")
                .option("pathGlobFilter", vol.get("glob", "*"))
                .load(vol["path"])
                .select(
                    F.col("path"),
                    F.col("length").alias("size_bytes"),
                    F.col("modificationTime").alias("modified_time"),
                )
            )
            df.write.mode("overwrite").saveAsTable(vol["target_table"])
            count = df.count()
            detail = f"inventory of {vol['path']} -> {vol['target_table']} ({count} files)"

        elif op == VolumeTableOp.TABLE_TO_VOLUME:
            df = spark.table(vol["source_table"])
            w = df.write.format(vol.get("format", "parquet")).mode("overwrite")
            if vol.get("partition_by"):
                w = w.partitionBy(*vol["partition_by"])
            w.save(vol["path"])
            detail = f"exported {vol['source_table']} -> {vol['path']}"

        elif op == VolumeTableOp.VOLUME_TO_VOLUME:
            df = spark.read.format(vol.get("format", "parquet")).load(vol["source_path"])
            df.write.format(vol.get("format", "parquet")).mode("overwrite").save(vol["path"])
            detail = f"copied {vol['source_path']} -> {vol['path']}"

        else:  # VOLUME_TO_TABLE_CONTENT
            # Parsed content extraction (text / OCR) is content-type specific and
            # arrives with the rich-content handling; here it is explicitly out
            # of scope and reported rather than silently doing the wrong thing.
            raise ConfigError(
                "Parsed content extraction (volume_to_table content) is handled "
                "with the rich-content handler, not yet available in this build."
            )

        return StepOutcome(
            step=str(node.step),
            table=str(vol.get("target_table") or vol.get("path", "")),
            status=StepStatus.SUCCESS,
            detail=detail,
        )

    def _session(self) -> Any:
        """Return the Spark session (lazy)."""
        if self.spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            self.spark = get_or_create_spark()
        return self.spark
