"""The DWH-to-Files step executor.

This executor extracts a warehouse or database table to files for delivery to a
client, writing two companions beside the data so the extract is self-describing
and verifiable:

    * a schema sidecar, recording each column's native type, precision, scale,
      length and nullability, so a reader (including this framework) keeps the
      exact types even for CSV, which carries none; and
    * a manifest, recording the files, the row count and a checksum, so a
      consumer can confirm the extract is complete before using it.

Reading over JDBC and writing the files use Spark; the sidecar and manifest
construction are pure (tested in :mod:`umf.engine.delivery`). Parquet is the
default output because it preserves types; CSV is available but always paired
with the sidecar.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from umf.adapters.source.jdbc import JdbcSourceConfig, JdbcSourceReader
from umf.core.constants import StepStatus
from umf.core.logging_setup import get_logger
from umf.core.run_context import RunContext, StepOutcome
from umf.engine.chunking import single_chunk
from umf.engine.dag import Node
from umf.engine.delivery import build_manifest, build_schema_sidecar
from umf.engine.executor import StepExecutor
from umf.secrets.registry import SecretRegistry

_log = get_logger("steps.dwh_to_files")


class DwhToFilesExecutor(StepExecutor):
    """Extracts a warehouse table to files with a sidecar and manifest.

    Attributes:
        secrets: The secret registry, to resolve the connection password.
        spark: The Spark session (obtained lazily).
    """

    def __init__(
        self, *, secrets: Optional[SecretRegistry] = None, spark: Any = None
    ) -> None:
        """Create the executor.

        Args:
            secrets: The secret registry for the password reference.
            spark: The Spark session (optional).
        """
        self.secrets = secrets or SecretRegistry()
        self.spark = spark

    def execute(self, ctx: RunContext, node: Node, table_config: dict) -> StepOutcome:
        """Extract the source table to files for the node's table.

        Args:
            ctx: The run context.
            node: The DAG node.
            table_config: The per-table config with a JDBC ``source`` and a
                ``delivery`` section naming the destination path and format.

        Returns:
            The step outcome.
        """
        source = table_config.get("source") or {}
        delivery = table_config.get("delivery") or {}
        destination = delivery.get("destination")
        file_format = str(delivery.get("file_format", "parquet"))

        password = self.secrets.resolve_optional(source.get("password_ref"))
        reader = JdbcSourceReader(
            JdbcSourceConfig(
                url=source["url"],
                driver=source["driver"],
                dwh_family=source.get("category_family", source.get("category", "generic")).lower(),
                table=source.get("table"),
                query=source.get("query"),
                user=source.get("user"),
                password=password,
                chunks=single_chunk(),
                extra_options=dict(source.get("options", {})),
            ),
            spark=self.spark,
        )

        # Describe the schema and write the sidecar (exact types preserved).
        schema = reader.describe_schema()
        sidecar = build_schema_sidecar(schema.columns)

        # Read and write the data.
        df = reader.read()
        spark = self._session()
        w = df.write.format(file_format).mode("overwrite")
        if delivery.get("partition_by"):
            w = w.partitionBy(*delivery["partition_by"])
        w.save(destination)

        row_count = df.count()
        manifest = build_manifest(
            files=[destination],
            row_count=row_count,
            run_id=ctx.run_id,
            source=reader.source_name,
        )

        # Write the sidecar and manifest beside the data (best effort on the
        # marker write; the data itself is already safely written).
        self._write_json(f"{destination}/_schema.json", sidecar)
        self._write_json(f"{destination}/_manifest.json", manifest)

        return StepOutcome(
            step=str(node.step),
            table=node.table,
            status=StepStatus.SUCCESS,
            rows_read=row_count,
            rows_written=row_count,
            detail=f"extracted {reader.source_name} to files at {destination}",
        )

    def _write_json(self, path: str, data: dict) -> None:
        """Write a small JSON companion file via the Hadoop FileSystem.

        Args:
            path: The destination path.
            data: The JSON-serialisable content.
        """
        spark = self._session()
        try:
            jvm = spark._jvm
            hadoop_conf = spark._jsc.hadoopConfiguration()
            p = jvm.org.apache.hadoop.fs.Path(path)
            fs = p.getFileSystem(hadoop_conf)
            stream = fs.create(p, True)
            stream.write(bytearray(json.dumps(data, indent=2), "utf-8"))
            stream.close()
        except Exception as exc:  # best effort
            _log.warning("could not write companion file",
                         extra={"path": path, "error": repr(exc)})

    def _session(self) -> Any:
        """Return the Spark session (lazy)."""
        if self.spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            self.spark = get_or_create_spark()
        return self.spark
