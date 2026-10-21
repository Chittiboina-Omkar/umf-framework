"""Building the executor registry with the real, Spark-backed step executors.

The runner is given an :class:`~umf.engine.executor.ExecutorRegistry` that maps
each step to the executor that runs it. :func:`build_executor_registry` wires the
real executors for the steps implemented so far (files to raw, raw to curated),
each sharing a table writer and reconciliation measurer that use the run's
Spark session.

Steps not yet implemented (warehouse extract, gold, vectors) are simply not
registered; if a run needs one, that node fails in isolation with a clear "no
executor registered" message and the rest of the run proceeds. This lets the
framework be rolled out step by step without a half-built step silently doing
the wrong thing.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from umf.adapters.source.base import SourceReader
from umf.adapters.source.files import FileFormat, FileSourceConfig, FileSourceReader
from umf.adapters.table_format import get_writer
from umf.core.constants import Step, TableFormat
from umf.core.exceptions import ConfigError
from umf.engine.executor import ExecutorRegistry
from umf.recon.measurer import SparkReconMeasurer
from umf.secrets.registry import SecretRegistry
from umf.steps.curated_to_gold import CuratedToGoldExecutor
from umf.steps.dwh_to_files import DwhToFilesExecutor
from umf.steps.dwh_to_table import DwhToTableExecutor
from umf.steps.files_to_raw import FilesToRawExecutor
from umf.steps.raw_to_curated import RawToCuratedExecutor
from umf.steps.vectors import VolumeTableExecutor
from umf.typing.source_column import SourceColumn


def default_file_reader_factory(table_config: dict) -> SourceReader:
    """Build a file source reader from a table's config.

    Args:
        table_config: The per-table config, whose ``source`` section describes
            the files to read (path, format, options, and an optional schema
            contract).

    Returns:
        A :class:`~umf.adapters.source.files.FileSourceReader`.

    Raises:
        ConfigError: If the source section is missing a path or format.
    """
    source = table_config.get("source") or {}
    path = source.get("path")
    fmt = source.get("format")
    if not path or not fmt:
        raise ConfigError(
            "A file source needs source.path and source.format.",
            context={"has_path": bool(path), "has_format": bool(fmt)},
        )

    # Build an optional schema contract from the config, so CSV / JSON can supply
    # authoritative types rather than relying on inference.
    contract = None
    raw_contract = source.get("contract")
    if raw_contract:
        contract = [
            SourceColumn(
                name=c["name"],
                native_type=c["type"],
                precision=c.get("precision"),
                scale=c.get("scale"),
                length=c.get("length"),
                nullable=c.get("nullable", True),
            )
            for c in raw_contract
        ]

    config = FileSourceConfig(
        path=path,
        file_format=FileFormat.coerce(fmt),
        options=dict(source.get("options", {})),
        contract=contract,
        strict=bool(source.get("strict", True)),
    )
    return FileSourceReader(config)


def build_executor_registry(
    *,
    table_format: TableFormat = TableFormat.DELTA,
    spark: Any = None,
    reader_factory: Optional[Callable[[dict], SourceReader]] = None,
    secrets: Optional[SecretRegistry] = None,
) -> ExecutorRegistry:
    """Build a registry with the real step executors wired up.

    Args:
        table_format: The default target table format; selects the writer.
        spark: The Spark session to use for writing and measuring.
        reader_factory: A factory for source readers, defaulting to the file
            reader factory. Injected so a different source family can be plugged
            in.
        secrets: The secret registry for resolving connection passwords from
            references; a default (environment-variable) registry is used if
            omitted.

    Returns:
        An :class:`ExecutorRegistry` with the implemented steps registered.
    """
    writer = get_writer(table_format, spark)
    measurer = SparkReconMeasurer()
    readers = reader_factory or default_file_reader_factory
    secret_registry = secrets or SecretRegistry()

    registry = ExecutorRegistry()
    registry.register(
        Step.FILES_TO_RAW,
        FilesToRawExecutor(
            writer=writer, measurer=measurer, reader_factory=readers, spark=spark
        ),
    )
    registry.register(
        Step.RAW_TO_CURATED,
        RawToCuratedExecutor(writer=writer, measurer=measurer, spark=spark),
    )
    registry.register(
        Step.DWH_TO_TABLE,
        DwhToTableExecutor(
            writer=writer, measurer=measurer, secrets=secret_registry, spark=spark
        ),
    )
    registry.register(
        Step.CURATED_TO_GOLD,
        CuratedToGoldExecutor(writer=writer, measurer=measurer, spark=spark),
    )
    registry.register(
        Step.DWH_TO_FILES,
        DwhToFilesExecutor(secrets=secret_registry, spark=spark),
    )
    registry.register(Step.VOLUME_TABLE, VolumeTableExecutor(spark=spark))
    # The vectors step needs a vector store and an embedder, which are external
    # services supplied by the caller; it is registered separately via
    # register_vectors() when those are available.
    return registry


def register_vectors(
    registry: ExecutorRegistry, *, store: Any, embedder: Any, spark: Any = None
) -> None:
    """Register the vectors executor with a concrete store and embedder.

    The vectors step depends on an external vector store and embedding model, so
    it is registered only when the caller provides them, rather than wired by
    default.

    Args:
        registry: The registry to add the vectors executor to.
        store: The vector store backend.
        embedder: The embedding function.
        spark: The Spark session (optional).
    """
    from umf.steps.vectors import VectorsExecutor

    registry.register(
        Step.VECTORS, VectorsExecutor(store=store, embedder=embedder, spark=spark)
    )
