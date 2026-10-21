"""Planning client delivery, vectors, volume/table operations and manifests.

This module holds the pure planning logic for the consumption-side and auxiliary
operations, kept here so it can be tested without Spark or any external service:

    * delivery       what to produce for the client (files / table / view) and
                      how (format, partitioning, a done marker);
    * vectors         how to chunk and embed, and the delete-sync decision that
                      keeps the vector store in step with the source;
    * volume/table    which direction an operation goes and what it produces;
    * manifests       the schema sidecar and manifest written beside extracted
                      files so completeness and types are recorded.

The executors that actually write files, build vectors or move volumes use these
plans and the relevant Spark or service adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from umf.core.constants import OutputType, _StrEnum
from umf.core.exceptions import ConfigError


# --- client delivery ------------------------------------------------------- #


@dataclass(frozen=True)
class DeliveryPlan:
    """What and how to deliver to the client.

    Attributes:
        output_type: Files, table, view, or both.
        destination: Where to deliver (a path for files, an identifier for a
            table or view).
        file_format: The file format for a files delivery (for example
            ``"parquet"`` or ``"csv"``).
        partition_by: Columns to partition a files delivery by.
        single_file: Whether to coalesce a files delivery into one file.
        done_marker: Whether to write a ``_SUCCESS`` marker after delivery, so a
            downstream consumer knows the delivery is complete.
        dated_folder: Whether to deliver into a dated folder (keeping history)
            rather than overwriting.
    """

    output_type: OutputType
    destination: str
    file_format: str = "parquet"
    partition_by: Sequence[str] = field(default_factory=tuple)
    single_file: bool = False
    done_marker: bool = True
    dated_folder: bool = False


def build_delivery_plan(config: dict) -> DeliveryPlan:
    """Build a delivery plan from a table's ``delivery`` config section.

    Args:
        config: The per-table config.

    Returns:
        The :class:`DeliveryPlan`.

    Raises:
        ConfigError: If the delivery section is missing a destination.
    """
    delivery = config.get("delivery") or {}
    output_type = OutputType.coerce(delivery.get("output_type", "TABLE"))
    destination = delivery.get("destination") or (config.get("target") or {}).get("table")
    if not destination:
        raise ConfigError("Delivery needs a destination (a path or a table name).")
    return DeliveryPlan(
        output_type=output_type,
        destination=destination,
        file_format=str(delivery.get("file_format", "parquet")),
        partition_by=list(delivery.get("partition_by", [])),
        single_file=bool(delivery.get("single_file", False)),
        done_marker=bool(delivery.get("done_marker", True)),
        dated_folder=bool(delivery.get("dated_folder", False)),
    )


# --- vectors --------------------------------------------------------------- #


class ChunkStrategy(_StrEnum):
    """How source text is split into chunks before embedding."""

    FIXED = "FIXED"          # fixed-size windows with overlap
    SENTENCE = "SENTENCE"    # split on sentence boundaries
    PARAGRAPH = "PARAGRAPH"  # split on paragraph boundaries


@dataclass(frozen=True)
class VectorPlan:
    """How to build vectors from a source.

    Attributes:
        source_ref: The source table or volume reference.
        id_column: The column uniquely identifying each source item, used for
            delete-sync.
        text_column: The column holding the text to embed.
        metadata_columns: Columns carried alongside each vector.
        chunk_strategy: How text is chunked.
        chunk_size: The chunk size (characters or tokens, per the embedder).
        chunk_overlap: Overlap between consecutive chunks.
        embed_model: The embedding model identifier.
        dimension: The embedding dimension.
        store: The target vector store name.
    """

    source_ref: str
    id_column: str
    text_column: str
    metadata_columns: Sequence[str]
    chunk_strategy: ChunkStrategy
    chunk_size: int
    chunk_overlap: int
    embed_model: str
    dimension: int
    store: str


def plan_vectors(config: dict) -> VectorPlan:
    """Build a vector plan from a table's ``vector`` config section.

    Args:
        config: The per-table config.

    Returns:
        The :class:`VectorPlan`.

    Raises:
        ConfigError: If required vector settings are missing.
    """
    vector = config.get("vector") or {}
    required = ("source_ref", "id_column", "text_column", "embed_model", "store")
    missing = [k for k in required if not vector.get(k)]
    if missing:
        raise ConfigError(f"Vector config missing required keys: {missing}.")
    return VectorPlan(
        source_ref=vector["source_ref"],
        id_column=vector["id_column"],
        text_column=vector["text_column"],
        metadata_columns=list(vector.get("metadata_columns", [])),
        chunk_strategy=ChunkStrategy.coerce(vector.get("chunk_strategy", "FIXED")),
        chunk_size=int(vector.get("chunk_size", 1000)),
        chunk_overlap=int(vector.get("chunk_overlap", 100)),
        embed_model=vector["embed_model"],
        dimension=int(vector.get("dimension", 1536)),
        store=vector["store"],
    )


@dataclass
class DeleteSyncPlan:
    """Which vectors to add, delete and keep to match the source.

    Attributes:
        to_add: Source item ids not yet in the vector store.
        to_delete: Vector store ids whose source item is gone (delete-sync).
        to_keep: Ids present on both sides.
    """

    to_add: List[str] = field(default_factory=list)
    to_delete: List[str] = field(default_factory=list)
    to_keep: List[str] = field(default_factory=list)


def delete_sync_plan(
    source_ids: Set[str], vector_ids: Set[str]
) -> DeleteSyncPlan:
    """Decide the add / delete / keep sets to sync vectors with the source.

    This keeps the vector store faithful to the source: new items are added,
    items removed from the source have their vectors removed, and unchanged
    items are kept. It is set arithmetic, so it is exact and trivially tested.

    Args:
        source_ids: The ids currently present in the source.
        vector_ids: The ids currently present in the vector store.

    Returns:
        A :class:`DeleteSyncPlan`.
    """
    return DeleteSyncPlan(
        to_add=sorted(source_ids - vector_ids),
        to_delete=sorted(vector_ids - source_ids),
        to_keep=sorted(source_ids & vector_ids),
    )


# --- volume / table operations -------------------------------------------- #


class VolumeTableOp(_StrEnum):
    """The concrete operation a volume/table step performs."""

    VOLUME_TO_TABLE_INVENTORY = "VOLUME_TO_TABLE_INVENTORY"  # file metadata table
    VOLUME_TO_TABLE_CONTENT = "VOLUME_TO_TABLE_CONTENT"      # parsed content table
    TABLE_TO_VOLUME = "TABLE_TO_VOLUME"                      # export a table to files
    VOLUME_TO_VOLUME = "VOLUME_TO_VOLUME"                    # copy files across


def resolve_volume_table_op(config: dict) -> VolumeTableOp:
    """Resolve which volume/table operation a config asks for.

    Args:
        config: The per-table config, whose ``volume`` section names the
            direction and, for a volume-to-table, whether to produce an
            inventory or parsed content.

    Returns:
        The :class:`VolumeTableOp`.

    Raises:
        ConfigError: If the direction is missing or unrecognised.
    """
    vol = config.get("volume") or {}
    direction = str(vol.get("direction", "")).strip().lower()
    if direction == "volume_to_table":
        kind = str(vol.get("table_kind", "inventory")).lower()
        return (
            VolumeTableOp.VOLUME_TO_TABLE_CONTENT
            if kind == "content"
            else VolumeTableOp.VOLUME_TO_TABLE_INVENTORY
        )
    if direction == "table_to_volume":
        return VolumeTableOp.TABLE_TO_VOLUME
    if direction == "volume_to_volume":
        return VolumeTableOp.VOLUME_TO_VOLUME
    raise ConfigError(
        "volume.direction must be one of: volume_to_table, table_to_volume, "
        f"volume_to_volume (got '{direction}')."
    )


# --- manifests and schema sidecars ---------------------------------------- #


def build_schema_sidecar(source_columns: Sequence[Any]) -> Dict[str, Any]:
    """Build a schema sidecar describing extracted columns and their types.

    Written beside extracted files so that a downstream reader (including this
    framework) knows the exact types, which plain CSV would otherwise lose.

    Args:
        source_columns: The source columns (objects with ``name``,
            ``native_type``, ``precision``, ``scale``, ``length``, ``nullable``).

    Returns:
        A JSON-serialisable dictionary describing the schema.
    """
    return {
        "version": 1,
        "columns": [
            {
                "name": c.name,
                "type": c.native_type,
                "precision": c.precision,
                "scale": c.scale,
                "length": c.length,
                "nullable": c.nullable,
            }
            for c in source_columns
        ],
    }


def build_manifest(
    *, files: Sequence[str], row_count: int, checksum: Optional[str] = None,
    run_id: str = "", source: str = "",
) -> Dict[str, Any]:
    """Build a manifest recording a completed extraction.

    The manifest lets a consumer confirm the extraction is complete and intact
    before processing: it lists the files, the total row count and an optional
    checksum.

    Args:
        files: The file paths produced.
        row_count: The total rows extracted.
        checksum: An optional overall checksum.
        run_id: The run that produced the extraction.
        source: The source identifier.

    Returns:
        A JSON-serialisable manifest dictionary.
    """
    return {
        "version": 1,
        "run_id": run_id,
        "source": source,
        "file_count": len(files),
        "files": list(files),
        "row_count": row_count,
        "checksum": checksum,
    }
