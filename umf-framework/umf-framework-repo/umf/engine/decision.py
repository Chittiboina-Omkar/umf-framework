"""Deciding which steps to run from the source and target.

The user describes what they have (a source) and what they want (how far the
data should go), and the framework decides the ordered list of steps. This keeps
the user from having to know the internal step names or their order: they say
"a database, bring it to curated" and the framework runs the warehouse extract
then the curated build, in that order.

An explicit list of steps in the config always wins, for the cases where someone
wants to run exactly one step (for example re-running only the curated build).
When the config leaves steps as ``AUTO``, the rules here derive them.

This module is pure Python and fully tested.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

from umf.core.constants import Step, _StrEnum
from umf.core.exceptions import ConfigError


class SourceKind(_StrEnum):
    """The broad kind of source, which decides the first step.

    These are coarser than the many source categories (Oracle, Snowflake, S3,
    ...): what matters for step selection is only whether the source is a
    warehouse / database (needs extraction), files, or an existing layer in the
    lakehouse that a later step reads directly.
    """

    DWH = "DWH"          # a warehouse or database that must be extracted
    FILES = "FILES"      # files on storage
    RAW = "RAW"          # an existing raw table (start from curated)
    CURATED = "CURATED"  # an existing curated table (start from consumption)
    VOLUME = "VOLUME"    # a files volume (for volume/table/vector operations)
    TABLE = "TABLE"      # an existing table (for table/volume/vector operations)
    VECTOR = "VECTOR"    # an existing vector store


class TargetLayer(_StrEnum):
    """How far the data should go, which decides the last step."""

    FILES_CLIENT = "FILES_CLIENT"    # deliver files to the client
    TABLE_CLIENT = "TABLE_CLIENT"    # deliver a table to the client (lands in raw)
    RAW = "RAW"                      # stop at raw
    CURATED = "CURATED"              # stop at curated
    CONSUMPTION = "CONSUMPTION"      # build gold / deliver
    VECTORS = "VECTORS"              # build vectors
    VOLUME = "VOLUME"                # produce a volume (export / copy)


# The ordered "spine" of layer-building steps. Deriving a pipeline means taking a
# contiguous slice of this spine that starts where the source sits and ends at
# the requested target layer.
_SPINE = [
    Step.FILES_TO_RAW,      # files -> raw
    Step.RAW_TO_CURATED,    # raw -> curated
    Step.CURATED_TO_GOLD,   # curated -> consumption
]


def decide_steps(
    *,
    source_kind: SourceKind,
    target_layer: TargetLayer,
    with_vectors: bool = False,
    deliver_files: bool = False,
) -> List[Step]:
    """Derive the ordered steps for a source and target.

    Args:
        source_kind: What the source is.
        target_layer: How far the data should go.
        with_vectors: Whether to also build vectors at the end.
        deliver_files: For a warehouse source going only to files, whether the
            delivery is files (``DWH_TO_FILES``) rather than a table.

    Returns:
        The ordered list of steps.

    Raises:
        ConfigError: If the source and target combination is not supported.
    """
    steps: List[Step] = []

    # --- volume / table / vector operations are standalone ---------------- #
    if target_layer == TargetLayer.VOLUME or source_kind in (
        SourceKind.VOLUME,
        SourceKind.TABLE,
        SourceKind.VECTOR,
    ):
        if with_vectors or target_layer == TargetLayer.VECTORS:
            return [Step.VECTORS]
        return [Step.VOLUME_TABLE]

    # --- the first step depends on the source ----------------------------- #
    if source_kind == SourceKind.DWH:
        if target_layer == TargetLayer.FILES_CLIENT:
            # Warehouse straight to files for the client; no raw build.
            result = [Step.DWH_TO_FILES]
            if with_vectors:
                result.append(Step.VECTORS)
            return result
        # Otherwise the warehouse lands directly in raw.
        steps.append(Step.DWH_TO_TABLE)
        spine_start = 1  # continue the spine at RAW_TO_CURATED
    elif source_kind == SourceKind.FILES:
        steps.append(Step.FILES_TO_RAW)
        spine_start = 1
    elif source_kind == SourceKind.RAW:
        spine_start = 1  # start at RAW_TO_CURATED
    elif source_kind == SourceKind.CURATED:
        spine_start = 2  # start at CURATED_TO_GOLD
    else:  # pragma: no cover - defensive
        raise ConfigError(f"Unsupported source kind '{source_kind}'.")

    # --- how far along the spine to go ------------------------------------ #
    spine_end = {
        TargetLayer.RAW: 1,          # stop after reaching raw
        TargetLayer.TABLE_CLIENT: 1,  # a delivered table lands in raw
        TargetLayer.CURATED: 2,      # through RAW_TO_CURATED
        TargetLayer.CONSUMPTION: 3,  # through CURATED_TO_GOLD
        TargetLayer.VECTORS: 3,      # build everything then vectors
    }.get(target_layer)

    if spine_end is None:
        raise ConfigError(
            f"Source {source_kind} cannot target layer {target_layer}."
        )

    steps.extend(_SPINE[spine_start:spine_end])

    if with_vectors or target_layer == TargetLayer.VECTORS:
        steps.append(Step.VECTORS)

    if not steps:
        raise ConfigError(
            f"No steps result from source {source_kind} to {target_layer}; "
            "check the source and target settings."
        )
    return steps


def resolve_steps(config: dict) -> List[Step]:
    """Resolve the steps for a run from its config.

    If ``steps`` is an explicit list (anything other than ``"AUTO"``), it is used
    as given, so a user can run a single step. Otherwise the steps are derived
    from ``pipeline.source_kind`` and ``pipeline.target_layer`` (with optional
    ``pipeline.with_vectors`` and ``pipeline.deliver_files`` flags).

    Args:
        config: The resolved config.

    Returns:
        The ordered list of steps.

    Raises:
        ConfigError: If neither an explicit list nor the pipeline hints are
            present, or a value is invalid.
    """
    raw_steps = config.get("steps", "AUTO")
    if isinstance(raw_steps, (list, tuple)) and [str(s).upper() for s in raw_steps] != ["AUTO"]:
        try:
            return [Step.coerce(s) for s in raw_steps]
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc

    pipeline = config.get("pipeline") or {}
    source_kind = pipeline.get("source_kind")
    target_layer = pipeline.get("target_layer")
    if not source_kind or not target_layer:
        raise ConfigError(
            "To derive steps automatically, set pipeline.source_kind and "
            "pipeline.target_layer (or provide an explicit 'steps' list)."
        )
    try:
        return decide_steps(
            source_kind=SourceKind.coerce(source_kind),
            target_layer=TargetLayer.coerce(target_layer),
            with_vectors=bool(pipeline.get("with_vectors", False)),
            deliver_files=bool(pipeline.get("deliver_files", False)),
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
