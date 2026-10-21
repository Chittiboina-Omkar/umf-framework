"""The curated layer plan: choosing how incoming data is applied.

The curated layer can apply data in several ways, set by ``curated.mode``:

    LATEST_SNAPSHOT     replace the table with the newest full set    -> OVERWRITE
    UPSERT_LATEST       keep the newest row per key                    -> MERGE
    INCREMENTAL_APPEND  add new rows, keep history                     -> APPEND
    CDC_APPLY           apply inserts, updates and deletes             -> MERGE (+delete)
    SCD2                keep history with effective dates              -> MERGE (SCD2)

This module turns the configured mode (and the related settings: keys, ordering
column, delete handling) into a validated :class:`CuratedPlan` that the curated
step executes. The plan-building and validation are pure Python and tested here;
executing the plan uses the transform, key, DQ and staging pieces already built,
and the atomic publish from the table writer.

Why validate here as well as in the config schema:
    The config schema already marks the key and ordering fields mandatory for
    the modes that need them. Validating again at plan time keeps the curated
    logic correct even when it is driven directly (for example from a test or an
    internal call) rather than through the full config path, and it maps the
    mode to the right commit mode in one obvious place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from umf.adapters.table_format.base import CommitMode
from umf.core.constants import CuratedMode, KeyStrategy
from umf.core.exceptions import ConfigError
from umf.engine.keys import KeyPlan, plan_key
from umf.engine.transforms import TransformOp, build_transform_plan


# How each curated mode is published into the target.
_MODE_TO_COMMIT = {
    CuratedMode.LATEST_SNAPSHOT: CommitMode.OVERWRITE,
    CuratedMode.UPSERT_LATEST: CommitMode.MERGE,
    CuratedMode.INCREMENTAL_APPEND: CommitMode.APPEND,
    CuratedMode.CDC_APPLY: CommitMode.MERGE,
    CuratedMode.SCD2: CommitMode.MERGE,
}

# Modes that merge on a key and therefore require keys and an ordering column.
_KEY_MODES = {CuratedMode.UPSERT_LATEST, CuratedMode.CDC_APPLY, CuratedMode.SCD2}


@dataclass(frozen=True)
class CuratedPlan:
    """A validated plan for applying data to the curated layer.

    Attributes:
        mode: The curated mode.
        commit_mode: The commit mode the staging orchestrator will use.
        key_plan: The resolved key plan (empty for snapshot / append).
        ordering_column: The column selecting the newest row per key, for the
            key-based modes.
        delete_handling: For CDC, how deletes are applied (soft / hard / ignore).
        transforms: The ordered transform plan.
        requires_keys: Whether this mode merges on a key.
    """

    mode: CuratedMode
    commit_mode: CommitMode
    key_plan: Optional[KeyPlan]
    ordering_column: Optional[str]
    delete_handling: Optional[str]
    transforms: Sequence[TransformOp]
    requires_keys: bool


def build_curated_plan(
    *,
    mode: CuratedMode,
    all_columns: Sequence[str],
    key_strategy: Optional[KeyStrategy] = None,
    key_columns: Sequence[str] = (),
    ordering_column: Optional[str] = None,
    delete_handling: Optional[str] = None,
    transforms: Sequence[dict] = (),
) -> CuratedPlan:
    """Validate the curated settings and build a plan.

    Args:
        mode: The curated mode.
        all_columns: All columns of the data after transforms, used to resolve
            the key plan.
        key_strategy: The key strategy, required for the key-based modes.
        key_columns: The key columns (see the key strategy).
        ordering_column: The ordering column, required for the key-based modes.
        delete_handling: The delete handling for CDC (soft / hard / ignore).
        transforms: The raw transform list from the config.

    Returns:
        A validated :class:`CuratedPlan`.

    Raises:
        ConfigError: If a key-based mode is missing its key strategy or ordering
            column, or if delete handling is invalid for CDC.
    """
    commit_mode = _MODE_TO_COMMIT[mode]
    requires_keys = mode in _KEY_MODES
    transform_plan = build_transform_plan(transforms)

    key_plan: Optional[KeyPlan] = None
    if requires_keys:
        if key_strategy is None:
            raise ConfigError(
                f"Curated mode {mode} requires a key strategy (curated.key_strategy)."
            )
        if not ordering_column:
            raise ConfigError(
                f"Curated mode {mode} requires an ordering column "
                "(curated.ordering_column) to pick the newest row per key."
            )
        key_plan = plan_key(
            strategy=key_strategy,
            key_columns=key_columns,
            all_columns=all_columns,
        )

    if mode == CuratedMode.CDC_APPLY:
        allowed = {"soft", "hard", "ignore"}
        dh = (delete_handling or "soft").lower()
        if dh not in allowed:
            raise ConfigError(
                f"CDC delete_handling must be one of {sorted(allowed)}, got "
                f"'{delete_handling}'."
            )
        delete_handling = dh

    return CuratedPlan(
        mode=mode,
        commit_mode=commit_mode,
        key_plan=key_plan,
        ordering_column=ordering_column,
        delete_handling=delete_handling,
        transforms=transform_plan,
        requires_keys=requires_keys,
    )


def merge_keys(plan: CuratedPlan) -> List[str]:
    """Return the columns a merge joins on for a curated plan.

    Args:
        plan: The curated plan.

    Returns:
        The merge key columns, or an empty list for non-merge modes.
    """
    if plan.key_plan is None:
        return []
    return list(plan.key_plan.merge_keys)
