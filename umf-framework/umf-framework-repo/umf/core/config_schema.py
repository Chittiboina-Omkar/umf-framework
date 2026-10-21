"""Declarative description of every configuration field.

This module is the single source of truth for what a config may contain and how
each field behaves. Instead of scattering ``if "x" not in config: raise`` checks
through the code, every field is described once as a :class:`FieldSpec`, and the
validator in :mod:`umf.core.config_loader` interprets those specs.

The classification you asked for is encoded directly here through
:class:`~umf.core.constants.FieldRisk`:

    * ``HIGH`` risk fields are **mandatory**. The framework will not guess a
      default for them, because a wrong guess could corrupt data, lose data,
      produce a wrong result, or write to the wrong place (for example a merge
      key, the load type, delete handling, or the target table name). If a HIGH
      field is missing, validation fails before anything runs.
    * ``SAFE`` fields have a documented default whose worst case only changes
      behaviour, never harms data (for example a partition column, or a
      retention setting that keeps everything). If a SAFE field is empty, the
      framework will use the default but first surfaces one confirmation so the
      user is aware.
    * ``SILENT`` fields are safe and so minor that no confirmation is needed
      (for example a table description).

Each field also declares *when* it applies, so a field is only required in the
situations where it is actually used. For example a curated merge key is only
required when the run includes the curated step with an upsert-style mode. This
is how the framework asks for "only the parameters that are actually needed" for
a given run, rather than demanding everything every time.

This registry is intentionally extensible. Later parts of the framework add
their own fields by appending more :class:`FieldSpec` entries; the validator
does not need to change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Sequence

from umf.core.constants import (
    CuratedMode,
    FieldRisk,
    KeyStrategy,
    LoadType,
    OutputType,
    ReconLevel,
    SchemaMode,
    Step,
    TableFormat,
)

# A predicate decides whether a field applies to the current config. It is given
# the whole resolved-so-far config dictionary and returns True when the field is
# relevant. Keeping these as small functions makes the "only ask when needed"
# logic explicit and testable.
Applicability = Callable[[dict], bool]


def always(_config: dict) -> bool:
    """Applicability predicate: the field always applies.

    Args:
        _config: The config (unused).

    Returns:
        Always ``True``.
    """
    return True


def _steps(config: dict) -> List[str]:
    """Return the list of step names the config requests, as upper-case strings.

    Args:
        config: The config dictionary.

    Returns:
        The requested steps, or an empty list if none are specified yet. The
        special value ``"AUTO"`` is returned as-is so predicates can treat an
        auto-derived pipeline as "may include any step".
    """
    raw = config.get("steps", "AUTO")
    if isinstance(raw, str):
        return [raw.strip().upper()]
    if isinstance(raw, (list, tuple)):
        return [str(s).strip().upper() for s in raw]
    return []


def when_step(*step_values: Step) -> Applicability:
    """Build a predicate that is true when the config includes any given step.

    ``AUTO`` steps are treated as "could include anything", so fields guarded by
    a specific step are still asked for when the pipeline is auto-derived and
    might contain that step. This errs on the side of asking, which is the safe
    direction for HIGH-risk fields.

    Args:
        *step_values: The steps that make the field relevant.

    Returns:
        An applicability predicate.
    """
    wanted = {s.value for s in step_values}

    def predicate(config: dict) -> bool:
        steps = _steps(config)
        if "AUTO" in steps:
            return True
        return any(s in wanted for s in steps)

    return predicate


def when_curated_mode(*modes: CuratedMode) -> Applicability:
    """Build a predicate true when curated runs in one of the given modes.

    Used, for example, to require a merge key only for the modes that actually
    merge on a key.

    Args:
        *modes: The curated modes that make the field relevant.

    Returns:
        An applicability predicate.
    """
    wanted = {m.value for m in modes}

    def predicate(config: dict) -> bool:
        curated = config.get("curated") or {}
        mode = str(curated.get("mode", "")).strip().upper()
        return mode in wanted

    return predicate


def when_load_type(*types: LoadType) -> Applicability:
    """Build a predicate true when the load type is one of the given types.

    Args:
        *types: The load types that make the field relevant.

    Returns:
        An applicability predicate.
    """
    wanted = {t.value for t in types}

    def predicate(config: dict) -> bool:
        load = config.get("load") or {}
        value = str(load.get("type", "")).strip().upper()
        return value in wanted

    return predicate


def when_key_strategy(*strategies: KeyStrategy) -> Applicability:
    """Build a predicate true when the key strategy is one of the given values.

    Args:
        *strategies: The key strategies that make the field relevant.

    Returns:
        An applicability predicate.
    """
    wanted = {s.value for s in strategies}

    def predicate(config: dict) -> bool:
        curated = config.get("curated") or {}
        value = str(curated.get("key_strategy", "")).strip().upper()
        return value in wanted

    return predicate


@dataclass(frozen=True)
class FieldSpec:
    """Description of a single configuration field.

    Attributes:
        path: Dotted path to the field inside the config, for example
            ``"target.table"`` or ``"curated.mode"``. The validator uses this to
            read the value from the nested config dictionary.
        risk: The :class:`FieldRisk`. HIGH makes the field mandatory when it
            applies; SAFE and SILENT allow a default.
        description: A short, user-facing explanation of what the field controls
            and why it matters. Shown in validation messages and confirmations.
        applies: Predicate deciding whether the field is relevant to the current
            config. Defaults to :func:`always`.
        default: The value used when a SAFE or SILENT field is absent. Must be
            ``None`` for HIGH-risk fields (they are never defaulted).
        default_reason: A short explanation of what the default means in
            practice, shown in the confirmation prompt (for example
            "no partitioning" or "keep all runs").
        allowed: An optional enum type or explicit sequence of allowed values.
            When set, the validator checks the value against it. An enum type is
            checked case-insensitively via its ``coerce`` method.
        example: An optional example value, included in error messages to make a
            fix obvious.
    """

    path: str
    risk: FieldRisk
    description: str
    applies: Applicability = always
    default: Any = None
    default_reason: str = ""
    allowed: Optional[Any] = None
    example: Optional[Any] = None

    @property
    def is_mandatory(self) -> bool:
        """Whether this field must be supplied when it applies.

        Returns:
            True for HIGH-risk fields.
        """
        return self.risk == FieldRisk.HIGH


# The registry itself. Grouped by area for readability. This is the Part 1 set:
# the common fields plus the high-risk and representative safe fields we agreed
# on. Later parts extend this list; nothing else needs to change.
_SCHEMA: List[FieldSpec] = [
    # --- top level -------------------------------------------------------- #
    FieldSpec(
        path="environment",
        risk=FieldRisk.HIGH,
        description="Deployment environment; selects config, catalogs and "
        "secrets and can change safety behaviour.",
        allowed=None,  # validated as an Environment enum by the loader
        example="PROD",
    ),
    FieldSpec(
        path="client_id",
        risk=FieldRisk.HIGH,
        description="Identifier of the client / tenant this run belongs to; "
        "used for isolation and naming.",
        example="acme",
    ),
    FieldSpec(
        path="platform",
        risk=FieldRisk.SAFE,
        description="Compute platform. 'AUTO' asks the framework to detect it.",
        default="AUTO",
        default_reason="detect the platform automatically",
        example="DATABRICKS",
    ),
    FieldSpec(
        path="steps",
        risk=FieldRisk.SAFE,
        description="Which steps to run. 'AUTO' derives them from source and "
        "target.",
        default="AUTO",
        default_reason="derive the steps from the source and target",
        example=["DWH_TO_TABLE", "RAW_TO_CURATED"],
    ),

    # --- source ----------------------------------------------------------- #
    FieldSpec(
        path="source.category",
        risk=FieldRisk.HIGH,
        description="The kind of source (for example a cloud DWH, an RDBMS, or "
        "files). Selects the reader adapter.",
        example="RDBMS",
    ),

    # --- load ------------------------------------------------------------- #
    FieldSpec(
        path="load.type",
        risk=FieldRisk.HIGH,
        description="How much data to move and how changes are detected. A wrong "
        "guess could rewrite a whole table or miss changes.",
        allowed=LoadType,
        example="INCREMENTAL",
    ),
    FieldSpec(
        path="load.watermark_column",
        risk=FieldRisk.HIGH,
        description="The column used to detect new or changed rows. Required for "
        "incremental loads; a wrong column silently misses data.",
        applies=when_load_type(LoadType.INCREMENTAL),
        example="updated_at",
    ),
    FieldSpec(
        path="load.volume_class",
        risk=FieldRisk.SAFE,
        description="Expected data volume; enables chunking and other large-scale "
        "handling when set to a large class.",
        default="medium",
        default_reason="treat the table as medium sized",
        allowed=("small", "medium", "large", "very_large"),
        example="large",
    ),

    # --- raw -------------------------------------------------------------- #
    FieldSpec(
        path="raw.write_mode",
        risk=FieldRisk.SAFE,
        description="Whether raw appends every run or keeps only the last N runs.",
        default="append",
        default_reason="append every run and keep full history",
        allowed=("append", "last_n"),
        example="append",
    ),
    FieldSpec(
        path="raw.retention_runs",
        risk=FieldRisk.SAFE,
        description="How many successful runs to keep when write_mode is "
        "'last_n'. Older runs are archived, not deleted.",
        applies=lambda c: str((c.get("raw") or {}).get("write_mode", "")).lower()
        == "last_n",
        default=10,
        default_reason="keep the last 10 successful runs",
        example=10,
    ),

    # --- schema handling -------------------------------------------------- #
    FieldSpec(
        path="schema.mode",
        risk=FieldRisk.HIGH,
        description="How an incoming schema is reconciled with the target. "
        "OVERWRITE can drop target columns, so this is a decision, not a guess.",
        allowed=SchemaMode,
        example="EVOLVE",
    ),

    # --- curated ---------------------------------------------------------- #
    FieldSpec(
        path="curated.mode",
        risk=FieldRisk.HIGH,
        description="How curated applies incoming data (snapshot, upsert, CDC, "
        "SCD2, ...). Drives correctness of the whole curated table.",
        applies=when_step(Step.RAW_TO_CURATED),
        allowed=CuratedMode,
        example="UPSERT_LATEST",
    ),
    FieldSpec(
        path="curated.key_strategy",
        risk=FieldRisk.HIGH,
        description="How the business key is formed when there is no reliable "
        "primary key (composite columns or a hash of them).",
        applies=when_curated_mode(
            CuratedMode.UPSERT_LATEST, CuratedMode.CDC_APPLY, CuratedMode.SCD2
        ),
        allowed=KeyStrategy,
        example="COMPOSITE",
    ),
    FieldSpec(
        path="curated.key_columns",
        risk=FieldRisk.HIGH,
        description="The columns that together identify a row. A wrong set "
        "causes duplicates or wrong merges, so it must be supplied, not guessed.",
        applies=when_key_strategy(
            KeyStrategy.NATURAL_PK,
            KeyStrategy.COMPOSITE,
            KeyStrategy.HASH,
        ),
        example=["order_id", "line_no"],
    ),
    FieldSpec(
        path="curated.ordering_column",
        risk=FieldRisk.HIGH,
        description="The column that decides which row is newest per key. A wrong "
        "choice can keep a stale row as the current one.",
        applies=when_curated_mode(
            CuratedMode.UPSERT_LATEST, CuratedMode.CDC_APPLY, CuratedMode.SCD2
        ),
        example="updated_at",
    ),
    FieldSpec(
        path="curated.delete_handling",
        risk=FieldRisk.HIGH,
        description="What to do with deletes (soft, hard, or ignore). A wrong "
        "default can either miss deletes or remove live data.",
        applies=when_curated_mode(CuratedMode.CDC_APPLY),
        allowed=("soft", "hard", "ignore"),
        example="soft",
    ),

    # --- target ----------------------------------------------------------- #
    FieldSpec(
        path="target.table_format",
        risk=FieldRisk.HIGH,
        description="Open table format for the target table.",
        allowed=TableFormat,
        example="DELTA",
    ),
    FieldSpec(
        path="target.catalog",
        risk=FieldRisk.SAFE,
        description="Target catalog. Optional only on path-based platforms that "
        "have no catalog; required on catalog-based platforms (checked per "
        "platform in a later part).",
        default=None,
        default_reason="no catalog (path-based target)",
        example="main",
    ),
    FieldSpec(
        path="target.schema",
        risk=FieldRisk.HIGH,
        description="Target schema / namespace. Writing to the wrong schema can "
        "overwrite unrelated tables, so this is never guessed.",
        example="sales_raw",
    ),
    FieldSpec(
        path="target.table",
        risk=FieldRisk.HIGH,
        description="Target table name. Writing to the wrong table is "
        "destructive, so this is never guessed.",
        example="orders",
    ),
    FieldSpec(
        path="target.partition_by",
        risk=FieldRisk.SAFE,
        description="Columns to partition the target by.",
        default=[],
        default_reason="no partitioning",
        example=["ingestion_date"],
    ),

    # --- reconciliation --------------------------------------------------- #
    FieldSpec(
        path="recon.level",
        risk=FieldRisk.SAFE,
        description="Depth of reconciliation. L1 and L2 are always applied; this "
        "chooses whether row hashing (L3) is added.",
        default="L2",
        default_reason="counts plus aggregate checks (no row hashing)",
        allowed=ReconLevel,
        example="L2",
    ),
    FieldSpec(
        # Classified SAFE because the *default* (zero) is the safe value: an
        # exact match for decimals and integers. The danger is only a loose,
        # non-zero tolerance, which can only arise from an explicit user choice,
        # never from the default. Leaving it empty therefore cannot harm data.
        path="recon.decimal_tolerance",
        risk=FieldRisk.SAFE,
        description="Allowed difference when comparing decimal / integer sums. "
        "Zero means exact match; raising it can hide precision or money errors.",
        default=0,
        default_reason="exact match required for decimals and integers",
        example=0,
    ),

    # --- delivery --------------------------------------------------------- #
    FieldSpec(
        path="delivery.output_type",
        risk=FieldRisk.HIGH,
        description="What a client delivery produces (files, table, view, both).",
        applies=when_step(Step.CURATED_TO_GOLD),
        allowed=OutputType,
        example="TABLE",
    ),

    # --- operation -------------------------------------------------------- #
    FieldSpec(
        path="operation.run_mode",
        risk=FieldRisk.SAFE,
        description="How the run behaves (normal, backfill, reprocess, dry_run, "
        "validate).",
        default="normal",
        default_reason="a normal run",
        allowed=("normal", "backfill", "reprocess", "dry_run", "validate"),
        example="normal",
    ),
]


def schema() -> Sequence[FieldSpec]:
    """Return the full field registry.

    Returns:
        The sequence of :class:`FieldSpec` entries describing every known field.
        The caller must treat it as read-only.
    """
    return tuple(_SCHEMA)
