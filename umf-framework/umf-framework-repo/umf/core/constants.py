"""Enumerations shared across the whole framework.

Using enumerations instead of free-form strings means a typo becomes an error
you can catch early, editors can auto-complete the valid options, and the set
of allowed values lives in exactly one place. Every enum below derives from
``str`` as well as ``Enum`` so that:

    * ``RunStatus.SUCCESS == "SUCCESS"`` is ``True`` (handy when a value arrives
      as plain text from a JSON config), and
    * the value serialises straight to its string form in JSON and logs.

Why this matters for production:
    The config a user writes is plain JSON text. When we read, for example,
    ``"load_type": "INCREMENTAL"`` we convert it into :class:`LoadType` early so
    the rest of the framework works with a validated, typed value and an unknown
    option fails immediately with a clear message rather than deep inside a job.
"""

from __future__ import annotations

from enum import Enum
from typing import List


class _StrEnum(str, Enum):
    """Base class for string-valued enumerations.

    Subclassing both ``str`` and ``Enum`` lets a member compare equal to its
    plain string value and serialise to that value without special handling.
    """

    def __str__(self) -> str:  # pragma: no cover - trivial
        """Return the plain string value (so ``str(RunStatus.SUCCESS)`` works)."""
        return str(self.value)

    @classmethod
    def values(cls) -> List[str]:
        """Return every allowed value as a list of plain strings.

        Returns:
            All member values, useful for building validation error messages
            such as ``"allowed values: " + ", ".join(LoadType.values())``.
        """
        return [member.value for member in cls]

    @classmethod
    def coerce(cls, value: object) -> "_StrEnum":
        """Convert a raw value (usually text from JSON) into an enum member.

        The comparison is case-insensitive and tolerant of surrounding spaces,
        because config values are hand-written and ``"incremental"`` should mean
        the same as ``"INCREMENTAL"``.

        Args:
            value: The raw value to convert. Usually a string from the config.

        Returns:
            The matching enum member.

        Raises:
            ValueError: If the value does not match any member. The caller is
                expected to turn this into a user-facing validation error.
        """
        if isinstance(value, cls):
            return value
        text = str(value).strip().upper()
        for member in cls:
            if member.value.upper() == text:
                return member
        raise ValueError(
            f"{cls.__name__}: '{value}' is not valid. "
            f"Allowed values: {', '.join(cls.values())}"
        )


class Environment(_StrEnum):
    """Which deployment environment a run belongs to.

    The environment selects which config, catalogs and secrets are used and can
    change behaviour (for example, a type-mapping problem stops the run in
    ``PROD`` but may continue with a warning in ``DEV``).
    """

    DEV = "DEV"
    TEST = "TEST"
    PROD = "PROD"


class Platform(_StrEnum):
    """The compute platform the framework is running on.

    ``AUTO`` is the value a user puts in the config to ask the framework to
    detect the platform itself. The remaining members are concrete platforms.
    ``UNKNOWN`` is what detection returns when it cannot identify the platform;
    the framework then relies on an explicit value in the config.
    """

    AUTO = "AUTO"
    DATABRICKS = "DATABRICKS"
    FABRIC = "FABRIC"
    SYNAPSE = "SYNAPSE"
    GLUE = "GLUE"
    EMR = "EMR"
    DATAPROC = "DATAPROC"
    ON_PREM = "ON_PREM"
    LOCAL = "LOCAL"
    UNKNOWN = "UNKNOWN"


class Step(_StrEnum):
    """The pipeline steps the framework can run.

    A run executes one or more of these. The framework usually derives the set
    of steps from the source and target, but a user can also request a single
    step explicitly.
    """

    DWH_TO_FILES = "DWH_TO_FILES"      # S1  extract a warehouse to files
    DWH_TO_TABLE = "DWH_TO_TABLE"      # S1b extract a warehouse straight to raw
    FILES_TO_RAW = "FILES_TO_RAW"      # S2  ingest files into raw
    RAW_TO_CURATED = "RAW_TO_CURATED"  # S3  build curated from raw
    CURATED_TO_GOLD = "CURATED_TO_GOLD"  # S4  build gold / deliver to client
    VOLUME_TABLE = "VOLUME_TABLE"      # S5  volume <-> table conversions
    VECTORS = "VECTORS"                # S6  build / refresh vectors
    DROP = "DROP"                      # client data removal (for example GDPR)
    ARCHIVE = "ARCHIVE"                # move retention overflow to archive
    MAINTENANCE = "MAINTENANCE"        # optimize / vacuum / expire snapshots


class LoadType(_StrEnum):
    """How much data a step moves and how changes are detected."""

    FULL = "FULL"                      # read everything every run
    INCREMENTAL = "INCREMENTAL"        # only rows past a watermark
    CDC = "CDC"                        # apply insert / update / delete events
    APPEND_ONLY = "APPEND_ONLY"        # facts that are only ever added
    SNAPSHOT_DIFF = "SNAPSHOT_DIFF"    # compare full snapshots to find deletes


class RunStatus(_StrEnum):
    """Overall status of a whole run.

    ``PARTIAL`` means some tables succeeded and others failed; the run is not a
    total failure but is not fully clean either. This matters because one
    table's failure must never stop unrelated tables.
    """

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    ABORTED = "ABORTED"                # stopped before processing (for example
    #                                    invalid config, or a lock held)


class StepStatus(_StrEnum):
    """Status of a single step for a single table."""

    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"                # nothing to do (for example no new data)
    NEEDS_DECISION = "NEEDS_DECISION"  # blocked until a human sets a config value


class ErrorCategory(_StrEnum):
    """A coarse category for every error, used for triage and alerting.

    Grouping errors lets dashboards and alerts answer "what kind of thing broke"
    at a glance, and lets the framework decide whether an error is worth a retry
    (a transient ``CONNECTION`` error) or not (a ``CONFIG`` error).
    """

    CONFIG = "CONFIG"                  # bad or missing configuration
    CONNECTION = "CONNECTION"          # could not reach a source or target
    AUTH = "AUTH"                      # credentials or permissions
    SCHEMA = "SCHEMA"                  # schema or type mapping problem
    DATA = "DATA"                      # bad data content
    DQ = "DQ"                          # a data-quality rule failed
    RECON = "RECON"                    # reconciliation did not pass
    PLATFORM = "PLATFORM"              # platform / runtime problem
    INTERNAL = "INTERNAL"              # a bug in the framework itself


class Severity(_StrEnum):
    """How serious a logged event is."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class TableFormat(_StrEnum):
    """The open table format used for a target table."""

    DELTA = "DELTA"
    ICEBERG = "ICEBERG"


class SchemaMode(_StrEnum):
    """How the framework reconciles an incoming schema with the target schema.

    See the design document for the full behaviour. In short:
        FIXED      the target schema is a contract; any mismatch fails.
        OVERWRITE  the target is made to mirror the source (dropping a column
                   needs a safety threshold and approval).
        EVOLVE     additive only: new columns are added, nothing is removed.
        STRICT     names, types and order must all match exactly.
    """

    FIXED = "FIXED"
    OVERWRITE = "OVERWRITE"
    EVOLVE = "EVOLVE"
    STRICT = "STRICT"


class CuratedMode(_StrEnum):
    """How the curated layer applies incoming data."""

    LATEST_SNAPSHOT = "LATEST_SNAPSHOT"      # replace with the newest full set
    UPSERT_LATEST = "UPSERT_LATEST"          # merge, keep newest row per key
    INCREMENTAL_APPEND = "INCREMENTAL_APPEND"  # add new rows, keep history
    CDC_APPLY = "CDC_APPLY"                  # apply insert / update / delete
    SCD2 = "SCD2"                            # keep history with effective dates


class KeyStrategy(_StrEnum):
    """How a business key is formed when a table has no reliable primary key.

    This is used for merges, de-duplication and change detection. ``COMPOSITE``
    and ``HASH`` are the answer to "the source has no primary key": the user
    names the columns that together identify a row, and the framework either
    uses them directly or hashes them into a single stable key.
    """

    NATURAL_PK = "NATURAL_PK"          # the source already has a primary key
    COMPOSITE = "COMPOSITE"            # several named columns together are unique
    HASH = "HASH"                      # hash of the named columns is the key
    ALL_COLUMNS_HASH = "ALL_COLUMNS_HASH"  # hash of every column (full-row dedup)
    NO_KEY = "NO_KEY"                  # append-only, no de-duplication


class OutputType(_StrEnum):
    """What a client delivery step produces."""

    FILES = "FILES"
    TABLE = "TABLE"
    VIEW = "VIEW"
    BOTH = "BOTH"


class ReconLevel(_StrEnum):
    """Depth of reconciliation performed after a step.

    L1 and L2 are always on. L3 (row hashing) is added only where both sides are
    Spark, because cross-engine hashes are unreliable without careful
    canonical formatting.
    """

    L1 = "L1"  # row count and schema
    L2 = "L2"  # L1 plus aggregates: sums, min/max, null counts, distinct keys
    L3 = "L3"  # L2 plus row-level hashing


class FieldRisk(_StrEnum):
    """How dangerous it is to guess a default for a config field.

    This drives validation. If a field is ``HIGH`` risk, the framework refuses
    to guess and the field becomes mandatory, because a wrong default could
    corrupt data, cause silent precision loss, or write to the wrong place. If a
    field is ``SAFE``, the framework may apply a documented default but will ask
    for one confirmation first.
    """

    HIGH = "HIGH"    # a wrong default could corrupt or lose data -> mandatory
    SAFE = "SAFE"    # a wrong default only changes behaviour, never harms data
    SILENT = "SILENT"  # safe and so minor it needs no confirmation at all
