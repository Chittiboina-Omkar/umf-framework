"""Declarative definitions of every control and log table.

This module is the single source of truth for the shape of the framework's
operational tables: the logs that record what happened, and the control tables
that hold state (watermarks, config versions). Defining them here, once, as data
means:

    * an engine adapter can generate the correct Delta or Iceberg DDL from the
      same definition, on any platform;
    * the record builders in :mod:`umf.control.records` can be checked against
      the table they target, so a record can never drift out of sync with its
      table; and
    * documentation of the operational schema is always accurate, because it is
      the code.

Two groups of tables are defined:

    Control tables  hold durable state the framework reads and updates:
        * ``config_versions``  every saved version of a config, with audit info
        * ``watermark``        the current watermark per table
    (more control tables, such as a source registry and approvals, are added in
    later parts using the same pattern.)

    Log tables  are append-only records of what happened. They are never
    updated in place; a new row is written for each event. This is what makes
    the audit trail trustworthy and crash-safe: a partially written run still
    leaves a truthful set of rows.

Why counts and metadata only, never data values:
    Log tables live in the client environment but may be read by operators and
    shipped to dashboards. They therefore record counts, identifiers, column
    names, types and timings, but never customer data values. The one place that
    references actual bad rows, ``reject_records``, stores only a pointer to the
    separately held, access-restricted reject data, not the row contents.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from umf.control.column_types import ColumnType


@dataclass(frozen=True)
class ColumnDef:
    """Definition of one column in a control or log table.

    Attributes:
        name: The column name.
        type: The engine-neutral :class:`ColumnType`.
        nullable: Whether the column may be null. Identifiers and timestamps are
            typically not nullable; measured values often are, because they may
            be unknown.
        description: A short explanation of what the column holds. This is used
            to generate documentation and column comments where the engine
            supports them.
        precision: Decimal precision, only meaningful when ``type`` is
            ``DECIMAL``.
        scale: Decimal scale, only meaningful when ``type`` is ``DECIMAL``.
    """

    name: str
    type: ColumnType
    nullable: bool = True
    description: str = ""
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass(frozen=True)
class TableDef:
    """Definition of a control or log table.

    Attributes:
        name: The logical table name (the physical name is prefixed per client
            and environment by the engine adapter in a later part).
        kind: Either ``"log"`` (append-only) or ``"control"`` (stateful).
        description: What the table is for.
        columns: The ordered list of column definitions.
        partition_by: Columns to partition by, chosen so common queries (by run
            or by date) prune well. Empty means no partitioning.
        identity: The columns that together identify a row. For log tables this
            is informational (used for de-duplication on replay); for control
            tables it is the key used to read and update state.
    """

    name: str
    kind: str
    description: str
    columns: Sequence[ColumnDef]
    partition_by: Tuple[str, ...] = ()
    identity: Tuple[str, ...] = ()

    def column_names(self) -> List[str]:
        """Return the column names in definition order.

        Returns:
            The list of column names, used to validate records against the
            table.
        """
        return [c.name for c in self.columns]


# A few column shapes recur in almost every table, so we build them with small
# helpers to keep the definitions short and consistent.


def _ts(name: str, desc: str, nullable: bool = False) -> ColumnDef:
    """Build a UTC timestamp column."""
    return ColumnDef(name, ColumnType.TIMESTAMP, nullable=nullable, description=desc)


def _str(name: str, desc: str, nullable: bool = True) -> ColumnDef:
    """Build a string column."""
    return ColumnDef(name, ColumnType.STRING, nullable=nullable, description=desc)


def _long(name: str, desc: str, nullable: bool = True) -> ColumnDef:
    """Build a 64-bit integer column (counts, durations)."""
    return ColumnDef(name, ColumnType.LONG, nullable=nullable, description=desc)


def _bool(name: str, desc: str, nullable: bool = True) -> ColumnDef:
    """Build a boolean column."""
    return ColumnDef(name, ColumnType.BOOLEAN, nullable=nullable, description=desc)


def _json(name: str, desc: str) -> ColumnDef:
    """Build a JSON (serialised) column."""
    return ColumnDef(name, ColumnType.JSON, nullable=True, description=desc)


# --- control tables -------------------------------------------------------- #

CONFIG_VERSIONS = TableDef(
    name="config_versions",
    kind="control",
    description="Every saved version of a config, with audit information. This "
    "replaces a Git pull-request history because the config lives at the client. "
    "Rows are never updated or deleted, so the full history and any rollback are "
    "always reconstructable.",
    columns=[
        _long("version", "Monotonic version number, starting at 1.", nullable=False),
        _str("client_id", "Client the config belongs to.", nullable=False),
        _str("environment", "Environment the config applies to.", nullable=False),
        _str("config_hash", "SHA-256 of the canonical config JSON.", nullable=False),
        _json("config_json", "The full config at this version (secrets are "
              "references only, never raw values)."),
        _str("author", "Who saved this version.", nullable=False),
        _str("note", "Optional change note."),
        _ts("created_at", "When this version was saved."),
    ],
    identity=("client_id", "environment", "version"),
)

WATERMARK = TableDef(
    name="watermark",
    kind="control",
    description="The current watermark for each table, used by incremental and "
    "CDC loads. The watermark is only advanced after a step succeeds and "
    "reconciliation passes, so a failed run never moves it forward.",
    columns=[
        _str("table_key", "Stable key identifying the table within the client.",
             nullable=False),
        _str("watermark_value", "The current watermark value, stored as text so "
             "any column type (timestamp, id) is supported.", nullable=False),
        _str("watermark_column", "The source column the watermark tracks."),
        _str("last_run_id", "The run that last advanced this watermark."),
        _ts("updated_at", "When the watermark was last advanced."),
    ],
    identity=("table_key",),
)

# --- log tables ------------------------------------------------------------ #

RUN_LOG = TableDef(
    name="run_log",
    kind="log",
    description="One row per run: identity, status and timing. Written at start "
    "(status RUNNING) and again at finish (final status), so a crashed run is "
    "still visible as RUNNING and can be reconciled.",
    columns=[
        _str("run_id", "Unique run identifier.", nullable=False),
        _str("client_id", "Client the run belongs to."),
        _str("environment", "Environment the run executed in."),
        _str("platform", "Resolved compute platform."),
        _str("framework_version", "Framework build that produced the run."),
        _long("config_version", "Config version used (links to config_versions)."),
        _str("status", "Overall run status."),
        _str("run_mode", "normal / backfill / reprocess / dry_run / validate."),
        _str("trigger", "What started the run (schedule, manual, api)."),
        _ts("started_at", "When the run started."),
        _ts("ended_at", "When the run finished.", nullable=True),
        ColumnDef("duration_seconds", ColumnType.DOUBLE, True,
                  "Wall-clock duration in seconds."),
        _long("tables_total", "Distinct tables touched."),
        _long("outcomes_total", "Total step outcomes recorded."),
        _str("error_summary", "Short, non-sensitive summary if the run failed."),
    ],
    partition_by=(),
    identity=("run_id",),
)

STEP_LOG = TableDef(
    name="step_log",
    kind="log",
    description="One row per (step, table) execution, recording status and "
    "timing. Separate STARTED and finished rows are written so an in-flight or "
    "crashed step is visible.",
    columns=[
        _str("run_id", "Run this step belongs to.", nullable=False),
        _str("step", "Step name.", nullable=False),
        _str("table_name", "Logical table processed."),
        _str("status", "Step status."),
        _ts("started_at", "When the step started."),
        _ts("ended_at", "When the step finished.", nullable=True),
        ColumnDef("duration_seconds", ColumnType.DOUBLE, True, "Duration in seconds."),
        _str("detail", "Short, non-sensitive note (for example skip reason)."),
    ],
    identity=("run_id", "step", "table_name", "started_at"),
)

TABLE_LOG = TableDef(
    name="table_log",
    kind="log",
    description="Row-count accounting per table per step, so every row can be "
    "accounted for: read = written + rejected + dropped.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("step", "Step name.", nullable=False),
        _str("table_name", "Logical table.", nullable=False),
        _long("rows_read", "Rows read from the source."),
        _long("rows_written", "Rows written to the target."),
        _long("rows_rejected", "Rows sent to the reject table."),
        _long("rows_inserted", "Rows inserted (merge)."),
        _long("rows_updated", "Rows updated (merge)."),
        _long("rows_deleted", "Rows deleted (hard delete)."),
        _long("bytes_written", "Bytes written, if known."),
        _ts("ts", "When this accounting was recorded."),
    ],
    identity=("run_id", "step", "table_name"),
)

FILE_LOG = TableDef(
    name="file_log",
    kind="log",
    description="One row per source file processed, with its identity, size, "
    "checksum, row count and status. This is how duplicate or re-sent files are "
    "detected and skipped (idempotency).",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("file_path", "Full path of the file.", nullable=False),
        _long("file_size", "File size in bytes."),
        _ts("modified_time", "Source modification time.", nullable=True),
        _str("checksum", "Checksum used for duplicate detection."),
        _long("rows", "Rows read from the file."),
        _str("status", "processed / skipped / quarantined / failed."),
        _str("error", "Short error description if the file failed."),
        _ts("ts", "When the file was processed."),
    ],
    identity=("run_id", "file_path"),
)

EXTRACT_LOG = TableDef(
    name="extract_log",
    kind="log",
    description="One row per extraction chunk from a warehouse or database, with "
    "the watermark before and after and the row count, so a large resumable "
    "extract can be audited chunk by chunk.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Source table extracted.", nullable=False),
        _str("chunk_id", "Identifier of the chunk within the extract."),
        _str("query_hash", "Hash of the extraction query (not the data)."),
        _str("watermark_before", "Watermark at the start of the chunk."),
        _str("watermark_after", "Watermark at the end of the chunk."),
        _long("row_count", "Rows extracted in this chunk."),
        _ts("started_at", "When the chunk started."),
        _ts("ended_at", "When the chunk finished.", nullable=True),
        _str("status", "Chunk status."),
    ],
    identity=("run_id", "table_name", "chunk_id"),
)

SCHEMA_LOG = TableDef(
    name="schema_log",
    kind="log",
    description="Schema versions and drift events per table, including where the "
    "schema came from (source metadata, sidecar, contract, inferred).",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _long("schema_version", "Monotonic schema version for the table."),
        _str("schema_origin", "SOURCE_METADATA / SIDECAR / CONTRACT / INFERRED."),
        _str("change_type", "added / dropped / type_changed / renamed / none."),
        _str("column_name", "Column involved in the change, if any."),
        _str("detail", "Short description of the change."),
        _ts("ts", "When recorded."),
    ],
    identity=("run_id", "table_name", "schema_version"),
)

TYPE_MAPPING_LOG = TableDef(
    name="type_mapping_log",
    kind="log",
    description="One row per column mapping, recording the source type and the "
    "target type with precision, scale and length, whether the mapping was exact "
    "or used a fallback, and whether there is a data-loss risk. This is the "
    "evidence behind 'exact first, never silent'.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _str("column_name", "Column.", nullable=False),
        _str("source_type", "Native source type."),
        _str("target_type", "Resolved target type."),
        _long("precision", "Numeric precision, if applicable."),
        _long("scale", "Numeric scale, if applicable."),
        _long("length", "String length, if applicable."),
        _bool("exact", "True if the mapping is exact (no loss)."),
        _bool("fallback_used", "True if an unmappable-type fallback was applied."),
        _bool("data_loss_risk", "True if the mapping risks losing information."),
        _str("decision", "The configured data_type_handling decision, if any."),
        _ts("ts", "When recorded."),
    ],
    identity=("run_id", "table_name", "column_name"),
)

DQ_RESULT_LOG = TableDef(
    name="dq_result_log",
    kind="log",
    description="One row per data-quality rule evaluation, with the outcome and "
    "the action taken.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _str("rule_name", "Rule identifier.", nullable=False),
        _str("rule_type", "not_null / unique / range / regex / referential / ..."),
        _bool("passed", "Whether the rule passed."),
        _long("failed_count", "Number of rows that failed the rule."),
        _str("action", "fail / quarantine / warn / flag."),
        _str("detail", "Short, non-sensitive detail."),
        _ts("ts", "When evaluated."),
    ],
    identity=("run_id", "table_name", "rule_name"),
)

REJECT_RECORDS = TableDef(
    name="reject_records",
    kind="log",
    description="Metadata about rejected rows. The actual row contents are stored "
    "separately in an access-restricted reject data table (customer data), and "
    "referenced here by a pointer only, so this log stays free of data values.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _str("reject_reason", "Why the row was rejected."),
        _str("rule_name", "Rule that rejected the row, if any."),
        _str("row_ref", "Pointer to the stored reject row (not the data)."),
        _ts("ts", "When rejected."),
    ],
    identity=("run_id", "table_name", "row_ref"),
)

RECON_RESULTS = TableDef(
    name="recon_results",
    kind="log",
    description="One row per reconciliation check, comparing a source value to a "
    "target value with a tolerance and a pass/fail status. This is the gate that "
    "proves no bad or partial data reached the target.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("step", "Step the check belongs to.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _str("check_name", "count / sum:<col> / null_count:<col> / max_length ..."),
        _str("level", "Recon level (L1 / L2 / L3)."),
        _str("source_value", "Value measured on the source side."),
        _str("target_value", "Value measured on the target side."),
        _str("difference", "Difference between the two, as text."),
        _str("tolerance", "Allowed tolerance for this check."),
        _str("status", "pass / fail."),
        _ts("ts", "When checked."),
    ],
    identity=("run_id", "step", "table_name", "check_name"),
)

ERROR_LOG = TableDef(
    name="error_log",
    kind="log",
    description="Every error, categorised for triage and alerting. Messages and "
    "context are non-sensitive; data values are never recorded here.",
    columns=[
        _str("run_id", "Run identifier."),
        _str("step", "Step where the error occurred, if any."),
        _str("table_name", "Table involved, if any."),
        _str("file_path", "File involved, if any."),
        _str("error_type", "Exception class name."),
        _str("category", "Error category (CONFIG / CONNECTION / ...)."),
        _str("severity", "INFO / WARNING / ERROR / CRITICAL."),
        _str("message", "Short, non-sensitive description."),
        _bool("retryable", "Whether a retry could help."),
        _long("attempt_no", "Retry attempt number, if applicable."),
        _json("context", "Non-sensitive structured context."),
        _ts("ts", "When the error occurred."),
    ],
    identity=("run_id", "ts"),
)

RETENTION_ARCHIVE_LOG = TableDef(
    name="retention_archive_log",
    kind="log",
    description="Records which runs were archived or deleted by the retention "
    "policy, and where archived data was moved.",
    columns=[
        _str("run_id", "Run performing the retention action.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _str("action", "archive / delete."),
        _str("run_kept_or_removed", "The run or batch affected."),
        _str("location", "Archive destination, if archived."),
        _ts("ts", "When performed."),
    ],
    identity=("run_id", "table_name", "ts"),
)

DELIVERY_LOG = TableDef(
    name="delivery_log",
    kind="log",
    description="Records each delivery to a client: what was delivered, where, "
    "how many rows, and the outcome.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Table delivered.", nullable=False),
        _str("output_type", "FILES / TABLE / VIEW / BOTH."),
        _str("destination", "Where it was delivered."),
        _long("rows", "Rows delivered."),
        _str("status", "success / failed."),
        _ts("ts", "When delivered."),
    ],
    identity=("run_id", "table_name", "ts"),
)

MAINTENANCE_LOG = TableDef(
    name="maintenance_log",
    kind="log",
    description="Records maintenance operations such as optimize, vacuum and "
    "snapshot expiry.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("table_name", "Table.", nullable=False),
        _str("operation", "optimize / vacuum / expire_snapshots / compact."),
        _str("detail", "Short description or result summary."),
        _ts("ts", "When performed."),
    ],
    identity=("run_id", "table_name", "ts"),
)

VECTOR_LOG = TableDef(
    name="vector_log",
    kind="log",
    description="Records vector operations: how many chunks and vectors were "
    "written or deleted, the store and the embedding model used.",
    columns=[
        _str("run_id", "Run identifier.", nullable=False),
        _str("source_ref", "What was embedded (table or volume reference)."),
        _long("chunks", "Chunks produced."),
        _long("vectors_written", "Vectors written to the store."),
        _long("vectors_deleted", "Vectors removed (delete-sync)."),
        _str("store", "Vector store name."),
        _str("model", "Embedding model identifier."),
        _ts("ts", "When performed."),
    ],
    identity=("run_id", "source_ref", "ts"),
)

CONFIG_AUDIT_LOG = TableDef(
    name="config_audit_log",
    kind="log",
    description="Audit trail of config changes: who changed what and when. "
    "Complements config_versions with the action semantics (save, rollback).",
    columns=[
        _long("config_version", "Version affected.", nullable=False),
        _str("action", "save / rollback / activate."),
        _str("author", "Who performed the action."),
        _str("note", "Optional note."),
        _str("config_hash", "Hash of the config at this version."),
        _ts("ts", "When performed."),
    ],
    identity=("config_version", "ts"),
)

LOCK_LOG = TableDef(
    name="lock_log",
    kind="log",
    description="Records lock acquisition and release, used to prevent two runs "
    "touching the same table at once.",
    columns=[
        _str("lock_key", "The resource being locked (for example a table).",
             nullable=False),
        _str("run_id", "Run holding or releasing the lock.", nullable=False),
        _str("action", "acquire / release / expired."),
        _ts("ts", "When performed."),
    ],
    identity=("lock_key", "run_id", "ts"),
)

ALERT_LOG = TableDef(
    name="alert_log",
    kind="log",
    description="Records alerts sent (for example by email), their severity and "
    "delivery status.",
    columns=[
        _str("run_id", "Run identifier, if any."),
        _str("channel", "Alert channel (for example outlook)."),
        _str("severity", "Severity of the alert."),
        _str("subject", "Short subject line."),
        _str("status", "sent / failed."),
        _ts("ts", "When sent."),
    ],
    identity=("run_id", "ts"),
)


# A registry of every table, so other modules can iterate (for example to create
# all tables on first run) without hard-coding the list.
ALL_TABLES: Dict[str, TableDef] = {
    t.name: t
    for t in (
        # control
        CONFIG_VERSIONS,
        WATERMARK,
        # log
        RUN_LOG,
        STEP_LOG,
        TABLE_LOG,
        FILE_LOG,
        EXTRACT_LOG,
        SCHEMA_LOG,
        TYPE_MAPPING_LOG,
        DQ_RESULT_LOG,
        REJECT_RECORDS,
        RECON_RESULTS,
        ERROR_LOG,
        RETENTION_ARCHIVE_LOG,
        DELIVERY_LOG,
        MAINTENANCE_LOG,
        VECTOR_LOG,
        CONFIG_AUDIT_LOG,
        LOCK_LOG,
        ALERT_LOG,
    )
}


def get_table(name: str) -> TableDef:
    """Return a table definition by name.

    Args:
        name: The logical table name.

    Returns:
        The :class:`TableDef`.

    Raises:
        KeyError: If no table with that name is defined.
    """
    return ALL_TABLES[name]


def log_tables() -> List[TableDef]:
    """Return all append-only log tables.

    Returns:
        The list of tables whose ``kind`` is ``"log"``.
    """
    return [t for t in ALL_TABLES.values() if t.kind == "log"]


def control_tables() -> List[TableDef]:
    """Return all stateful control tables.

    Returns:
        The list of tables whose ``kind`` is ``"control"``.
    """
    return [t for t in ALL_TABLES.values() if t.kind == "control"]
