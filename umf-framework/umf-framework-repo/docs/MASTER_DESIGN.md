# Multi-Cloud Migration & Data Framework — Master Design Document

**Status:** Design (NO code yet — code starts only after your written "confirm")
**Purpose:** One place that captures every decision, default, and open question from our discussion, plus every real-time scenario we identified (91+ points), each resolved with a concrete design choice.
**How to use:** Read top to bottom. Wherever you see **[DEFAULT]** it is my recommended behaviour you can change. Wherever you see **[OPEN]** I still need your decision. Section 25 is the master coverage checklist.

---

## 0. One honest framing (read this first)

Three things you asked for, and what is actually possible:

1. **"One framework for all migrations, parameter-driven."** → 100% possible as an architecture.
2. **"Any unexpected situation must be resolved automatically."** → No framework can auto-*resolve* the unknown. What we guarantee instead: the unknown is **detected, stopped safely (never silent corruption), highlighted, logged, and alerted**, with one place to fix it. This is what makes it trustworthy in production.
3. **"Code must never be downloadable / stolen."** → Code runs in the client environment, so it cannot be hidden 100%. We make theft hard (compiled/obfuscated wheel + license + contract). Absolute protection would require code to run only in *our* environment, which conflicts with "client data never leaves client" — and you chose data-safety first.

**The real design principle that makes "cover the whole world" achievable is not pre-writing everything — it is: strong core + safe-fail + extensibility (add an adapter, don't change the core).**

---

## 1. Confirmed decisions (from our conversation)

| # | Decision | Value |
|---|----------|-------|
| 1 | Engine | PySpark |
| 2 | Config format | JSON |
| 3 | Layers | source → raw → curated → consumption (gold + delivery) |
| 4 | Raw = exact mirror of source, dynamic DDL, only audit columns added | yes |
| 5 | Curated mode is a parameter (latest / upsert / incremental / CDC / SCD2) | yes |
| 6 | Raw retention: append by default; keep last N **successful** runs if set; auto-archive extras | yes |
| 7 | DWH → Files / Table / Both, by client requirement | yes |
| 8 | Reconciliation after every step, mandatory, cannot be turned off | yes |
| 9 | Full logging: every file, table, error, rejected row; append-only; survives crash | yes |
| 10 | Target catalog/schema/table/location/format always parameterized | yes |
| 11 | Multi-cloud AND non-cloud/on-prem | yes |
| 12 | UI = notebook UI, runs inside the platform, auto-detects platform, uses user/job identity | yes |
| 13 | Code protection first? No — **data safety first, code ~90% via obfuscation+license** | yes |
| 14 | Unexpected = safe-handle + highlight + one place to fix (not auto-resolve) | yes |
| 15 | **Config lives at the client** (not in our repo); state/logs/data at client | yes |
| 16 | Client gets only the compiled wheel (not source), via private package feed | yes |
| 17 | No telemetry / no outbound calls | yes |

---

## 2. Core principles

1. **Parameter-driven:** one codebase; behaviour changes only via config.
2. **Exact-first:** always map types/length/precision/scale exactly; deviate only when physically impossible (3 cases, see §10).
3. **Safe-fail:** on anything unknown or risky — stop that unit, highlight, log, alert. Never silently change data.
4. **Extensibility:** new source/cloud/format/target/vector/auth = new adapter honouring a contract; core untouched.
5. **Data stays at client:** no data, no config, no telemetry leaves the client environment.
6. **Isolation:** per-client config; one table/step failure does not stop unrelated units.
7. **Every run reproducible:** the exact resolved config (version/hash) is logged with each run.

---

## 3. Architecture overview

```
          ┌──────────────────────────────────────────────┐
          │  Notebook UI (optional, convenience only)      │
          │  - platform auto-detect                        │
          │  - dropdowns for cloud/catalog/schema/table    │
          │  - writes JSON config to client control store  │
          │  - "Run now" or "Submit as job"                │
          │  - shows logs / recon / dependency graph       │
          └───────────────┬────────────────────────────────┘
                          │ (config JSON, at client)
                          ▼
   ┌───────────────────────────────────────────────────────┐
   │  CORE ENGINE  (compiled/obfuscated wheel, our IP)       │
   │  config validation · run/step manager · dependency DAG  │
   │  recon · logging · error handling · locking · restart   │
   └───────────────┬───────────────────────────────────────┘
                   │ uses
                   ▼
   ┌───────────── ADAPTERS (plug-in, contract-based) ───────┐
   │ storage · catalog · DWH connector · type-map · table    │
   │ format (Delta/Iceberg) · vector store · secrets · alert │
   │ auth (IAM/SP/MI/Kerberos/key) · scheduler-submit        │
   └─────────────────────────────────────────────────────────┘
                   │ reads/writes (all at client)
                   ▼
   ┌─────────────────────────────────────────────────────────┐
   │ Client environment: source systems, raw/curated/gold,    │
   │ control + log tables, config store, secret store          │
   └─────────────────────────────────────────────────────────┘
```

Our side: private source repo + private package feed (versioned wheel). Client side: everything else.

---

## 4. The 9 layers

| Layer | Name | Responsibility |
|-------|------|----------------|
| L0 | Connectivity | connect (JDBC/ODBC/API/file/stream), auth (IAM, service principal, managed identity, Kerberos, key, SSL) |
| L1 | Source reader | read data + metadata; one adapter per source category |
| L2 | Type & schema | source types → canonical types → target types; precision/scale/length; drift; unmappable policy |
| L3 | Extract / ingest | chunking, parallelism, checkpoint, file/manifest, large-scale handling |
| L4 | Raw | exact mirror + audit columns; retention/archive |
| L5 | Curated | modes (snapshot/upsert/incremental/CDC/SCD2); DQ; transforms; delete handling; reference data; conflict resolution |
| L6 | Consumption | gold (SQL + structured logic) and client delivery (files/table/view) |
| L7 | Reconciliation | schema + data recon after every step; scale handling; sign-off |
| L8 | Vectors | chunk, embed, vector store, delete-sync |
| L9 | Control / log / UI | config, state (watermarks), logs, dependency graph, approvals, UI |

**Static rule:** once a step is chosen, the layers it uses are fixed (see §8). The user never chooses layers.

---

## 5. Steps

| Step | Meaning |
|------|---------|
| S1  | DWH → Files (Parquet/CSV + schema sidecar + manifest) |
| S1b | DWH → Raw table |
| S2  | Files → Raw |
| S3  | Raw → Curated |
| S4  | Curated → Gold |
| S5  | Volume ↔ Table |
| S6  | → Vectors |
| D   | Drop (client data removal, GDPR) |
| A   | Archive (retention overflow) |
| M   | Maintenance (optimize / vacuum / snapshot-expiry) |
| R   | Reconciliation (automatic, every step) |
| L   | Logging (automatic, every step) |

---

## 6. Source → Target → Steps decision rules

User gives **source**, **target (how far)**, **load type**. Framework derives steps.

| Source | Target (how far) | Steps auto-selected |
|--------|------------------|---------------------|
| DWH | files to client | S1 |
| DWH | raw table | S1b |
| DWH | curated | S1b → S3 |
| DWH | gold/consumption | S1b → S3 → S4 |
| DWH | both files + table | S1 + S1b |
| Files | raw | S2 |
| Files | curated | S2 → S3 |
| Files | gold | S2 → S3 → S4 |
| Any curated/gold | vectors | … → S6 |
| Table ↔ volume | — | S5 |

R and L always run. Manual override allowed (run a single step). **[DEFAULT]** first run of any new table = initial full load; subsequent runs = incremental.

---

## 7. Step → Layers mapping (fixed)

| Step | Layers used |
|------|-------------|
| S1  | L0 → L1 → L2 → L3 |
| S1b | L0 → L1 → L2 → L3 → L4 |
| S2  | L0 → L1 → L2 → L3 → L4 |
| S3  | L5 |
| S4  | L6 |
| S5  | L3/L4 (direction-dependent) |
| S6  | L8 |
| any | L7 (recon), L9 (control/log) always |

---

## 8. Source categories (all databases of the world)

We do not pre-write every system; we classify and provide an adapter per category + a generic JDBC/ODBC fallback + safe-fail for unknown types.

| Category | Examples | Entry | Handling |
|----------|----------|-------|----------|
| Cloud DWH | Snowflake, BigQuery, Redshift, Synapse, Fabric WH, Databricks SQL, Firebolt, Yellowbrick, Exasol | L0→L1→L2→L3 | catalog metadata + chunked extract |
| MPP / Enterprise | Teradata, Netezza, Vertica, Greenplum, Exadata, SAP HANA/BW, Sybase, Informix, Actian | L0→L1→L2→L3 | partition-based parallel extract |
| RDBMS (OLTP) | Oracle, SQL Server, PostgreSQL, MySQL/MariaDB, Db2 (LUW/zOS/iSeries), Aurora, AlloyDB, Cockroach, TiDB, Yugabyte, SingleStore | L0→L1→L2→L3 | watermark/CDC incremental |
| Analytics/OLAP | ClickHouse, StarRocks, Doris, Druid, Pinot, Kinetica, MonetDB | L0→L1→L2→L3 | category adapter |
| SQL engines / lakehouse | Hive, Trino, Presto, Impala, Dremio, Athena, Delta, Iceberg, Hudi | L0→L1→L3 | native metadata |
| NoSQL / document | MongoDB, Cassandra, DynamoDB, Cosmos DB, HBase | L1 (schema-infer)→L2 | infer + flatten; schema drift expected |
| Files | CSV, TSV, fixed-width, JSON, JSONL, Parquet, ORC, Avro, Excel, XML, logs | L1→L2 | format reader + sidecar/contract |
| Object storage | S3, ADLS Gen2, OneLake, GCS, Blob, MinIO, Ceph, OCI, OSS, R2, B2, Wasabi, Spaces | L0 | storage adapter |
| File transfer | SFTP, FTP(S), HTTPS, SCP, SMB/CIFS, NFS, HDFS, AWS Transfer | L0 | protocol adapter |
| Collaboration drives | SharePoint, OneDrive, Google Drive, Box, Dropbox | L0 | optional adapters |
| SaaS / API | Salesforce, SAP, generic REST | L0→L1 | **[OPEN]** scope — paging/API adapter if in scope |
| Streaming | Kafka, Event Hubs, Kinesis | special | **[OPEN]** future; micro-batch if in scope |
| Regional cloud | Alibaba MaxCompute/AnalyticDB/Hologres, Huawei GaussDB, Tencent | L0→L1→L2→L3 | category adapter |

---

## 9. Type & schema handling (exact-first)

### 9.1 Rule
Always read source metadata (precision, scale, length, nullable, PK/FK/default, comments) and map **exactly**. Deviation only in 3 cases:
1. Target lacks the type (e.g., Iceberg has no tinyint/smallint → int).
2. Precision exceeds target limit (Spark/Delta decimal max p=38; Oracle unbounded NUMBER, BigQuery BIGNUMERIC up to 76).
3. Source carries no type info (CSV/JSON/Excel/XML).

### 9.2 Common target mapping (condensed)
boolean→boolean; tinyint→TINYINT(Delta)/int(Iceberg); smallint/int/bigint→same; float/double→same; decimal(p,s) p≤38→exact; char/varchar(n)→STRING (+length as metadata/DQ); text/clob→STRING; binary/blob→BINARY; date→DATE; timestamp (no tz)→TIMESTAMP_NTZ(Delta, needs table feature)/timestamp(Iceberg); timestamp tz→TIMESTAMP(UTC)/timestamptz; time→STRING; interval→STRING; json/xml/variant→STRING (or VARIANT where supported); geo→WKT string or WKB binary; uuid→STRING; array/struct/map→same.

### 9.3 Per-source special cases (condensed — full matrices in build phase)
- **Oracle:** NUMBER(no precision)→policy; NUMBER negative scale; DATE carries time→timestamp; TIMESTAMP(9)→truncate to µs + warn; INTERVAL/ROWID/XMLTYPE/LONG→string.
- **SQL Server/Synapse:** money→decimal(19,4); smallmoney→decimal(10,4); datetime (3.33ms); datetime2(7)→µs+warn; datetimeoffset; time→string; uniqueidentifier→string; sql_variant/hierarchyid→string; rowversion.
- **Snowflake:** NUMBER default (38,0); TIMESTAMP_*(9)→µs+warn; VARIANT/OBJECT/ARRAY→string/variant; GEOGRAPHY→string; VECTOR.
- **BigQuery:** BIGNUMERIC(76)→string if >38; NUMERIC(38,9); DATETIME; INTERVAL; JSON; GEOGRAPHY.
- **Teradata:** BYTEINT; PERIOD→two cols or string; INTERVAL; TIMESTAMP(6); NUMBER.
- **PostgreSQL:** numeric(unbounded)→policy; money; timetz; interval; jsonb; arrays; inet; uuid.
- **MySQL/MariaDB:** unsigned bigint→decimal(20,0); unsigned int→bigint; year; tinyint(1); 0000-00-00→null+log; enum/set; bit; json.
- **Db2/HANA:** TIMESTAMP(12)→µs+warn; DECFLOAT; GRAPHIC; ST_GEOMETRY.

### 9.4 Unmappable policy (parameter, per-column override)
| Case | Options | **[DEFAULT]** |
|------|---------|---------------|
| decimal precision > 38 | string / fail / decimal(38,s)+warn | **string** + highlight + approval |
| nanosecond timestamp | truncate+warn / string / fail | **truncate+warn** (client confirms) |
| out-of-range / zero dates | null+log / string / fail | **fail** |
| geo / json / interval | string / binary / skip | **string** |
| pre-1582 dates | `date_rebase_mode` CORRECTED/LEGACY | **CORRECTED**+log |

**Rule:** never silent. On deviation → `type_mapping_log` entry + highlight; prod = **hold for approval**, dev = continue with warning. Approved mappings stored in `approved_mappings`, reused next time.

### 9.5 Schema drift policy (per table)
| Event | **[DEFAULT]** |
|-------|---------------|
| new column | evolve (add + log) |
| dropped column | fail (safe) — or `keep_nullable` if chosen |
| type widening | evolve |
| type narrowing | fail |
| renamed column | **detect via position+type heuristic → flag for approval** (cannot be 100% certain) |
| reordered columns | map by name, ignore order |
| duplicate/special-char/reserved names | sanitize + log (Delta column-mapping mode where needed) |

---

## 10. Raw layer
- Exact mirror: original names, order, types, precision/scale.
- Added audit columns (prefixed via `audit_prefix` to avoid clashes): `ingestion_time` (UTC), `run_id`, `batch_id`, `source_file_name`, `source_file_modified_time`, optional `row_hash`, `source_system`.
- DDL generated dynamically from source metadata (CREATE TABLE); no hand-written DDL.
- Write mode: append **[DEFAULT]**, or keep last N successful runs → overflow auto-archived.
- `schema_origin` recorded: SOURCE_METADATA / SIDECAR / CONTRACT / INFERRED. Strict mode fails on INFERRED.
- Idempotent: same file/batch re-run does not duplicate (file-level tracking).
- Partition by ingestion date / source date / custom.

---

## 11. Curated layer
- `curated.mode`: LATEST_SNAPSHOT | UPSERT_LATEST | INCREMENTAL_APPEND | CDC_APPLY | SCD2.
- `delete_handling`: soft / hard / ignore. `history_retention`: all / N days / N runs. `dedupe`: key + ordering column.
- CDC: op column, ordering column; out-of-order handled by ordering; delete-then-insert handled.
- Snapshot-diff deletes: key missing today → mark deleted, **guarded by a safety threshold** (if today's count drops > X%, stop + alert instead of mass-deleting).
- Transforms (config-driven): rename, cast, select/drop, derived (expression), constant, trim/standardize, default, PII mask/hash.
- **Reference/lookup (master) data** — small but critical tables get a dedicated `is_reference=true` flag: loaded first, validated, used in referential DQ. **[ADDED]**
- **Referential integrity DQ** — orphan child rows (FK with no parent) flagged/quarantined per rule. **[ADDED]**
- **Conflict resolution** when same key arrives from two sources — `source_priority` or latest-timestamp wins, logged. **[ADDED]**
- DQ rules: not-null, unique, range, regex, allowed-values, referential, row-count threshold, freshness, schema. Action per rule: fail / quarantine / warn / flag. Results to `dq_result_log`; bad rows to `reject_records`.

---

## 12. Consumption (gold + delivery)
- **Gold:** logic in `gold_logic` table, `logic_type = SQL | STRUCTURED`; declared dependencies (DAG, cycle-checked); refresh full/incremental; SQL validated before run; stale/failed upstream → skip + alert.
- **Delivery:** `output_type = FILES | TABLE | VIEW | BOTH`; naming/partition/compression; done-marker; overwrite vs dated folders; access grant to client; delivery frequency; delivery retry; `delivery_log` confirmation.

---

## 13. Large-scale / 100 TB handling  **[ADDED — was the biggest gap]**
`volume_class`: small | medium | large | very_large. large/very_large auto-enables:
- **Chunking:** by partition / key-range / date; never single query.
- **Parallelism:** bounded by memory + connection limits + source load budget.
- **Checkpoint per chunk:** resume from failed chunk, not from start.
- **File sizing:** target ~128 MB–1 GB; avoid millions of tiny files.
- **Source protection:** throttle, off-peak window, read isolation (snapshot/`nolock`-equivalent), **replica vs primary** choice. **[ADDED]**
- **Write at scale:** partition pruning on merge; compaction/optimize; Z-order/clustering on filter columns.
- **Backfill mode:** years of history in resumable windows; separate from daily.
- **Backfill vs daily collision:** lock + queue so they don't overwrite each other. **[ADDED]**
- **Poison record:** a single bad row that fails the job every time → after K retries, isolate that row/chunk to `reject_records` and continue. **[ADDED]**
- **Skew/shuffle tuning**, auto-scaling guidance.
- **Recon at scale:** partition-level counts + aggregates + sampling; full hash only where chosen.

---

## 14. Reconciliation (mandatory)
### 14.1 Schema recon (before load)
Per column: exists, type match, **precision ≥**, **scale ≥**, **length ≥**, nullable. Shortfall → highlight (data-loss risk) → prod hold / dev warn. Logged in `type_mapping_log`.

### 14.2 Data recon (after load)
Row count; **numeric sum (exact for decimal/int, tolerance for float)** — this is the primary catch for precision/scale loss; min/max; null count; distinct key count; **max string length** (truncation catch); date min/max; optional row-hash (L3) where both sides are Spark.

### 14.3 Levels
L1 (count+schema) and L2 (aggregates) mandatory everywhere. L3 (hash) default on Files→Raw and Raw→Curated (both Spark). DWH→Files/Table: L1+L2 mandatory, L3 per chosen table (cross-engine hash needs canonical formatting; float needs tolerance). **[OPEN]** numeric sum push-down in DWH vs Spark — **[DEFAULT]** push-down in DWH (source of truth, no data movement).

### 14.4 Honest limits  **[ADDED]**
- Cross-engine sum/hash can differ by float/decimal/null/encoding; handled by exact-for-decimal + tolerance-for-float + canonical hash.
- **Recon passing does NOT prove business logic is correct** — recon checks fidelity (did data move intact), not semantics (is the transform right). Business correctness needs test cases + client sign-off.

### 14.5 Failure behaviour
Recon fail → **watermark does NOT advance**, next step stops, alert sent. Retry policy: **[DEFAULT]** stop on recon fail (no auto-retry), require investigation. Results in `recon_results`.

---

## 15. Orchestration
- **Dependency graph (DAG):** each (step+table) is a node; edges = dependencies. Run a single node, a table's full flow, a group, or everything. Order enforced (raw→curated→gold).
- **Table-wise isolation:** one table's failure stops only its downstream; others continue. Run status SUCCESS / PARTIAL / FAILED.
- **Gold trigger:** only after all required upstream curated tables succeed. **[ADDED detail]**
- **Scheduling:** schedule-based; platform scheduler executes (Databricks Workflows, ADF, Fabric pipelines, Airflow, EventBridge, Cloud Scheduler, cron). Pipelines/jobs/schedules **auto-created on deploy from JSON** (hybrid: auto discovery → human review of gaps → auto job creation). New source table → "pending approval", never auto-added.
- **Schedule miss → catch-up.** **Long run overlapping next schedule → skip/queue (no overlap).** **[ADDED]**
- **Restart from failed step.** Concurrency lock per table. Pause/resume/retire a table (retire → keep or drop).
- **Validate / dry-run** before deploy (JSON, connections, type maps, gold SQL, cycles).

---

## 16. Migration-specific  **[ADDED — was thin]**
- **Initial full load + ongoing incremental** combined; re-baseline is a separate mode.
- **Cutover plan:** defined switch point; pre-cutover full recon.
- **Parallel run:** old + new run together for a window; automated compare report. **[ADDED]**
- **Rollback:** Delta RESTORE / Iceberg snapshot rollback for target; documented fallback to old system.
- **Reconciliation sign-off:** client-facing recon report (counts, sums, key checks) as migration proof/acceptance artifact.
- **Zero-downtime vs downtime window:** stated per migration. **[ADDED]**
- **Multi-table point-in-time consistency:** option to pin all tables to one source snapshot/SCN/LSN so the set is mutually consistent. **[ADDED]**

---

## 17. Security
- **Code:** source only in our private repo; client gets compiled/obfuscated wheel from private feed; license/expiry check; contract (no reverse-engineering). ~90%, not absolute (stated honestly).
- **Data:** never leaves client; no telemetry; logs hold counts/metadata, not values; error messages masked; `reject_records` (real rows) stay in client storage, restricted, UI shows counts+reasons only; data preview default off, PII-masked when on; tests use synthetic data only.
- **Config at client:** version/audit/rollback in control tables (replaces Git PR since config is client-side). **[OPEN]** confirm this replacement is acceptable.
- **Secrets:** identity-first (IAM/SP/MI/Kerberos); when a secret is unavoidable (DWH JDBC, SFTP, vector key) → stored in client secret store, **referenced by name only** in config, never typed in UI, never in JSON.
- **PII:** discovery (name/pattern/dictionary-based) flags candidate PII; masking/hash at curated/gold; **non-prod masking** when prod data is copied to dev. **[ADDED]**
- **Roles:** Admin/Developer/Operator/Viewer/Approver mapped to platform groups. Honest note: notebook UI is *not* the security boundary — real enforcement is platform permissions + prod-writes-as-service-principal + prod config approval.
- **On-prem:** Kerberos, HDFS/SMB/NFS, air-gapped. **Air-gapped license = offline signed key** (no license-server callout). **[ADDED]**

---

## 18. Config (JSON) structure
Parameter priority: **run-override > table > source > global**. Three kinds: mandatory (only for chosen steps), conditional (e.g., CDC → op/ordering columns), optional (defaults). Validated before run.

Illustrative shape (design, not code):
```
{
  "environment": "dev|test|prod",
  "client_id": "...", "timezone": "UTC",
  "platform": "auto|databricks|fabric|glue|emr|dataproc|synapse|onprem",
  "steps": "auto|[S1b,S3,...]",
  "source": { "category": "...", "connection": {"host","port","auth_type","secret_ref","ssl"},
              "selection": {"schemas|tables|regex|exclude|custom_sql|columns|filter"},
              "format_options": {...for files...}, "metadata_level":"NONE|BASIC|FULL|FULL_PLUS" },
  "load": { "type":"FULL|INCREMENTAL|CDC|APPEND_ONLY|SNAPSHOT_DIFF",
            "watermark_column", "lookback", "volume_class", "parallelism" },
  "raw":  { "write_mode":"append|last_n", "retention_runs", "archive":{"enabled","location","purge_days"},
            "audit_prefix", "partition_by", "schema_origin", "strict" },
  "schema": { "drift_policy":{...}, "unmappable_policy":{...per column...}, "date_rebase_mode" },
  "curated": { "mode", "keys", "ordering_column", "delete_handling", "history_retention",
               "transforms":[...], "dq_rules":[...], "is_reference", "source_priority" },
  "gold": { "logic_ref", "logic_type", "dependencies", "refresh" },
  "target": { "catalog","schema","table","location","table_format":"delta|iceberg",
              "create_if_missing", "managed|external", "naming_rule", "properties" },
  "delivery": { "output_type","naming","partition","compression","done_marker","grant" },
  "recon": { "level":"L1|L2|L3", "numeric_sum_side":"dwh|spark", "tolerance", "on_fail":"stop" },
  "vector": { "store","embed_model","dim","chunking" },
  "operation": { "run_mode":"normal|backfill|reprocess|dry_run|validate", "date_range","retries","alerts" }
}
```

---

## 19. Control & log tables (all at client)
**Control:** `source_registry`, `table_config`, `column_config`, `dq_rules`, `constraints`, `gold_logic`, `approved_mappings`, `watermark`, `config_versions` (version/audit), `approvals`, `locks`.

**Log (append-only, crash-safe, secrets masked, resolved-config snapshot per run):**
`run_log`, `step_log`, `table_log`, `file_log`, `extract_log`, `watermark_log`, `schema_log`, `type_mapping_log`, `dq_result_log`, `reject_records`, `recon_results`, `retention_archive_log`, `drop_log`, `maintenance_log`, `delivery_log`, `vector_log`, `error_log`, `config_audit_log`, `alert_log`.

`error_log` fields: run_id, step, table, file, error_code, category (config/connection/auth/schema/data/DQ/recon/platform), severity, message, stack, row/col, retryable, attempt_no.

**Log retention policy** (so logs don't grow forever): keep detail N days, aggregate older, purge per `log_retention`. **[ADDED]**
**Control backup/DR:** control/log tables included in client backup schedule; documented restore. **[ADDED]**

---

## 20. UI (notebook-based)
Two cells: install wheel; `launch_ui()`. Platform auto-detected; uses user/job identity.
**Fallback levels:** (1) rich UI (ipywidgets), (2) platform-native widgets, (3) JSON paste cell — exact support per platform confirmed during testing.
**Tabs:** Environment/platform/client · Connections (secret-ref + test) · Pipeline (steps, auto-selected fields) · Source/Target pickers (catalog→schema→table dropdowns, metadata level) · Schema & type-mapping preview (highlights + approvals) · Curated settings · Schedule · Run & monitor · Logs/Recon/Errors (filter/search/export, paginated) · Approvals · Config history (version/rollback/promote).
**Run modes:** "Run now" (small/test, in notebook) and "Submit as job" (survives notebook close; prod runs as service principal). Scheduled runs need no UI.
**Cost visibility per run** surfaced where the platform exposes it. **[ADDED]**

---

## 21. Extensibility (adapter contracts)
| New thing | Add in | Core changes? |
|-----------|--------|---------------|
| new database/DWH | L0 connect + L1 metadata + L2 type-map adapter | no |
| new cloud/storage | L0 storage adapter | no |
| new file format | L1 reader | no |
| new target format | L4/L5 writer | no |
| new vector store | L8 adapter | no |
| new DQ rule | rule registry entry | no |
| new auth (e.g., Kerberos) | L0 auth adapter | no |
| new odd type | L2 mapping entry (human adds; unknown type safe-fails until then) | no |

---

## 22. Operations & lifecycle
Logging (every file/table/error/reject, crash-safe) · Alerts (Outlook via Graph/SMTP; other channels as adapters) · Framework version pin per client+env, upgrade dev→test→prod, rollback · Config migration on version change · **Log retention** · **Control backup/DR** · Support without data (**redacted support bundle** — logs/config/versions, no values) · **Onboarding checklist** (permissions, secrets, network) and **offboarding** (full data+config removal with verification) · **Cost visibility**.

---

## 23. Platform coverage
**Compute:** Databricks (AWS/Azure/GCP), Microsoft Fabric, Synapse Spark, AWS Glue, EMR (EC2/EKS/Serverless), GCP Dataproc (+Serverless), Cloudera, OSS Spark on K8s/on-prem.
**Storage:** S3, ADLS Gen2, OneLake, GCS, Blob, HDFS, MinIO/Ceph, NFS/SMB, OCI/OSS/others.
**Catalog:** Unity, Hive, Glue, Fabric Lakehouse, Iceberg REST/Polaris/Nessie, BigLake, path-based.
A **capability matrix** (Delta/Iceberg read-write, constraints, catalog, auth per platform) is built and verified against current docs during the test phase — "works everywhere" is claimed only after a platform is certified. Fabric: Delta native, Iceberg via its supported path.

---

## 24. Phase plan
1. Scope lock (now) → this document confirmed.
2. Design detail finalized (config schema, table columns, UI flow, capability matrix, test plan).
3. **Phase 1 code:** core + config validation + logging + recon + DWH→Raw + Raw→Curated + notebook UI, on **one platform [OPEN]**.
4. Real-data test on that platform.
5. Phase 2: DWH→Files, type-mapping matrices, constraints capture.
6. Phase 3: Gold, Drop, Archive, Maintenance.
7. Phase 4: Volume↔Table, Vectors.
8. Phase 5: remaining DWH/storage adapters, multi-platform + on-prem certification, migration cutover/parallel-run/sign-off tooling.
Each phase tested before the next.

---

## 25. Master coverage checklist (91+ points)
Legend: ✅ covered · ⚠️ was thin → now resolved in this doc · (section)

A. Source/extraction: schema drift ✅(9.5); rename detect ⚠️→✅(9.5); no-watermark ✅(6); **watermark dup/null** ✅(now: validated, fallback to full/hash); clock-back/overlap ⚠️→✅(15); source down/retry ✅(13); timeout ⚠️→✅(13); **source lock/isolation** ✅(13); 100TB ✅(13); billions ✅(13); **replica vs primary** ✅(13).

B. Data quality: precision/scale ✅(14.2); length trunc ✅(14.2); type mismatch ✅(14.1); null/empty tokens ✅; dup keys ✅; out-of-order CDC ✅(11); late data ✅(11); hard delete ✅(11); partial snapshot threshold ✅(11); encoding ✅; CSV embedded/multiline ✅; corrupt→quarantine ✅; tz/date-rebase ✅(9.4); **referential integrity** ✅(11); **multi-source conflict** ✅(11); float tolerance ✅(14.2).

C. Orchestration: step auto-select ✅(6); individual vs full ✅(15); DAG ✅(15); isolate failure ✅(15); cluster-death resume ✅(13); lock ✅(15); schedule miss catch-up ✅(15); **overlap skip/queue** ✅(15); **gold trigger timing** ✅(15); **backfill vs daily collision** ✅(13); restart ✅(15); **poison record** ✅(13).

D. Target: auto-create ✅; evolution ✅(9.5); small-files/compaction ✅(13); merge-at-scale ✅(13); catalogs ✅(23); managed/external ✅; time-travel/drop ✅(12/§Drop); **concurrent-writer commit conflict** ✅(now: retry-on-conflict + lock); **existing-table schema mismatch** ✅(now: schema recon on first run → hold); partition choice ⚠️→✅(13).

E. Recon: schema ✅(14.1); count/sum/null/distinct/maxlen ✅(14.2); hash-where-safe ✅(14.3); scale sampling ✅(13/14.3); cross-engine honesty ✅(14.4); **retry-vs-stop** ✅(14.5); periodic full recon ✅; **recon≠business-correctness** ✅(14.4).

F. Security: code-vs-data ✅(0/17); config-at-client ✅(17); secret-ref ✅(17); obfuscation+license ✅(17); no-download ✅(17); no-telemetry ✅(17); PII mask ✅(17); **PII discovery** ✅(17); roles ✅(17); **air-gapped license** ✅(17); Kerberos/on-prem ✅(17); audit ✅(19).

G. Ops: logging ✅(19); crash-safe log ✅(19); alerts ✅(22); version upgrade ✅(22); config migration ✅(22); **log retention** ✅(19); **control backup/DR** ✅(19); redacted support ✅(22); onboarding/offboarding ✅(22); **cost visibility** ✅(20/22).

H. Migration: initial+incremental ✅(16); **cutover** ✅(16); **parallel run** ✅(16); rollback ✅(16); **sign-off** ✅(16); **zero-downtime/window** ✅(16); historical+delta reconcile ✅(16).

I. Newly added: **Kafka/stream** [OPEN](8); **SaaS/API** [OPEN](8); **multi-table point-in-time consistency** ✅(16); **reference/lookup data** ✅(11); **non-prod masking** ✅(17).

**Result:** every A–H point is now either covered or resolved in this document. Only genuine scope choices remain under I (streaming, SaaS/API) and the [OPEN] items in §26.

---

## 26. OPEN items — your decision needed
1. Config version/audit in control tables (replacing Git PR, since config is client-side) — OK?
2. Recon numeric-sum push-down in DWH (default) vs Spark — OK?
3. Streaming (Kafka/Event Hubs/Kinesis) — in scope or future?
4. SaaS/API sources (Salesforce/SAP/REST) — in scope or future?
5. Phase 1 platform — Databricks or Fabric first?
6. Unmappable defaults (§9.4): precision>38→string, nanosecond→truncate+warn, zero/out-of-range dates→fail — OK?
7. Type-mapping hold: prod=hold-for-approval, dev=continue-with-warning — OK?
8. Collaboration drives (SharePoint/Drive/Box) — needed now or later?

---

## 27. Agreement
No code is written until you review this document and reply **"confirm"**. After confirm, we build Phase 1 only, test it, then proceed phase by phase.
