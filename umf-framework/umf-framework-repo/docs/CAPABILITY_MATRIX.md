# Platform capability matrix

The framework is *designed* to run everywhere, but each platform is only
*certified* after it has been tested there end to end with real data. This
matrix records that testing. Fill a cell with the date and framework version
when a capability is verified on a platform; leave it blank until then.

Status key: `ok` verified · `partial` works with noted limits · blank not yet
tested · `n/a` not applicable on that platform.

## Compute and table formats

| Capability | Databricks | Fabric | Synapse | AWS Glue | EMR | Dataproc | On-prem Spark |
|------------|-----------|--------|---------|----------|-----|----------|---------------|
| Platform auto-detect | | | | | | | |
| Spark session reuse | | | | | | | |
| Delta read/write | | | | | | | |
| Delta MERGE | | | | | | | |
| Iceberg read/write | | | | | | | |
| Iceberg MERGE | | | | | | | |
| Catalog create schema/table | | | | | | | |
| Atomic overwrite (INSERT OVERWRITE) | | | | | | | |
| NOT NULL / CHECK constraints | | | | | | | |

## Sources

| Source | Describe schema | Read | Incremental | Chunked read | Certified on |
|--------|-----------------|------|-------------|--------------|--------------|
| Files: CSV | | | n/a | n/a | |
| Files: Parquet/ORC/Avro | | | n/a | n/a | |
| Oracle (JDBC) | | | | | |
| SQL Server (JDBC) | | | | | |
| PostgreSQL (JDBC) | | | | | |
| MySQL/MariaDB (JDBC) | | | | | |
| Snowflake | | | | | |
| BigQuery | | | | | |
| Redshift | | | | | |
| Teradata | | | | | |

## Catalogs

| Catalog | Create | Read | Write | Certified on |
|---------|--------|------|-------|--------------|
| Unity Catalog | | | | |
| Hive Metastore | | | | |
| AWS Glue Catalog | | | | |
| Fabric Lakehouse | | | | |
| Iceberg REST / Polaris / Nessie | | | | |

## Secrets and auth

| Mechanism | Databricks | Fabric | AWS | GCP | On-prem |
|-----------|-----------|--------|-----|-----|---------|
| Managed identity / instance role | | | | | |
| Secret scope / vault reference | | | | | |
| Kerberos | n/a | n/a | n/a | n/a | |

## Vector stores

| Store | List ids | Upsert | Delete | Certified on |
|-------|----------|--------|--------|--------------|
| Databricks Vector Search | | | | |
| Azure AI Search | | | | |
| OpenSearch | | | | |
| pgvector | | | | |

## UI

| UI mode | Databricks | Fabric | Synapse | Glue/EMR notebook | Dataproc notebook |
|---------|-----------|--------|---------|-------------------|-------------------|
| Rich (ipywidgets) | | | | | |
| JSON-config fallback | ok | ok | ok | ok | ok |

(The JSON-config fallback needs only Python, so it is certified everywhere by
construction.)
