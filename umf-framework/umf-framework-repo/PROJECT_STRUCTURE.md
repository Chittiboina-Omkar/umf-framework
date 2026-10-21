# Project structure

The complete framework, built across 12 parts. Everything below is implemented.
The pure-Python logic is tested here (36 tests pass with no Spark); the
Spark-backed operations are validated per platform (see
`docs/CAPABILITY_MATRIX.md`).

```
umf-framework/
├── README.md                     overview, install, quick start
├── pyproject.toml                wheel packaging (core has no deps; spark/ui extras)
├── PROJECT_STRUCTURE.md          this file
├── docs/
│   ├── DEPLOYMENT.md             build, obfuscation, private feed, per-platform, config-at-client
│   └── CAPABILITY_MATRIX.md      what is certified on each platform (fill during testing)
├── configs/
│   ├── example_files_to_curated.json
│   └── example_dwh_to_raw.json
├── tests/                        36 tests, run with pytest or tests/run_tests.py (no pytest needed)
│   ├── fakes.py
│   ├── test_core_and_typing.py
│   ├── test_engine_and_recon.py
│   └── test_control_runner_ui.py
└── umf/                          the framework package (rename-able)
    ├── __init__.py               version, public exports
    ├── core/                     [Part 1] config, validation, logging, platform detect, errors, run context
    ├── control/                  [Part 2] 20 log/control tables, sink, watermark store, config store
    ├── secrets/                  [Part 2] reference-based secret resolution
    ├── typing/                   [Part 3,9] exact-first type mapping; 8 source families + generic
    │   └── mappers/              oracle, sqlserver, snowflake, bigquery, mysql, postgresql, teradata, generic
    ├── adapters/                 the multi-cloud plug-in layer
    │   ├── spark_session.py      [Part 3] platform-agnostic Spark session (L0)
    │   ├── source/               [Part 3,9] file reader, JDBC reader (L1)
    │   ├── table_format/         [Part 4] Delta and Iceberg writers
    │   └── vector/               [Part 10] vector store interface
    ├── engine/                   the orchestration and data-handling core
    │   ├── identifiers.py        [Part 4] safe table naming
    │   ├── audit.py              [Part 4] exact-mirror audit columns
    │   ├── retention.py          [Part 4] last-N-run retention
    │   ├── staging.py            [Part 4] stage -> recon gate -> atomic commit / discard
    │   ├── recon_gate.py         [Part 4] gate interface
    │   ├── keys.py               [Part 6] composite/hash keys, uniqueness, row hash
    │   ├── transforms.py         [Part 6] config-driven transforms (incl PII mask/hash)
    │   ├── curated.py            [Part 6] curated modes -> commit mode
    │   ├── chunking.py           [Part 9] large-table chunking + incremental predicate
    │   ├── decision.py           [Part 7] source+target -> steps
    │   ├── dag.py                [Part 7] dependency graph, topological order
    │   ├── executor.py           [Part 7] step executor interface + registry + dry-run
    │   ├── gold.py               [Part 10] gold logic (SQL/structured), validation, ordering
    │   ├── delivery.py           [Part 10] delivery, vectors, volume/table, manifest planning
    │   └── runner.py             [Part 7] single entry point: run(config)
    ├── recon/                    [Part 5] schema + data reconciliation, gate, sign-off
    ├── dq/                       [Part 6] data-quality rules and decisions
    ├── steps/                    [Part 8,9,10] the real step executors
    │   ├── common.py             type-plan gate, business columns, recon config
    │   ├── files_to_raw.py       S2
    │   ├── raw_to_curated.py     S3
    │   ├── dwh_to_table.py       S1b
    │   ├── dwh_to_files.py       S1
    │   ├── curated_to_gold.py    S4 (+ delivery)
    │   ├── vectors.py            S6 + volume/table (S5)
    │   └── registry.py           build_executor_registry(spark)
    └── ui/                       [Part 11] notebook UI
        ├── model.py              config <-> form (pure)
        ├── formatting.py         validation / run / recon / log rendering (pure)
        ├── launch.py             launch_ui + dependency-free JsonConfigUI fallback
        └── notebook_ui.py        rich ipywidgets UI (lazy)
```

## Build status

| Part | Area | Pure logic tested here | Needs your environment |
|------|------|------------------------|------------------------|
| 1 | Core (config, logging, platform, run) | yes | — |
| 2 | Control/log tables, secrets | yes | table-backed sink on Spark |
| 3 | Type mapping, file reader | yes (mapping) | file read on Spark |
| 4 | Stage/raw, atomic commit, Delta/Iceberg writers | yes (orchestration) | writers on Spark |
| 5 | Reconciliation | yes (compare/schema) | measurer on Spark |
| 6 | Curated: modes, keys, DQ, transforms | yes (decisions) | Spark data ops |
| 7 | Orchestration + runner | yes (full, dry-run) | — |
| 8 | Real executors (files->raw, raw->curated) | yes (gate/registry) | Spark data ops |
| 9 | DWH extract, chunking, 5 more mappers | yes (chunking/mappers) | JDBC on Spark |
| 10 | Gold, delivery, DWH->files, vectors, volume/table | yes (planning/validation) | Spark + services |
| 11 | Notebook UI | yes (model/formatting/fallback) | rich UI in a notebook |
| 12 | Packaging, docs, tests | yes (36 tests pass) | — |

## Next, in your environment

1. Build the wheel (`python -m build`) and install it on one platform
   (Databricks or Fabric recommended first).
2. Run a small files-to-curated job with a tiny Parquet/CSV sample; confirm the
   recon gate, audit columns and atomic commit behave as expected.
3. Run a DWH-to-raw job against a test database; confirm type mapping,
   incremental and chunking.
4. Fill in `docs/CAPABILITY_MATRIX.md` as each capability is verified.
5. Add the remaining source, catalog and vector-store adapters as needed; each
   is a new file behind an existing interface, with no change to the core.
```
