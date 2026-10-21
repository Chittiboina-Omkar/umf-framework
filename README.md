# Universal Migration & Data Framework (UMF)

One parameter-driven framework that moves data from any supported source (data
warehouse, database, files, object storage) to any supported target (Delta or
Iceberg tables, files, or vectors), across every major cloud and on-premise
Spark platform. Behaviour is controlled entirely by a JSON config; there is no
per-client or per-table code.

## Core principles

- **Parameter-driven.** One codebase; a JSON config decides everything.
- **Exact-first, never silent.** Types map exactly where possible; where a type
  cannot be represented, the table fails with the details surfaced, unless the
  config records an explicit decision. Precision and scale loss are caught by
  reconciliation.
- **No bad or partial data.** Data is staged, reconciled, then committed
  atomically; a failed check discards the staged data and leaves the target
  untouched.
- **Isolation.** One table's failure never stops another; a run reports
  `SUCCESS`, `PARTIAL` or `FAILED` honestly.
- **No question at run time.** Every decision lives in the config; a scheduled
  run is unattended, and unapproved defaults block a real run.
- **Portable.** The core depends only on the Python standard library; Spark and
  cloud SDKs are optional extras the platform already provides. Everything
  platform-specific lives behind adapters.
- **Data and config stay with the client; secrets are referenced, never stored.**

## Install

```bash
pip install umf-framework            # core (no third-party dependencies)
pip install "umf-framework[spark]"   # add PySpark where the platform lacks it
pip install "umf-framework[ui]"      # add the rich notebook UI
```

On a managed platform (Databricks, Fabric, Glue, EMR, Dataproc) Spark is already
present, so the plain install is usually enough.

## Quick start (notebook)

```python
from umf.ui import launch_ui
import umf

ui = launch_ui(framework_version=umf.__version__)   # opens the UI, or a
                                                     # dependency-free fallback
```

Or run a config directly:

```python
import json, umf
from umf.engine.runner import run
from umf.steps.registry import build_executor_registry
from umf.control import ControlPlane, JsonlFileSink
from umf.adapters.spark_session import get_or_create_spark

config = json.load(open("configs/example_files_to_curated.json"))
spark = get_or_create_spark()
registry = build_executor_registry(spark=spark)
control = ControlPlane(sink=JsonlFileSink("/dbfs/tmp/umf-logs"))

summary = run(config, framework_version=umf.__version__,
              executor_registry=registry, control_plane=control)
print(summary["status"])
```

A dry run (no executor registry) validates the whole orchestration without
touching data:

```python
summary = run(config, framework_version=umf.__version__)   # dry-run
```

## Layout

```
umf/
  core/       config, validation, logging, platform detection, errors, run context
  control/    control and log tables, log sink, watermark store, config store
  secrets/    reference-based secret resolution
  typing/     exact-first type mapping (source -> canonical -> target)
  adapters/   spark session, source readers (files, jdbc), Delta/Iceberg writers, vector store
  engine/     identifiers, audit, retention, staging+atomic commit, keys, transforms,
              curated, chunking, decision rules, DAG, executor, gold, delivery, runner
  recon/      schema and data reconciliation, gate, sign-off report
  dq/         data-quality rules and decisions
  steps/      step executors (files->raw, raw->curated, dwh->raw/files, gold, vectors, volume/table)
  ui/         notebook UI (rich + dependency-free fallback)
```

See `docs/DEPLOYMENT.md` for packaging, obfuscation and per-platform rollout,
and `docs/CAPABILITY_MATRIX.md` for what has been certified on each platform.

## Tests

```bash
pytest                       # with pytest installed
python tests/run_tests.py    # portable fallback, no pytest required
```

The tests cover the correctness-critical, pure-Python behaviour (validation,
type mapping, chunking, reconciliation, staging isolation, orchestration, DQ,
gold, delivery, control plane, secrets, UI) and run without Spark.

## Honest status

The pure-Python core is implemented and tested. The Spark-backed data
operations (reading sources, writing Delta/Iceberg, merges, measurement) are
written against each platform's APIs but must be validated in your own
environment before production use, platform by platform — see the capability
matrix. "Works on every platform" is a design property; each platform is only
*certified* after it is tested there.
