# Deployment guide

How the framework is built, protected and rolled out, and how a client uses it
without ever receiving the source code or sending you their data.

## 1. Build the wheel

```bash
python -m build          # produces dist/umf_framework-<version>-py3-none-any.whl
```

The core has no third-party dependencies, so the wheel installs anywhere. Spark
and the UI are optional extras the platform usually already provides.

## 2. Protect the code (obfuscation)

The code runs in the client environment, so it cannot be hidden completely; the
goal is to make copying it hard and to back that with a licence and a contract.
Two common options wrap the wheel build:

- **PyArmor** obfuscates the Python bytecode and can bind a build to an expiry
  or a machine. Run it over the `umf/` package, then build the wheel from the
  obfuscated output.
- **Nuitka** compiles the package to a native extension, which is harder still
  to read, at the cost of a platform-specific build.

Add a licence/expiry check at import time (a signed token the wheel verifies).
For air-gapped sites the token is an **offline signed key** the client is given,
so no licence server call is needed.

Honest limit: obfuscation raises the cost of theft, it does not make it
impossible. The contract's no-reverse-engineering clause is the backstop.

## 3. Publish to a private feed

Push the (obfuscated) wheel to a **private package feed** the client can install
from but cannot browse the source of:

- Azure Artifacts, AWS CodeArtifact, GCP Artifact Registry, or a private PyPI.

The client installs by version:

```bash
pip install umf-framework==<version> --index-url <your-private-feed>
```

The source repository stays entirely on your side.

## 4. Per-platform install

| Platform | How the wheel is installed |
|----------|----------------------------|
| Databricks | cluster library, or `%pip install` in a notebook, from the private feed |
| Microsoft Fabric | environment library, or `%pip install` in a notebook |
| Synapse Spark | workspace package / `%pip install` |
| AWS Glue | `--additional-python-modules` pointing at the wheel in S3 |
| EMR | bootstrap action `pip install`, or a packaged virtual environment |
| Dataproc | init action `pip install`, or a custom image |
| On-prem / OSS Spark | `pip install` into the driver and executor Python |
| Air-gapped | copy the wheel in; install from the local file; offline licence key |

## 5. Config and state stay at the client

- The **config** lives in the client's own storage, versioned and audited in the
  `config_versions` control table (this replaces Git history, which is not used
  because the config never leaves the client).
- **Logs, watermarks and reject data** live in the client's control/log tables.
- **Secrets** live in the client's own vault; the config carries only a
  reference (for example `databricks://scope/key`), never a value.
- There is **no telemetry**; the framework makes no outbound calls.

## 6. Scheduling

The framework is a single entry point (`umf.engine.runner.run`) that takes a
config. A platform scheduler calls it:

- Databricks Workflows, Microsoft Fabric pipelines, Azure Data Factory, Apache
  Airflow, AWS EventBridge / Step Functions, GCP Cloud Scheduler, or cron.

A scheduled run must have no unapproved defaults; the runner refuses to proceed
otherwise, so a scheduled run never relies on an assumption nobody approved.

## 7. Upgrades and rollback

- Pin a wheel version per client and environment. Upgrade dev -> test -> prod.
- A config-schema change ships with a migration so existing configs keep
  working. Roll back by installing the previous wheel version; the config store
  keeps every prior config version, so configs can be rolled back independently.

## 8. Support without seeing data

When a client hits a problem, they export a **redacted support bundle**: the run
logs, the resolved config (secrets are references) and the versions, with no
data values. That is enough to diagnose most issues without access to the
client's data.
