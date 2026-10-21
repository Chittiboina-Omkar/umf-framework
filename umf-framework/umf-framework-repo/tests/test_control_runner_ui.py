"""Tests for the control plane, secrets, the runner's isolation and the UI.

These cover persistence (logs, watermarks, config versions), secret handling by
reference, the runner's table isolation (one table fails, others continue), and
the UI's config building and fallback.
"""

from __future__ import annotations

import os
import tempfile

import umf
from umf.control import ControlPlane, FileConfigStore, FileWatermarkStore, JsonlFileSink
from umf.control import records
from umf.control.table_defs import ALL_TABLES
from umf.core.constants import Step, StepStatus
from umf.core.exceptions import UmfError
from umf.core.run_context import StepOutcome
from umf.engine.executor import ExecutorRegistry, StepExecutor
from umf.engine.runner import run
from umf.secrets import EnvSecretResolver, SecretError, SecretRegistry
from umf.ui.launch import JsonConfigUI, launch_ui
from umf.ui.model import build_config, flatten_config, relevant_sections, ui_fields


def test_tables_defined():
    assert len(ALL_TABLES) >= 20
    assert "run_log" in ALL_TABLES and "watermark" in ALL_TABLES


def test_record_rejects_unknown_column():
    raised = False
    try:
        records.build("run_log", run_id="x", status="SUCCESS", not_a_column="oops")
    except UmfError:
        raised = True
    assert raised


def test_config_store_versions_and_rollback():
    work = tempfile.mkdtemp()
    store = FileConfigStore(work, client_id="acme", environment="PROD")
    v1 = store.save({"a": 1}, author="t", note="init")
    store.save({"a": 2}, author="t")
    store.rollback(1, author="t")
    history = store.history()
    assert [h.version for h in history] == [1, 2, 3]
    assert history[-1].config_hash == v1.config_hash  # rollback restored v1


def test_watermark_only_moves_forward():
    work = tempfile.mkdtemp()
    wm = FileWatermarkStore(os.path.join(work, "wm.json"))
    wm.advance("t", "2026-10-20", run_id="r1")
    wm.advance("t", "2026-10-21", run_id="r1")
    change = wm.advance("t", "2026-10-01", run_id="r1")  # backward
    assert wm.get("t") == "2026-10-21"  # unchanged
    assert change.new_value == "2026-10-21"


def test_secret_by_reference():
    os.environ["UMF_TEST_SECRET"] = "hunter2secret"
    reg = SecretRegistry([EnvSecretResolver()])
    assert reg.resolve("env://UMF_TEST_SECRET") == "hunter2secret"
    raised = False
    try:
        reg.resolve("env://DOES_NOT_EXIST_UMF")
    except SecretError:
        raised = True
    assert raised


def _run_config() -> dict:
    return {
        "environment": "PROD", "client_id": "acme", "platform": "DATABRICKS",
        "steps": ["DWH_TO_TABLE", "RAW_TO_CURATED"],
        "source": {"category": "RDBMS"},
        "load": {"type": "INCREMENTAL", "watermark_column": "u", "volume_class": "medium"},
        "raw": {"write_mode": "append"}, "schema": {"mode": "EVOLVE"},
        "curated": {"mode": "UPSERT_LATEST", "key_strategy": "COMPOSITE",
                     "key_columns": ["id"], "ordering_column": "u"},
        "target": {"table_format": "DELTA", "catalog": "main", "schema": "r", "table": "orders", "partition_by": []},
        "recon": {"level": "L2", "decimal_tolerance": 0}, "operation": {"run_mode": "normal"},
        "tables": [
            {"name": "orders", "target": {"table_format": "DELTA", "catalog": "main", "schema": "r", "table": "orders", "partition_by": []}},
            {"name": "customers", "target": {"table_format": "DELTA", "catalog": "main", "schema": "r", "table": "customers", "partition_by": []}},
        ],
    }


def test_runner_dry_run_succeeds():
    summary = run(_run_config(), framework_version=umf.__version__)
    assert summary["status"] == "SUCCESS"
    assert len(summary["outcomes"]) == 4  # 2 tables x 2 steps


def test_runner_isolation_partial():
    class PartialFail(StepExecutor):
        def execute(self, ctx, node, table_config):
            if node.table == "customers" and node.step == Step.DWH_TO_TABLE:
                raise UmfError("simulated failure", context={"table": "customers"})
            return StepOutcome(step=str(node.step), table=node.table, status=StepStatus.SUCCESS)

    reg = ExecutorRegistry()
    reg.register_all(PartialFail())
    work = tempfile.mkdtemp()
    cp = ControlPlane(sink=JsonlFileSink(os.path.join(work, "logs")))
    summary = run(_run_config(), framework_version=umf.__version__,
                  executor_registry=reg, control_plane=cp)
    by = {(o["table"], o["step"].split(".")[-1]): o["status"] for o in summary["outcomes"]}
    assert summary["status"] == "PARTIAL"
    assert by[("orders", "DWH_TO_TABLE")] == "SUCCESS"
    assert by[("orders", "RAW_TO_CURATED")] == "SUCCESS"
    assert by[("customers", "DWH_TO_TABLE")] == "FAILED"
    assert by[("customers", "RAW_TO_CURATED")] == "SKIPPED"


def test_ui_model_build_and_sections():
    fields = ui_fields()
    assert any(f.path == "target.table" and f.mandatory for f in fields)
    assert "Curated" in relevant_sections([Step.RAW_TO_CURATED])
    assert "Curated" not in relevant_sections([Step.DWH_TO_FILES])
    cfg = build_config({"environment": "PROD", "target.schema": "r", "target.catalog": ""})
    assert cfg["target"]["schema"] == "r" and "catalog" not in cfg["target"]
    assert flatten_config(cfg)["target.schema"] == "r"


def test_ui_fallback_runs_dry():
    ui = launch_ui(framework_version=umf.__version__, prefer_rich=False)
    assert isinstance(ui, JsonConfigUI)
    ui.set_config(_run_config())
    summary = ui.run()
    assert summary["status"] == "SUCCESS"
