"""Tests for config validation, type mapping and chunking.

These cover the correctness-critical, pure-Python behaviour: that the framework
refuses to run on a bad or incomplete config, maps types exactly or fails
loudly, and splits a large extraction correctly.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from umf.core.config_loader import load_config, validate_config
from umf.core.constants import TableFormat
from umf.engine.chunking import (
    incremental_predicate,
    plan_date_chunks,
    plan_numeric_chunks,
    single_chunk,
)
from umf.typing import SourceColumn, TypeMappingEngine
from umf.typing.unmappable import HandlingConfig


def _complete_config() -> dict:
    """Return a complete, valid config for a files-to-raw run."""
    return {
        "environment": "PROD",
        "client_id": "acme",
        "platform": "DATABRICKS",
        "steps": ["DWH_TO_TABLE"],
        "source": {"category": "RDBMS"},
        "load": {"type": "INCREMENTAL", "watermark_column": "u", "volume_class": "medium"},
        "raw": {"write_mode": "append"},
        "schema": {"mode": "EVOLVE"},
        "target": {"table_format": "DELTA", "catalog": "main", "schema": "r", "table": "o", "partition_by": []},
        "recon": {"level": "L2", "decimal_tolerance": 0},
        "operation": {"run_mode": "normal"},
    }


def test_incomplete_config_lists_mandatory_errors():
    result = validate_config({"environment": "PROD"})
    assert not result.ok
    assert any("client_id" in e for e in result.errors)
    assert any("target.table" in e for e in result.errors)


def test_complete_config_validates_clean():
    result = validate_config(_complete_config())
    assert result.ok
    assert result.errors == []


def test_unattended_run_blocks_unapproved_defaults():
    # A config missing a safe field (operation) still validates, with a
    # confirmation; an unattended run treats that confirmation as blocking.
    cfg = _complete_config()
    cfg.pop("operation")
    result = load_config(cfg, require_no_confirmations=True)
    assert not result.ok


def test_explicit_default_needs_no_confirmation():
    cfg = _complete_config()
    # partition_by is explicitly [] (the default); it must not be flagged.
    result = validate_config(cfg)
    assert all("partition_by" not in c.path for c in result.confirmations)


def test_bad_enum_value_is_rejected():
    cfg = _complete_config()
    cfg["load"]["type"] = "WRONG"
    result = validate_config(cfg)
    assert not result.ok
    assert any("WRONG" in e for e in result.errors)


def test_oracle_number_maps_exactly():
    eng = TypeMappingEngine(source_name="oracle", table_format=TableFormat.DELTA,
                            handling=HandlingConfig.from_config(None))
    plan = eng.plan_table("t", [SourceColumn("amt", "number", precision=12, scale=2)])
    assert plan.ok
    assert plan.ddl_columns() == ["amt DECIMAL(12,2)"]


def test_unmappable_precision_blocks_without_decision():
    eng = TypeMappingEngine(source_name="oracle", table_format=TableFormat.DELTA,
                            handling=HandlingConfig.from_config(None))
    plan = eng.plan_table("t", [SourceColumn("big", "number", precision=42, scale=4)])
    assert not plan.ok
    assert plan.issues[0].name == "big"


def test_decision_resolves_unmappable():
    eng = TypeMappingEngine(
        source_name="oracle", table_format=TableFormat.DELTA,
        handling=HandlingConfig.from_config({"default": "FAIL", "columns": {"t.big": "DECIMAL_38"}}),
    )
    plan = eng.plan_table("t", [SourceColumn("big", "number", precision=42, scale=4)])
    assert plan.ok
    entry = plan.entries[0]
    assert entry.decision == "DECIMAL_38" and entry.data_loss_risk


def test_iceberg_widens_narrow_ints():
    eng = TypeMappingEngine(source_name="generic", table_format=TableFormat.ICEBERG,
                            handling=HandlingConfig.from_config(None))
    plan = eng.plan_table("t", [SourceColumn("flag", "tinyint")])
    assert plan.entries[0].target_type == "int"
    assert plan.entries[0].fallback_used


def test_mysql_unsigned_bigint_widens_to_decimal():
    eng = TypeMappingEngine(source_name="mysql", table_format=TableFormat.DELTA,
                            handling=HandlingConfig.from_config(None))
    plan = eng.plan_table("t", [SourceColumn("n", "bigint unsigned")])
    assert "DECIMAL(20,0)" in plan.entries[0].target_type


def test_numeric_chunks_cover_range():
    chunks = plan_numeric_chunks(column="id", min_value=1, max_value=1000, num_chunks=4)
    assert len(chunks) == 4
    assert ">= 1 " in chunks[0].predicate
    assert "<= 1000" in chunks[-1].predicate


def test_date_chunks_last_window_inclusive():
    chunks = plan_date_chunks(column="dt", start=date(2026, 1, 1), end=date(2026, 3, 15), window_days=30)
    assert chunks[-1].predicate.endswith("<= DATE '2026-03-15'")


def test_incremental_predicate_first_run_is_empty():
    assert incremental_predicate(column="u", watermark=None) == ""
    assert "u >" in incremental_predicate(column="u", watermark="2026-10-20")


def test_single_chunk_has_empty_predicate():
    assert single_chunk()[0].predicate == ""
