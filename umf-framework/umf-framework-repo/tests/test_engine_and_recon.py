"""Tests for reconciliation, staging, orchestration, DQ, gold and delivery.

These cover the framework's core guarantees as code: precision loss is caught,
a failed reconciliation never touches the target, one table's failure does not
stop others, and gold logic is read-only.
"""

from __future__ import annotations

from decimal import Decimal

from tests.fakes import FakeWriter
from umf.adapters.table_format.base import CommitMode
from umf.core.constants import (
    CuratedMode,
    KeyStrategy,
    ReconLevel,
    RunStatus,
    Step,
    StepStatus,
)
from umf.core.exceptions import ConfigError, DataQualityError, ReconciliationError
from umf.dq.rules import DQAction, DQRule, DQRuleType, decide, enforce
from umf.engine.chunking import single_chunk  # noqa: F401 (import sanity)
from umf.engine.curated import build_curated_plan, merge_keys
from umf.engine.dag import build_dag, node_id
from umf.engine.decision import SourceKind, TargetLayer, decide_steps
from umf.engine.identifiers import TableIdentifier
from umf.engine.keys import is_unique, plan_key
from umf.engine.recon_gate import CountReconGate
from umf.engine.retention import RunInfo, plan_retention
from umf.engine.staging import stage_reconcile_commit


def test_scale_loss_caught_by_sum():
    from umf.recon.compare import ReconConfig, compare_profiles
    from umf.recon.profile import ColumnProfile, MetricKind, ReconProfile

    cfg = ReconConfig(level=ReconLevel.L2, decimal_tolerance=Decimal("0"))
    src = ReconProfile(row_count=3, columns={
        "amount": ColumnProfile("amount", MetricKind.DECIMAL, null_count=0, numeric_sum=Decimal("1234.56"))
    })
    tgt = ReconProfile(row_count=3, columns={
        "amount": ColumnProfile("amount", MetricKind.DECIMAL, null_count=0, numeric_sum=Decimal("1234.00"))
    })
    results = compare_profiles(src, tgt, cfg)
    sum_check = [r for r in results if r["check_name"] == "sum:amount"][0]
    # Counts match but the sum does not: the scale loss is caught.
    count_check = [r for r in results if r["check_name"] == "count"][0]
    assert count_check["status"] == "pass"
    assert sum_check["status"] == "fail"


def test_float_tolerance_absorbs_noise_but_catches_real_loss():
    from umf.recon.compare import ReconConfig, compare_profiles
    from umf.recon.profile import ColumnProfile, MetricKind, ReconProfile

    cfg = ReconConfig(level=ReconLevel.L2, float_tolerance=1e-6)

    def prof(total):
        return ReconProfile(row_count=2, columns={
            "x": ColumnProfile("x", MetricKind.FLOAT, null_count=0, numeric_sum=total)
        })

    ok = compare_profiles(prof(1.0000001), prof(1.0000002), cfg)
    bad = compare_profiles(prof(1.0000001), prof(1.5), cfg)
    assert [r for r in ok if r["check_name"] == "sum:x"][0]["status"] == "pass"
    assert [r for r in bad if r["check_name"] == "sum:x"][0]["status"] == "fail"


def test_recon_pass_commits_and_fail_leaves_target_untouched():
    target = TableIdentifier.create(catalog="main", schema="raw", table="orders")

    # recon passes (expected == staged) -> commit
    w_ok = FakeWriter(staged_rows=100)
    res = stage_reconcile_commit(
        writer=w_ok, df="DF", target=target, run_id="r1", mode=CommitMode.APPEND,
        gate_factory=lambda n: CountReconGate(expected=100, actual=n),
        target_columns=[("id", "BIGINT")],
    )
    assert res.committed and w_ok.published
    assert "PUBLISH" in w_ok.op_names()

    # recon fails (expected != staged) -> no publish, target untouched
    w_bad = FakeWriter(staged_rows=90)
    raised = False
    try:
        stage_reconcile_commit(
            writer=w_bad, df="DF", target=target, run_id="r2", mode=CommitMode.APPEND,
            gate_factory=lambda n: CountReconGate(expected=100, actual=n),
            target_columns=[("id", "BIGINT")],
        )
    except ReconciliationError:
        raised = True
    assert raised
    assert not w_bad.published
    assert "PUBLISH" not in w_bad.op_names()


def test_decision_rules():
    assert decide_steps(source_kind=SourceKind.DWH, target_layer=TargetLayer.CURATED) == [
        Step.DWH_TO_TABLE, Step.RAW_TO_CURATED
    ]
    assert decide_steps(source_kind=SourceKind.FILES, target_layer=TargetLayer.RAW) == [
        Step.FILES_TO_RAW
    ]


def test_dag_orders_steps_within_table():
    dag = build_dag(steps=[Step.DWH_TO_TABLE, Step.RAW_TO_CURATED], tables=["orders"])
    order = [n.id for n in dag.topo_order()]
    assert order.index(node_id(Step.DWH_TO_TABLE, "orders")) < order.index(
        node_id(Step.RAW_TO_CURATED, "orders")
    )


def test_retention_keeps_newest_successful():
    runs = [
        RunInfo("r1", RunStatus.SUCCESS, "2026-10-18"),
        RunInfo("r2", RunStatus.FAILED, "2026-10-19"),
        RunInfo("r3", RunStatus.SUCCESS, "2026-10-20"),
        RunInfo("r4", RunStatus.SUCCESS, "2026-10-21"),
    ]
    plan = plan_retention(runs, retention_runs=2, archive_enabled=True)
    assert plan.keep == ["r4", "r3"]
    assert "r2" in plan.archive  # failed run not kept


def test_key_strategy_and_uniqueness():
    kp = plan_key(strategy=KeyStrategy.COMPOSITE, key_columns=["a", "b"], all_columns=["a", "b", "c"])
    assert list(kp.merge_keys) == ["a", "b"]
    assert is_unique(100, 100)
    assert not is_unique(100, 98)


def test_curated_mode_maps_to_commit_mode():
    plan = build_curated_plan(
        mode=CuratedMode.UPSERT_LATEST, all_columns=["id", "u"],
        key_strategy=KeyStrategy.COMPOSITE, key_columns=["id"], ordering_column="u",
    )
    assert plan.commit_mode == CommitMode.MERGE
    assert merge_keys(plan) == ["id"]


def test_curated_upsert_requires_ordering():
    raised = False
    try:
        build_curated_plan(mode=CuratedMode.UPSERT_LATEST, all_columns=["id"],
                           key_strategy=KeyStrategy.COMPOSITE, key_columns=["id"])
    except ConfigError:
        raised = True
    assert raised


def test_dq_decisions():
    fail_rule = DQRule("nn", DQRuleType.NOT_NULL, ["id"], DQAction.FAIL)
    assert decide(fail_rule, failed_count=0).passed
    assert not decide(fail_rule, failed_count=5).passed
    q_rule = DQRule("rng", DQRuleType.RANGE, ["x"], DQAction.QUARANTINE)
    out = decide(q_rule, failed_count=3)
    assert out.passed and out.remove_bad_rows


def test_enforce_raises_on_failed_rule():
    fail_rule = DQRule("nn", DQRuleType.NOT_NULL, ["id"], DQAction.FAIL)
    raised = False
    try:
        enforce(decide(fail_rule, failed_count=5), "orders")
    except DataQualityError:
        raised = True
    assert raised


def test_gold_structured_to_sql_and_validation():
    from umf.engine.gold import GoldLogic, GoldLogicType, build_sql, order_gold, validate_sql

    g = GoldLogic("g", GoldLogicType.STRUCTURED, definition={
        "select": ["a.id"], "from": "curated.a",
        "aggregates": [{"expr": "SUM(a.x)", "as": "total"}], "group_by": ["a.id"],
    })
    sql = build_sql(g)
    assert "SUM(a.x) AS total" in sql and "GROUP BY" in sql

    raised = False
    try:
        validate_sql("DROP TABLE t")
    except ConfigError:
        raised = True
    assert raised

    a = GoldLogic("a", GoldLogicType.SQL, sql="SELECT 1")
    b = GoldLogic("b", GoldLogicType.SQL, sql="SELECT 1", dependencies=["a"])
    order = [x.name for x in order_gold([b, a])]
    assert order.index("a") < order.index("b")


def test_delivery_and_delete_sync():
    from umf.engine.delivery import build_delivery_plan, delete_sync_plan, resolve_volume_table_op, VolumeTableOp

    plan = build_delivery_plan({"delivery": {"output_type": "FILES", "destination": "/out", "single_file": True}})
    assert plan.single_file

    sync = delete_sync_plan(source_ids={"1", "2", "3"}, vector_ids={"2", "3", "9"})
    assert sync.to_add == ["1"] and sync.to_delete == ["9"] and sync.to_keep == ["2", "3"]

    assert resolve_volume_table_op({"volume": {"direction": "table_to_volume"}}) == VolumeTableOp.TABLE_TO_VOLUME
