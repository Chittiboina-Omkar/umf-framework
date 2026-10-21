"""Engine: orchestration that turns staged data into committed, verified tables.

Part 4 adds the pieces that move data into the raw layer safely:

    identifiers   naming a target table consistently and safely
    audit         the audit columns added to the raw exact mirror
    retention     deciding which raw runs to keep, archive or delete
    recon_gate    the gate that must pass before any commit (basic here; the
                  full engine arrives in Part 5)
    staging       stage, reconcile, then commit atomically, or discard

The staging orchestration and the retention and audit logic are pure Python and
tested here; the Spark-backed table writers live in
:mod:`umf.adapters.table_format`.
"""

from umf.engine.identifiers import TableIdentifier
from umf.engine.recon_gate import (
    AlwaysPassGate,
    CountReconGate,
    ReconGate,
    ReconOutcome,
)
from umf.engine.retention import RetentionPlan, RunInfo, plan_retention
from umf.engine.staging import StageAndCommitResult, stage_reconcile_commit
from umf.engine.decision import SourceKind, TargetLayer, decide_steps, resolve_steps
from umf.engine.dag import Dag, Node, build_dag, node_id
from umf.engine.executor import (
    DryRunExecutor,
    ExecutorRegistry,
    StepExecutor,
    dry_run_registry,
)
from umf.engine.runner import run

__all__ = [
    "TableIdentifier",
    "ReconGate",
    "ReconOutcome",
    "AlwaysPassGate",
    "CountReconGate",
    "RunInfo",
    "RetentionPlan",
    "plan_retention",
    "StageAndCommitResult",
    "stage_reconcile_commit",
    # orchestration (Part 7)
    "SourceKind",
    "TargetLayer",
    "decide_steps",
    "resolve_steps",
    "Dag",
    "Node",
    "build_dag",
    "node_id",
    "StepExecutor",
    "DryRunExecutor",
    "ExecutorRegistry",
    "dry_run_registry",
    "run",
]
