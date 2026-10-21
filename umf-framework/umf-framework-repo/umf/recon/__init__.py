"""Reconciliation (layer L7): prove the data moved intact.

Reconciliation measures the same metrics on the source and the staged target and
compares them. It is what stands between staging and commit, so that no bad or
partial data ever reaches the target.

The division of labour:

    profile       the measured metrics (row count, per-column sum, nulls, ...)
    measurer      computes a profile from data (Spark; sampling at scale)
    compare       compares two profiles with the right tolerances (pure logic)
    schema_recon  checks precision / scale / length preservation, pre-load
    gate          the FullReconGate that plugs into the staging orchestrator
    report        the client-facing sign-off report

The comparison and tolerance logic (the correctness-critical part) is pure
Python and fully tested; only the measurer needs Spark.
"""

from umf.recon.compare import ReconConfig, compare_profiles
from umf.recon.gate import FullReconGate, SignoffReport, build_signoff_report
from umf.recon.measurer import ReconMeasurer, SparkReconMeasurer
from umf.recon.profile import ColumnProfile, MetricKind, ReconProfile
from umf.recon.schema_recon import schema_recon

__all__ = [
    "ReconProfile",
    "ColumnProfile",
    "MetricKind",
    "ReconConfig",
    "compare_profiles",
    "schema_recon",
    "ReconMeasurer",
    "SparkReconMeasurer",
    "FullReconGate",
    "SignoffReport",
    "build_signoff_report",
]
