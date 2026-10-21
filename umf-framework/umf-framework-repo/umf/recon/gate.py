"""The full reconciliation gate and the sign-off report.

Part 4 defined a minimal :class:`~umf.engine.recon_gate.ReconGate` interface and
a basic count gate so the staging orchestrator could enforce the control flow.
This module provides the full gate, :class:`FullReconGate`, which compares two
measured profiles with the real checks and tolerances from
:mod:`umf.recon.compare`. Because it implements the same interface, the staging
orchestrator uses it unchanged: the richer reconciliation simply slots in.

It also provides :func:`build_signoff_report`, which turns a set of recon
results into a compact, client-facing summary. For a migration, this report is
the acceptance artifact: it shows, check by check, that the data moved intact.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from umf.core.logging_setup import get_logger
from umf.engine.recon_gate import ReconGate, ReconOutcome
from umf.recon.compare import ReconConfig, compare_profiles
from umf.recon.profile import ReconProfile

_log = get_logger("recon.gate")


class FullReconGate(ReconGate):
    """A reconciliation gate that compares two measured profiles.

    Attributes:
        source_profile: The profile measured on the source side.
        target_profile: The profile measured on the staged target side.
        config: The comparison configuration (level and tolerances).
    """

    def __init__(
        self,
        *,
        source_profile: ReconProfile,
        target_profile: ReconProfile,
        config: ReconConfig,
    ) -> None:
        """Create the gate with the two profiles to compare.

        Taking already-measured profiles (rather than measuring inside the gate)
        keeps the gate pure and testable; the step is responsible for measuring
        the source and the staged target with a
        :class:`~umf.recon.measurer.ReconMeasurer` and passing the results here.

        Args:
            source_profile: The source side profile.
            target_profile: The staged target side profile.
            config: The comparison configuration.
        """
        self.source_profile = source_profile
        self.target_profile = target_profile
        self.config = config

    def check(self) -> ReconOutcome:
        """Compare the profiles and return the outcome.

        Returns:
            A :class:`ReconOutcome`. It passes only when every check passes; a
            single failing check fails the gate, which makes the staging
            orchestrator discard the staged data and leave the target untouched.
        """
        results = compare_profiles(self.source_profile, self.target_profile, self.config)
        failed = [r for r in results if r["status"] == "fail"]
        passed = not failed
        _log.info(
            "reconciliation evaluated",
            extra={
                "checks": len(results),
                "failed": len(failed),
                "passed": passed,
            },
        )
        return ReconOutcome(passed=passed, results=results)


@dataclass
class SignoffReport:
    """A compact summary of a reconciliation, suitable for client sign-off.

    Attributes:
        passed: Whether every check passed.
        total_checks: The number of checks performed.
        failed_checks: The number that failed.
        failures: The failing check results, for the detail section.
        by_level: A count of checks per recon level.
    """

    passed: bool
    total_checks: int
    failed_checks: int
    failures: List[dict]
    by_level: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        """Return the report as a plain dictionary.

        Returns:
            The report fields as a JSON-serialisable dictionary.
        """
        return {
            "passed": self.passed,
            "total_checks": self.total_checks,
            "failed_checks": self.failed_checks,
            "failures": self.failures,
            "by_level": self.by_level,
        }


def build_signoff_report(results: List[dict]) -> SignoffReport:
    """Summarise recon results into a sign-off report.

    Args:
        results: The recon result dictionaries from one or more comparisons
            (data recon, and optionally schema recon merged in).

    Returns:
        A :class:`SignoffReport`.
    """
    failures = [r for r in results if r["status"] == "fail"]
    by_level: Dict[str, int] = {}
    for r in results:
        by_level[r["level"]] = by_level.get(r["level"], 0) + 1

    return SignoffReport(
        passed=not failures,
        total_checks=len(results),
        failed_checks=len(failures),
        failures=failures,
        by_level=by_level,
    )
