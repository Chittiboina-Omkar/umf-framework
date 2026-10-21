"""The reconciliation gate that guards every commit.

The staging orchestrator must not publish staged data into the target until
reconciliation passes. It expresses that requirement through a small interface,
:class:`ReconGate`, so that the orchestration does not depend on the details of
how reconciliation is performed.

Part 4 provides a basic :class:`CountReconGate` that compares the staged row
count to the expected source row count. This is enough to enforce the control
flow and to catch gross data loss. The full reconciliation engine (schema
checks, numeric sums, null counts, max lengths, and optional row hashing) is
layer L7 and arrives in Part 5; it will implement this same :class:`ReconGate`
interface, so the orchestrator does not change when the richer checks are added.

Keeping the gate as an interface is what lets the orchestration be tested in
pure Python with a gate that can be told to pass or fail on demand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class ReconOutcome:
    """The result of running the reconciliation gate.

    Attributes:
        passed: Whether every check passed. When false, the commit must not
            proceed and the staged data is discarded.
        results: One entry per check performed, as plain dictionaries ready to
            write to the ``recon_results`` log table. Each has at least
            ``check_name``, ``source_value``, ``target_value`` and ``status``.
    """

    passed: bool
    results: List[dict] = field(default_factory=list)


class ReconGate:
    """Interface for a gate that decides whether a commit may proceed."""

    def check(self) -> ReconOutcome:
        """Run the reconciliation checks and return the outcome.

        Returns:
            A :class:`ReconOutcome`.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class AlwaysPassGate(ReconGate):
    """A gate that always passes. Intended only for tests and dry runs."""

    def check(self) -> ReconOutcome:
        """Return a passing outcome with no checks.

        Returns:
            A passing :class:`ReconOutcome`.
        """
        return ReconOutcome(passed=True, results=[])


class CountReconGate(ReconGate):
    """A basic gate that compares a staged count to an expected count.

    This catches gross row loss between reading the source and staging the data.
    It is deliberately simple; the full engine in Part 5 adds the aggregate and
    schema checks that catch precision and type problems.

    Attributes:
        expected: The expected (source) row count.
        actual: The actual staged row count.
    """

    def __init__(self, *, expected: int, actual: int) -> None:
        """Create the gate.

        Args:
            expected: The source row count.
            actual: The staged row count.
        """
        self.expected = expected
        self.actual = actual

    def check(self) -> ReconOutcome:
        """Compare the counts and build the outcome and its log record.

        Returns:
            A :class:`ReconOutcome` that passes only when the counts match
            exactly, which is the correct tolerance for a row count.
        """
        passed = self.expected == self.actual
        result = {
            "check_name": "count",
            "level": "L1",
            "source_value": str(self.expected),
            "target_value": str(self.actual),
            "difference": str(self.actual - self.expected),
            "tolerance": "0",
            "status": "pass" if passed else "fail",
        }
        return ReconOutcome(passed=passed, results=[result])
