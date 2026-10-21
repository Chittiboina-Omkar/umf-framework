"""Data-quality rules and what to do when they fail.

A data-quality rule describes a condition rows must satisfy (not null, unique,
in range, matching a pattern, in an allowed set, referencing a parent) and an
action to take when rows violate it. The actions are the ones agreed in the
design:

    FAIL        stop the table when any (or more than a threshold of) rows fail,
    QUARANTINE  move the failing rows to a reject table and continue with the
                rest,
    WARN        log the failures but keep the rows,
    FLAG        add a flag column marking the failing rows, and keep them.

This module separates the *decision* (given how many rows failed, what should
happen) from the *measurement* (counting failing rows in Spark). The decision is
pure Python and fully tested: feed it a failed count and it tells you the action,
whether the bad rows should be removed, and whether the table should fail. The
Spark evaluator that counts failing rows is a thin layer on top, added where a
live DataFrame is available.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from umf.core.constants import _StrEnum
from umf.core.exceptions import DataQualityError


class DQAction(_StrEnum):
    """What to do with rows that violate a rule."""

    FAIL = "FAIL"
    QUARANTINE = "QUARANTINE"
    WARN = "WARN"
    FLAG = "FLAG"


class DQRuleType(_StrEnum):
    """The kind of condition a rule checks."""

    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    RANGE = "RANGE"
    REGEX = "REGEX"
    ALLOWED_VALUES = "ALLOWED_VALUES"
    REFERENTIAL = "REFERENTIAL"


@dataclass(frozen=True)
class DQRule:
    """One data-quality rule.

    Attributes:
        name: A stable identifier for the rule, used in logs.
        rule_type: The :class:`DQRuleType`.
        columns: The columns the rule applies to.
        action: The :class:`DQAction` to take on violation.
        params: Rule-specific parameters (for example ``{"min": 0, "max": 100}``
            for a range rule, or ``{"values": ["A", "B"]}`` for an allowed-values
            rule).
        threshold: The maximum number of failing rows tolerated before a ``FAIL``
            action actually fails. Zero (the default) means any failing row
            fails the table. Ignored for non-``FAIL`` actions.
    """

    name: str
    rule_type: DQRuleType
    columns: Sequence[str]
    action: DQAction = DQAction.FAIL
    params: Dict[str, Any] = field(default_factory=dict)
    threshold: int = 0


@dataclass(frozen=True)
class DQOutcome:
    """The decided outcome of evaluating a rule against a failing-row count.

    Attributes:
        rule_name: The rule's name.
        rule_type: The rule's type.
        action: The action taken.
        failed_count: How many rows violated the rule.
        passed: Whether the rule is considered passed (a WARN or FLAG always
            passes; QUARANTINE passes because the bad rows are removed; FAIL
            passes only within its threshold).
        remove_bad_rows: Whether the failing rows should be removed from the data
            being written (true only for QUARANTINE).
        message: A short, non-sensitive description of what happened.
    """

    rule_name: str
    rule_type: DQRuleType
    action: DQAction
    failed_count: int
    passed: bool
    remove_bad_rows: bool
    message: str

    def to_record(self, run_id: str, table: str) -> dict:
        """Build a ``dq_result_log`` record from the outcome.

        Args:
            run_id: The run identifier.
            table: The table the rule was evaluated on.

        Returns:
            A record dictionary matching the ``dq_result_log`` table.
        """
        from umf.control import records  # local import to avoid a cycle at load

        return records.build(
            "dq_result_log",
            run_id=run_id,
            table_name=table,
            rule_name=self.rule_name,
            rule_type=str(self.rule_type),
            passed=self.passed,
            failed_count=self.failed_count,
            action=str(self.action),
            detail=self.message,
        )


def decide(rule: DQRule, *, failed_count: int) -> DQOutcome:
    """Decide the outcome of a rule given how many rows failed it.

    This is the pure decision used by the Spark evaluator once it has counted the
    violating rows. Separating it out means the policy (what each action does,
    and how the threshold works) is tested directly, without Spark.

    Args:
        rule: The rule being evaluated.
        failed_count: The number of rows that violated the rule.

    Returns:
        The :class:`DQOutcome`.
    """
    if failed_count <= 0:
        # Nothing failed: the rule passes regardless of action.
        return DQOutcome(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            action=rule.action,
            failed_count=0,
            passed=True,
            remove_bad_rows=False,
            message="no rows failed the rule",
        )

    if rule.action == DQAction.FAIL:
        passed = failed_count <= rule.threshold
        return DQOutcome(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            action=rule.action,
            failed_count=failed_count,
            passed=passed,
            remove_bad_rows=False,
            message=(
                f"{failed_count} row(s) failed; within threshold {rule.threshold}"
                if passed
                else f"{failed_count} row(s) failed; exceeds threshold {rule.threshold}"
            ),
        )

    if rule.action == DQAction.QUARANTINE:
        return DQOutcome(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            action=rule.action,
            failed_count=failed_count,
            passed=True,  # the table proceeds, with the bad rows removed
            remove_bad_rows=True,
            message=f"{failed_count} row(s) quarantined to the reject table",
        )

    if rule.action == DQAction.WARN:
        return DQOutcome(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            action=rule.action,
            failed_count=failed_count,
            passed=True,
            remove_bad_rows=False,
            message=f"{failed_count} row(s) failed; warning only, rows kept",
        )

    # FLAG: keep the rows but mark them; the flag column is added by the Spark
    # evaluator. The rule passes.
    return DQOutcome(
        rule_name=rule.name,
        rule_type=rule.rule_type,
        action=rule.action,
        failed_count=failed_count,
        passed=True,
        remove_bad_rows=False,
        message=f"{failed_count} row(s) flagged; rows kept",
    )


def enforce(outcome: DQOutcome, table: str) -> None:
    """Raise if a rule outcome means the table must fail.

    Args:
        outcome: The decided outcome.
        table: The table name, for the error message.

    Raises:
        DataQualityError: If the outcome did not pass (a ``FAIL`` action over
            its threshold). The error carries the counts, never data values.
    """
    if not outcome.passed:
        raise DataQualityError(
            f"Data-quality rule '{outcome.rule_name}' failed on table '{table}': "
            f"{outcome.message}.",
            context={
                "table": table,
                "rule": outcome.rule_name,
                "rule_type": str(outcome.rule_type),
                "failed_count": outcome.failed_count,
            },
        )
