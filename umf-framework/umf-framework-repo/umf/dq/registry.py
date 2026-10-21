"""Building data-quality rules from config, and evaluating them with Spark.

:func:`build_rules` turns the ``dq_rules`` section of a config into typed
:class:`~umf.dq.rules.DQRule` objects. :class:`SparkDQEvaluator` counts the rows
that violate each rule and hands the count to the pure decision in
:func:`umf.dq.rules.decide`, so the Spark layer only measures and the policy
lives in one tested place.

PySpark is imported lazily. :func:`build_rules` is pure and testable.
"""

from __future__ import annotations

from typing import Any, List, Sequence, Tuple

from umf.core.exceptions import ConfigError
from umf.core.logging_setup import get_logger
from umf.dq.rules import DQAction, DQOutcome, DQRule, DQRuleType, decide

_log = get_logger("dq.registry")


def build_rules(raw_rules: Sequence[dict]) -> List[DQRule]:
    """Build typed rules from the config's ``dq_rules`` list.

    Each raw rule is a dictionary like::

        {"name": "amount_range", "type": "RANGE", "columns": ["amount"],
         "action": "QUARANTINE", "params": {"min": 0}, "threshold": 0}

    Args:
        raw_rules: The list of raw rule dictionaries from the config.

    Returns:
        The list of typed :class:`DQRule` objects.

    Raises:
        ConfigError: If a rule has an unknown type or action, or is missing a
            required field.
    """
    rules: List[DQRule] = []
    for raw in raw_rules:
        try:
            rule_type = DQRuleType.coerce(raw["type"])
            action = DQAction.coerce(raw.get("action", "FAIL"))
        except (KeyError, ValueError) as exc:
            raise ConfigError(f"Invalid DQ rule {raw}: {exc}") from exc
        rules.append(
            DQRule(
                name=raw.get("name") or f"{rule_type}_{','.join(raw.get('columns', []))}",
                rule_type=rule_type,
                columns=list(raw.get("columns", [])),
                action=action,
                params=dict(raw.get("params", {})),
                threshold=int(raw.get("threshold", 0)),
            )
        )
    return rules


class SparkDQEvaluator:
    """Counts rule violations on a Spark DataFrame and decides each outcome.

    The evaluator builds a boolean "violation" expression per rule, counts the
    rows that violate it, and calls the pure decision. For a ``QUARANTINE`` rule
    it also returns the surviving DataFrame with the violating rows removed, so
    the caller can route the bad rows to the reject table.
    """

    def evaluate(
        self, df: Any, rules: Sequence[DQRule]
    ) -> Tuple[Any, List[DQOutcome]]:
        """Evaluate all rules against a DataFrame.

        Args:
            df: The Spark DataFrame to check.
            rules: The rules to evaluate, in order.

        Returns:
            A tuple of (the possibly-filtered DataFrame, the list of outcomes).
            The DataFrame has rows removed only for rules whose decision sets
            ``remove_bad_rows`` (quarantine).
        """
        from pyspark.sql import functions as F  # lazy import

        current = df
        outcomes: List[DQOutcome] = []

        for rule in rules:
            violation = self._violation_expr(F, rule)
            failed_count = current.filter(violation).count()
            outcome = decide(rule, failed_count=failed_count)
            outcomes.append(outcome)

            if outcome.remove_bad_rows:
                # Keep only the rows that do not violate the rule.
                current = current.filter(~violation)

        return current, outcomes

    @staticmethod
    def _violation_expr(F: Any, rule: DQRule) -> Any:
        """Build a boolean column that is true for rows violating the rule.

        Args:
            F: The ``pyspark.sql.functions`` module.
            rule: The rule to build an expression for.

        Returns:
            A Spark column expression that is true for violating rows.

        Raises:
            ConfigError: If the rule type is not supported by the evaluator.
        """
        cols = rule.columns
        if rule.rule_type == DQRuleType.NOT_NULL:
            # Violated when any of the columns is null.
            expr = F.lit(False)
            for c in cols:
                expr = expr | F.col(c).isNull()
            return expr
        if rule.rule_type == DQRuleType.RANGE:
            c = cols[0]
            low = rule.params.get("min")
            high = rule.params.get("max")
            expr = F.lit(False)
            if low is not None:
                expr = expr | (F.col(c) < F.lit(low))
            if high is not None:
                expr = expr | (F.col(c) > F.lit(high))
            return expr
        if rule.rule_type == DQRuleType.REGEX:
            c = cols[0]
            pattern = rule.params["pattern"]
            # Violated when the value does not match the pattern.
            return ~F.col(c).rlike(pattern)
        if rule.rule_type == DQRuleType.ALLOWED_VALUES:
            c = cols[0]
            allowed = rule.params["values"]
            return ~F.col(c).isin(list(allowed))
        if rule.rule_type == DQRuleType.UNIQUE:
            # Uniqueness is a whole-column property, not a per-row predicate, so
            # it is handled by the key uniqueness check rather than a filter.
            # Returning an always-false expression here means this evaluator does
            # not double-count it.
            return F.lit(False)
        if rule.rule_type == DQRuleType.REFERENTIAL:
            # Referential integrity needs a join against a parent table, which
            # the curated step performs with the lookup in hand; it is not a
            # simple per-row filter, so it is evaluated there.
            return F.lit(False)
        raise ConfigError(f"Unsupported DQ rule type '{rule.rule_type}'.")
