"""Data quality: rules, decisions and evaluation.

A rule says what rows must satisfy and what to do when they do not (fail,
quarantine, warn or flag). The decision logic is pure and tested; the Spark
evaluator counts violations and applies quarantine filtering.

Public surface:
    DQRule, DQRuleType, DQAction, DQOutcome   the rule model and outcome
    decide, enforce                           the pure decision and enforcement
    build_rules                               build rules from config
    SparkDQEvaluator                          count violations and decide (Spark)
"""

from umf.dq.registry import SparkDQEvaluator, build_rules
from umf.dq.rules import (
    DQAction,
    DQOutcome,
    DQRule,
    DQRuleType,
    decide,
    enforce,
)

__all__ = [
    "DQRule",
    "DQRuleType",
    "DQAction",
    "DQOutcome",
    "decide",
    "enforce",
    "build_rules",
    "SparkDQEvaluator",
]
