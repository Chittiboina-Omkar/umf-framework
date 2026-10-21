"""Config-driven column transforms for the curated layer.

The curated layer may rename columns, cast types, add derived or constant
columns, trim or default values, mask or hash sensitive fields, and drop or
select columns. Rather than hard-code any of this, the transforms are described
in the config as an ordered list of operations, turned into a typed plan here,
and applied to a DataFrame by a small Spark layer.

Order matters (a rename must happen before a later step references the new
name), so the plan preserves the config order exactly. Building the plan is pure
Python and tested; applying it is a lazy-Spark function.

Masking note:
    Masking and hashing are how PII is protected at the curated layer. Masking
    replaces a value with a fixed redaction, and hashing replaces it with a
    one-way hash so the value cannot be recovered but equal values still match.
    Both are applied here under the user's control, column by column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

from umf.core.constants import _StrEnum
from umf.core.exceptions import ConfigError


class TransformType(_StrEnum):
    """The kinds of transform operation supported."""

    RENAME = "RENAME"          # change a column's name
    CAST = "CAST"              # change a column's type
    DERIVED = "DERIVED"        # add a column from a SQL expression
    CONSTANT = "CONSTANT"      # add a column with a constant value
    TRIM = "TRIM"              # strip whitespace from string columns
    DEFAULT = "DEFAULT"        # replace nulls with a default value
    MASK = "MASK"              # redact a value (PII protection)
    HASH = "HASH"              # one-way hash a value (PII protection)
    DROP = "DROP"              # remove columns
    SELECT = "SELECT"          # keep only the named columns, in order


@dataclass(frozen=True)
class TransformOp:
    """One transform operation.

    Attributes:
        type: The :class:`TransformType`.
        params: Operation parameters. The meaning depends on the type, for
            example ``{"from": "amt", "to": "amount"}`` for a rename, or
            ``{"name": "total", "expr": "qty * price"}`` for a derived column.
    """

    type: TransformType
    params: Dict[str, Any] = field(default_factory=dict)


def build_transform_plan(raw_transforms: Sequence[dict]) -> List[TransformOp]:
    """Build an ordered transform plan from the config.

    Args:
        raw_transforms: The ``transforms`` list from the config, each a
            dictionary with a ``type`` and the parameters for that type.

    Returns:
        The ordered list of :class:`TransformOp`.

    Raises:
        ConfigError: If an operation has an unknown type.
    """
    plan: List[TransformOp] = []
    for raw in raw_transforms:
        try:
            op_type = TransformType.coerce(raw["type"])
        except (KeyError, ValueError) as exc:
            raise ConfigError(f"Invalid transform {raw}: {exc}") from exc
        params = {k: v for k, v in raw.items() if k != "type"}
        plan.append(TransformOp(type=op_type, params=params))
    return plan


def apply_transforms(df: Any, plan: Sequence[TransformOp]) -> Any:
    """Apply a transform plan to a Spark DataFrame (lazy Spark import).

    Operations are applied in order, so later operations see the effects of
    earlier ones.

    Args:
        df: The Spark DataFrame to transform.
        plan: The ordered transform plan.

    Returns:
        The transformed DataFrame.

    Raises:
        ConfigError: If an operation is missing a required parameter.
    """
    from pyspark.sql import functions as F  # lazy import

    current = df
    for op in plan:
        p = op.params
        try:
            if op.type == TransformType.RENAME:
                current = current.withColumnRenamed(p["from"], p["to"])
            elif op.type == TransformType.CAST:
                current = current.withColumn(p["column"], F.col(p["column"]).cast(p["to"]))
            elif op.type == TransformType.DERIVED:
                current = current.withColumn(p["name"], F.expr(p["expr"]))
            elif op.type == TransformType.CONSTANT:
                current = current.withColumn(p["name"], F.lit(p["value"]))
            elif op.type == TransformType.TRIM:
                for c in p["columns"]:
                    current = current.withColumn(c, F.trim(F.col(c)))
            elif op.type == TransformType.DEFAULT:
                current = current.withColumn(
                    p["column"], F.coalesce(F.col(p["column"]), F.lit(p["value"]))
                )
            elif op.type == TransformType.MASK:
                # Replace the value with a fixed redaction string.
                current = current.withColumn(
                    p["column"],
                    F.when(F.col(p["column"]).isNull(), F.lit(None)).otherwise(
                        F.lit(p.get("mask", "***REDACTED***"))
                    ),
                )
            elif op.type == TransformType.HASH:
                current = current.withColumn(
                    p["column"], F.sha2(F.col(p["column"]).cast("string"), 256)
                )
            elif op.type == TransformType.DROP:
                current = current.drop(*p["columns"])
            elif op.type == TransformType.SELECT:
                current = current.select(*p["columns"])
        except KeyError as exc:
            raise ConfigError(
                f"Transform {op.type} is missing a required parameter: {exc}."
            ) from exc

    return current
