"""Business keys: how a row is identified when there is no reliable primary key.

Merges, de-duplication and change detection all need a stable way to say "this
is the same row". When the source has a real primary key, that is used. When it
does not, the user names the columns that together identify a row, and this
module forms a key from them, either by using the columns directly (a composite
key) or by hashing them into one key column.

Getting the hash right is subtle, and getting it wrong silently produces
duplicates or wrong merges. The canonicalisation rules here address the known
traps:

    * nulls are replaced with a distinct token, so a null is not confused with
      an empty string;
    * values can be trimmed and case-folded, so ``"ABC "`` and ``"abc"`` match
      when the business says they should;
    * a delimiter separates the columns, so ``"a" + "bc"`` and ``"ab" + "c"`` do
      not collide; and
    * the column order is fixed, so the same row always hashes the same way.

The module also decides whether a chosen key is actually unique: comparing the
row count to the distinct-key count tells us if duplicates exist, which the
framework must surface rather than silently merge away.

The planning and the canonicalisation specification are pure Python and tested
here; applying the hash and de-duplicating a DataFrame are small Spark helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence

from umf.core.constants import KeyStrategy
from umf.core.exceptions import ConfigError


# The physical name of the generated hash key column, prefixed to avoid clashing
# with a source column.
HASH_KEY_COLUMN = "_umf_key_hash"
# The physical name of the row-hash column used for change detection.
ROW_HASH_COLUMN = "_umf_row_hash"


@dataclass(frozen=True)
class HashSpec:
    """How to canonicalise values before hashing them into a key.

    Attributes:
        null_token: The text a null is replaced with, kept distinct from any
            real value so a null never collides with an empty string.
        trim: Whether to strip surrounding whitespace from each value.
        case_insensitive: Whether to case-fold each value, so keys match
            regardless of letter case.
        delimiter: The separator placed between columns, so adjacent values
            cannot run together and collide.
        algorithm: The hash algorithm, fixed to a 256-bit hash to make
            collisions effectively impossible.
    """

    null_token: str = "\u0000NULL"
    trim: bool = False
    case_insensitive: bool = False
    delimiter: str = "|"
    algorithm: str = "sha2-256"


@dataclass(frozen=True)
class KeyPlan:
    """The resolved plan for a table's business key.

    Attributes:
        strategy: The key strategy in effect.
        key_columns: The columns that form the key (for composite / natural) or
            that are hashed (for hash). Empty for ``NO_KEY``.
        produces_hash_key: Whether a single hash key column is generated. True
            for ``HASH`` and ``ALL_COLUMNS_HASH``.
        hash_key_name: The name of the generated key column, when one is
            produced.
        hash_spec: The canonicalisation spec used for hashing.
        merge_keys: The columns a merge joins on. For composite / natural these
            are the key columns; for hash strategies it is the single hash key.
    """

    strategy: KeyStrategy
    key_columns: Sequence[str]
    produces_hash_key: bool
    hash_key_name: Optional[str]
    hash_spec: HashSpec
    merge_keys: Sequence[str]


def plan_key(
    *,
    strategy: KeyStrategy,
    key_columns: Sequence[str],
    all_columns: Sequence[str],
    hash_spec: Optional[HashSpec] = None,
) -> KeyPlan:
    """Resolve a key plan from the configured strategy and columns.

    Args:
        strategy: The configured key strategy.
        key_columns: The columns the user named as the key, required for every
            strategy except ``ALL_COLUMNS_HASH`` and ``NO_KEY``.
        all_columns: All columns of the data, used by ``ALL_COLUMNS_HASH``.
        hash_spec: The canonicalisation spec; the default is used if omitted.

    Returns:
        The resolved :class:`KeyPlan`.

    Raises:
        ConfigError: If a key-bearing strategy is given no key columns, or a
            named key column is not among the data's columns.
    """
    spec = hash_spec or HashSpec()
    cols = list(key_columns)
    available = {c.lower() for c in all_columns}

    if strategy == KeyStrategy.NO_KEY:
        return KeyPlan(strategy, [], False, None, spec, [])

    if strategy == KeyStrategy.ALL_COLUMNS_HASH:
        return KeyPlan(
            strategy=strategy,
            key_columns=list(all_columns),
            produces_hash_key=True,
            hash_key_name=HASH_KEY_COLUMN,
            hash_spec=spec,
            merge_keys=[HASH_KEY_COLUMN],
        )

    # NATURAL_PK, COMPOSITE and HASH all require named key columns.
    if not cols:
        raise ConfigError(
            f"Key strategy {strategy} requires 'key_columns' to be specified."
        )
    missing = [c for c in cols if c.lower() not in available]
    if missing:
        raise ConfigError(
            f"Key columns not found in the data: {missing}.",
            context={"key_columns": cols},
        )

    if strategy == KeyStrategy.HASH:
        return KeyPlan(
            strategy=strategy,
            key_columns=cols,
            produces_hash_key=True,
            hash_key_name=HASH_KEY_COLUMN,
            hash_spec=spec,
            merge_keys=[HASH_KEY_COLUMN],
        )

    # NATURAL_PK or COMPOSITE: the named columns are the merge keys directly.
    return KeyPlan(
        strategy=strategy,
        key_columns=cols,
        produces_hash_key=False,
        hash_key_name=None,
        hash_spec=spec,
        merge_keys=cols,
    )


def is_unique(row_count: int, distinct_key_count: int) -> bool:
    """Decide whether a key is unique from the row and distinct-key counts.

    Args:
        row_count: The number of rows.
        distinct_key_count: The number of distinct key values.

    Returns:
        True when every row has a distinct key (no duplicates). When false, the
        key is not unique and the framework must surface the duplicates rather
        than merge them away silently.
    """
    return row_count == distinct_key_count


def add_key_hash(df: Any, plan: KeyPlan) -> Any:
    """Add the hash key column to a DataFrame, per the plan (lazy Spark).

    Does nothing if the plan does not produce a hash key. The hash is built by
    canonicalising each key column (null token, optional trim and case-fold),
    joining with the delimiter, and applying a 256-bit hash.

    Args:
        df: The Spark DataFrame.
        plan: The key plan.

    Returns:
        The DataFrame, with the hash key column added when the plan calls for
        one; otherwise the input unchanged.
    """
    if not plan.produces_hash_key:
        return df

    from pyspark.sql import functions as F  # lazy import

    spec = plan.hash_spec
    parts = []
    for col in plan.key_columns:
        value = F.col(col).cast("string")
        if spec.trim:
            value = F.trim(value)
        if spec.case_insensitive:
            value = F.lower(value)
        # Replace null with the distinct token so null != empty string.
        value = F.coalesce(value, F.lit(spec.null_token))
        parts.append(value)

    combined = F.concat_ws(spec.delimiter, *parts)
    return df.withColumn(plan.hash_key_name, F.sha2(combined, 256))


def add_row_hash(df: Any, *, columns: Optional[Sequence[str]] = None,
                 spec: Optional[HashSpec] = None, name: str = ROW_HASH_COLUMN) -> Any:
    """Add a row-hash column over the given columns, for change detection.

    The row hash lets the curated layer tell whether a row changed (same key,
    different content) without comparing every field. It uses the same
    canonicalisation as the key hash, so it is stable and comparable.

    Args:
        df: The Spark DataFrame.
        columns: The columns to hash. Defaults to all columns of the DataFrame.
        spec: The canonicalisation spec; the default is used if omitted.
        name: The name of the row-hash column to add.

    Returns:
        The DataFrame with the row-hash column added.
    """
    from pyspark.sql import functions as F  # lazy import

    spec = spec or HashSpec()
    cols = list(columns) if columns else list(df.columns)
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit(spec.null_token)) for c in cols
    ]
    combined = F.concat_ws(spec.delimiter, *parts)
    return df.withColumn(name, F.sha2(combined, 256))
