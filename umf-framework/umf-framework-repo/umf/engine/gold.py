"""Gold logic: business rules that build the consumption layer.

Gold tables are built from business logic the user supplies, either as SQL or as
a structured definition the framework turns into SQL. This module models that
logic, validates it, and orders a set of gold tables so each is built after the
tables it depends on.

Three responsibilities, all pure and tested:

    * build_sql       turn a structured definition into a SELECT, or pass SQL
                      through;
    * validate_sql    confirm the logic is a read-only query, refusing anything
                      that could modify or drop data (a gold definition must
                      never contain DROP, DELETE, INSERT, and so on); and
    * order_gold      sort gold tables into dependency order, detecting cycles.

Validating that gold logic is read-only matters because the logic is executed
against the lakehouse; a stray or malicious ``DROP TABLE`` in a gold definition
could destroy data, so only ``SELECT`` / ``WITH`` queries are allowed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from umf.core.constants import _StrEnum
from umf.core.exceptions import ConfigError


class GoldLogicType(_StrEnum):
    """How a gold table's logic is expressed."""

    SQL = "SQL"              # a SQL SELECT the user wrote
    STRUCTURED = "STRUCTURED"  # a structured definition the framework compiles


class GoldRefresh(_StrEnum):
    """How a gold table is refreshed."""

    FULL = "FULL"            # rebuild the whole table each run
    INCREMENTAL = "INCREMENTAL"  # merge only new / changed rows


@dataclass(frozen=True)
class GoldLogic:
    """One gold table's build logic.

    Attributes:
        name: The gold table's name (used for dependency ordering).
        logic_type: Whether the logic is raw SQL or a structured definition.
        sql: The SQL SELECT, when ``logic_type`` is SQL.
        definition: The structured definition, when ``logic_type`` is
            STRUCTURED (see :func:`build_sql` for its shape).
        dependencies: The names of tables this gold table reads. Dependencies
            that are themselves gold tables determine build order.
        refresh: How the table is refreshed.
    """

    name: str
    logic_type: GoldLogicType
    sql: Optional[str] = None
    definition: Optional[dict] = None
    dependencies: Sequence[str] = field(default_factory=tuple)
    refresh: GoldRefresh = GoldRefresh.FULL


# SQL statement keywords that must never appear in gold logic, because gold logic
# is a read-only query. Matched as whole words, case-insensitively.
_FORBIDDEN = [
    "drop", "delete", "insert", "update", "truncate", "alter", "create",
    "merge", "grant", "revoke", "replace", "call", "execute",
]
_FORBIDDEN_RE = re.compile(r"\b(" + "|".join(_FORBIDDEN) + r")\b", re.IGNORECASE)
_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)


def validate_sql(sql: str) -> None:
    """Confirm a gold SQL statement is a read-only query.

    The check strips comments, then requires the statement to begin with
    ``SELECT`` or ``WITH`` and to contain none of the forbidden data-modifying
    or schema-changing keywords. This is a guard against a gold definition
    accidentally or maliciously changing or dropping data.

    Args:
        sql: The SQL to validate.

    Raises:
        ConfigError: If the SQL is empty, does not start with SELECT / WITH, or
            contains a forbidden keyword.
    """
    if not sql or not sql.strip():
        raise ConfigError("Gold SQL is empty.")

    stripped = _COMMENT_RE.sub(" ", sql).strip()
    first = stripped.split(None, 1)[0].lower() if stripped else ""
    if first not in ("select", "with"):
        raise ConfigError(
            "Gold SQL must be a read-only query starting with SELECT or WITH; "
            f"it starts with '{first}'."
        )
    match = _FORBIDDEN_RE.search(stripped)
    if match:
        raise ConfigError(
            f"Gold SQL contains the forbidden keyword '{match.group(1)}'. Gold "
            "logic must be read-only (SELECT / WITH only)."
        )


def build_sql(logic: GoldLogic) -> str:
    """Return the SQL for a gold logic, compiling a structured definition.

    For SQL logic the statement is validated and returned. For a structured
    definition the SQL is assembled from its parts and then validated, so both
    paths are guaranteed read-only.

    The structured definition shape (all parts optional except ``from``)::

        {
          "select": ["a.id", "a.name"],
          "from": "curated.accounts a",
          "joins": [{"type": "LEFT", "table": "curated.regions r",
                      "on": "a.region_id = r.id"}],
          "where": "a.active = true",
          "group_by": ["a.region_id"],
          "aggregates": [{"expr": "SUM(a.balance)", "as": "total_balance"}],
          "having": "SUM(a.balance) > 0",
          "order_by": ["a.region_id"],
          "limit": 1000
        }

    Args:
        logic: The gold logic.

    Returns:
        The SQL SELECT statement.

    Raises:
        ConfigError: If the logic is incomplete or the resulting SQL is not a
            valid read-only query.
    """
    if logic.logic_type == GoldLogicType.SQL:
        if not logic.sql:
            raise ConfigError(f"Gold '{logic.name}' is SQL type but has no sql.")
        validate_sql(logic.sql)
        return logic.sql.strip()

    definition = logic.definition or {}
    if "from" not in definition:
        raise ConfigError(
            f"Gold '{logic.name}' structured definition needs a 'from'."
        )

    # SELECT list: plain columns plus any aggregate expressions with aliases.
    select_items: List[str] = list(definition.get("select", []))
    for agg in definition.get("aggregates", []):
        alias = agg.get("as")
        expr = agg["expr"]
        select_items.append(f"{expr} AS {alias}" if alias else expr)
    select_clause = ", ".join(select_items) if select_items else "*"

    parts = [f"SELECT {select_clause}", f"FROM {definition['from']}"]

    for join in definition.get("joins", []):
        join_type = join.get("type", "INNER").upper()
        parts.append(f"{join_type} JOIN {join['table']} ON {join['on']}")

    if definition.get("where"):
        parts.append(f"WHERE {definition['where']}")
    if definition.get("group_by"):
        parts.append("GROUP BY " + ", ".join(definition["group_by"]))
    if definition.get("having"):
        parts.append(f"HAVING {definition['having']}")
    if definition.get("order_by"):
        parts.append("ORDER BY " + ", ".join(definition["order_by"]))
    if definition.get("limit") is not None:
        parts.append(f"LIMIT {int(definition['limit'])}")

    sql = " ".join(parts)
    validate_sql(sql)
    return sql


def order_gold(logics: Sequence[GoldLogic]) -> List[GoldLogic]:
    """Order gold tables so each is built after its gold dependencies.

    Only dependencies that are themselves gold tables in this set affect the
    order; dependencies on curated tables are upstream and already built. A
    dependency cycle among gold tables is an error.

    Args:
        logics: The gold logics to order.

    Returns:
        The logics in a valid build order.

    Raises:
        ConfigError: If there is a dependency cycle among the gold tables.
    """
    by_name = {g.name: g for g in logics}
    names = set(by_name)

    # Count only gold-to-gold dependencies.
    indegree: Dict[str, int] = {g.name: 0 for g in logics}
    for g in logics:
        for dep in g.dependencies:
            if dep in names:
                indegree[g.name] += 1

    ready = [g.name for g in logics if indegree[g.name] == 0]
    order: List[GoldLogic] = []
    while ready:
        name = ready.pop(0)
        order.append(by_name[name])
        for g in logics:
            if name in g.dependencies and g.name in indegree:
                indegree[g.name] -= 1
                if indegree[g.name] == 0:
                    ready.append(g.name)

    if len(order) != len(logics):
        raise ConfigError(
            "Dependency cycle detected among gold tables; cannot order them."
        )
    return order


def build_gold_logics(raw: Sequence[dict]) -> List[GoldLogic]:
    """Build gold logic objects from the config's gold list.

    Args:
        raw: The raw gold definitions from the config.

    Returns:
        The typed :class:`GoldLogic` list.

    Raises:
        ConfigError: If a definition has an invalid type or refresh value.
    """
    out: List[GoldLogic] = []
    for item in raw:
        try:
            logic_type = GoldLogicType.coerce(item.get("logic_type", "SQL"))
            refresh = GoldRefresh.coerce(item.get("refresh", "FULL"))
        except ValueError as exc:
            raise ConfigError(f"Invalid gold definition {item.get('name')}: {exc}") from exc
        out.append(
            GoldLogic(
                name=item["name"],
                logic_type=logic_type,
                sql=item.get("sql"),
                definition=item.get("definition"),
                dependencies=list(item.get("dependencies", [])),
                refresh=refresh,
            )
        )
    return out
