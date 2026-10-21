"""A dependency graph of work, so steps run in the right order with isolation.

A run does work for one or more tables, each going through one or more steps.
Those units form a graph: within a table the steps have a natural order (raw
before curated before gold), and across tables there can be extra dependencies
(a gold table that joins several curated tables must wait for all of them).

Representing the work as a directed acyclic graph (DAG) gives two things the
design calls for:

    * correct ordering, by running nodes in topological order, and
    * isolation, because when a node fails the runner can skip only the nodes
      that depend on it and let every independent node proceed. One table's
      failure never stops an unrelated table.

This module builds the DAG and produces the execution order; the runner walks
that order and applies the isolation rule. Everything here is pure Python and
tested.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence

from umf.core.constants import Step
from umf.core.exceptions import ConfigError


def node_id(step: Step, table: str) -> str:
    """Return the stable identifier for a (step, table) node.

    Args:
        step: The step.
        table: The table name.

    Returns:
        The node id, ``"<step>:<table>"``.
    """
    return f"{step}:{table}"


@dataclass(frozen=True)
class Node:
    """One unit of work: a single step for a single table.

    Attributes:
        id: The stable node identifier.
        step: The step to run.
        table: The table the step runs for.
        depends_on: The ids of nodes that must finish successfully first.
    """

    id: str
    step: Step
    table: str
    depends_on: Sequence[str] = field(default_factory=tuple)


@dataclass
class Dag:
    """A directed acyclic graph of work nodes.

    Attributes:
        nodes: A mapping of node id to :class:`Node`.
    """

    nodes: Dict[str, Node]

    def topo_order(self) -> List[Node]:
        """Return the nodes in a valid execution order.

        Uses Kahn's algorithm. A stable order is produced (nodes with no
        remaining dependencies are taken in insertion order) so runs are
        reproducible.

        Returns:
            The nodes in dependency order.

        Raises:
            ConfigError: If the graph contains a cycle (which would mean a
                contradictory set of dependencies, for example gold A depending
                on gold B and vice versa).
        """
        # Count incoming edges for each node.
        indegree: Dict[str, int] = {nid: 0 for nid in self.nodes}
        for node in self.nodes.values():
            for dep in node.depends_on:
                if dep not in self.nodes:
                    raise ConfigError(
                        f"Node '{node.id}' depends on unknown node '{dep}'."
                    )
                indegree[node.id] += 1

        # Start with nodes that have no dependencies, in insertion order.
        ready = [nid for nid in self.nodes if indegree[nid] == 0]
        order: List[Node] = []

        while ready:
            nid = ready.pop(0)
            order.append(self.nodes[nid])
            # Removing this node reduces the indegree of its dependents.
            for other in self.nodes.values():
                if nid in other.depends_on:
                    indegree[other.id] -= 1
                    if indegree[other.id] == 0:
                        ready.append(other.id)

        if len(order) != len(self.nodes):
            raise ConfigError(
                "Dependency cycle detected among steps; the pipeline cannot be "
                "ordered. Check cross-table dependencies for a loop."
            )
        return order


def build_dag(
    *,
    steps: Sequence[Step],
    tables: Sequence[str],
    cross_table_deps: Dict[str, List[str]] = None,  # type: ignore[assignment]
) -> Dag:
    """Build the work DAG for a set of steps and tables.

    Within each table, the steps are chained in the given order, so each step
    depends on the previous one for the same table (raw before curated before
    gold). Cross-table dependencies can be added for nodes that must wait on
    nodes of other tables.

    Args:
        steps: The ordered steps to run (from the decision rules).
        tables: The tables to run them for.
        cross_table_deps: Optional extra dependencies, mapping a node id to the
            list of node ids it must wait for. Node ids are built with
            :func:`node_id`.

    Returns:
        The built :class:`Dag`.
    """
    cross = cross_table_deps or {}
    nodes: Dict[str, Node] = {}

    for table in tables:
        previous_id = None
        for step in steps:
            nid = node_id(step, table)
            deps: List[str] = []
            if previous_id is not None:
                deps.append(previous_id)
            # Add any configured cross-table dependencies for this node.
            deps.extend(cross.get(nid, []))
            nodes[nid] = Node(id=nid, step=step, table=table, depends_on=tuple(deps))
            previous_id = nid

    return Dag(nodes=nodes)
