"""Formatting the framework's output for display, as plain text.

The UI and a plain notebook both need readable renderings of what happened: the
validation result, the run summary, the reconciliation checks, the columns that
need a decision, and recent log records. These functions produce plain text, so
they work in any environment (a rich UI can style them further, but the content
is the same) and they are pure and tested.

Nothing here reaches into Spark or a store; it formats data that has already
been produced.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence


def format_validation(errors: Sequence[str], confirmations: Sequence) -> str:
    """Render a validation result: errors to fix and defaults to confirm.

    Args:
        errors: The hard errors that block the run.
        confirmations: The confirmation objects (each with a ``message()``).

    Returns:
        A readable multi-line string.
    """
    lines: List[str] = []
    if errors:
        lines.append(f"{len(errors)} problem(s) to fix:")
        lines.extend(f"  - {e}" for e in errors)
    else:
        lines.append("No blocking problems.")
    if confirmations:
        lines.append("")
        lines.append(f"{len(confirmations)} default(s) to confirm:")
        lines.extend(f"  - {c.message()}" for c in confirmations)
    return "\n".join(lines)


def format_run_summary(summary: Dict[str, Any]) -> str:
    """Render a run summary: status, timing and per-table outcomes.

    Args:
        summary: The dictionary from ``RunContext.summary()``.

    Returns:
        A readable multi-line string.
    """
    lines = [
        f"Run {summary.get('run_id', '')[:12]}  status={summary.get('status')}",
        f"  platform={summary.get('platform')}  environment={summary.get('environment')}",
        f"  framework={summary.get('framework_version')}",
    ]
    outcomes = summary.get("outcomes", [])
    if outcomes:
        lines.append("  outcomes:")
        for o in outcomes:
            step = str(o.get("step", "")).split(".")[-1]
            counts = ""
            if o.get("rows_written") is not None:
                counts = f" rows={o['rows_written']}"
            lines.append(
                f"    [{o.get('status'):>13}] {o.get('table'):<20} {step}{counts}"
            )
    return "\n".join(lines)


def format_recon(results: Sequence[dict]) -> str:
    """Render reconciliation results as an aligned text table.

    Args:
        results: The recon result dictionaries.

    Returns:
        A readable table, with failing checks marked.
    """
    if not results:
        return "No reconciliation checks."
    header = f"{'status':<6} {'check':<28} {'source':>14} {'target':>14} {'diff':>12}"
    lines = [header, "-" * len(header)]
    for r in results:
        mark = "FAIL" if r.get("status") == "fail" else "ok"
        lines.append(
            f"{mark:<6} {str(r.get('check_name',''))[:28]:<28} "
            f"{str(r.get('source_value',''))[:14]:>14} "
            f"{str(r.get('target_value',''))[:14]:>14} "
            f"{str(r.get('difference',''))[:12]:>12}"
        )
    failures = sum(1 for r in results if r.get("status") == "fail")
    lines.append("-" * len(header))
    lines.append(f"{len(results)} checks, {failures} failed")
    return "\n".join(lines)


def format_needs_decision(outcomes: Sequence) -> List[str]:
    """Collect the tables that need a type decision, for highlighting.

    Args:
        outcomes: The step outcomes (objects or dicts with status and detail).

    Returns:
        A list of one-line descriptions, empty if nothing needs a decision.
    """
    lines: List[str] = []
    for o in outcomes:
        status = o.get("status") if isinstance(o, dict) else str(o.status)
        detail = o.get("detail") if isinstance(o, dict) else o.detail
        table = o.get("table") if isinstance(o, dict) else o.table
        if str(status) == "NEEDS_DECISION":
            lines.append(f"{table}: {detail}")
    return lines


def format_log_tail(records: Sequence[dict], *, limit: int = 20) -> str:
    """Render the most recent log records as readable lines.

    Args:
        records: Log record dictionaries (for example from a file sink).
        limit: The maximum number of most-recent records to show.

    Returns:
        A readable multi-line string.
    """
    if not records:
        return "No log records."
    tail = records[-limit:]
    lines = []
    for r in tail:
        ts = r.get("_written_at", r.get("ts", ""))
        status = r.get("status", "")
        name = r.get("table_name") or r.get("step") or r.get("check_name") or ""
        lines.append(f"{str(ts)[:19]}  {str(status):<13} {name}")
    return "\n".join(lines)
