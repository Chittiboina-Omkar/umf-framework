"""The notebook UI: a convenience for building and running a config.

The UI opens in the notebook, collects the config field by field (showing only
the sections relevant to the chosen steps), validates it, runs it, and shows the
run summary, reconciliation and logs. It is a thin layer; all the logic it uses
is the pure model and formatting here, which are tested, plus the runner.

Public surface:
    launch_ui         open the best available UI (rich, or JSON-config fallback)
    JsonConfigUI      the dependency-free fallback, usable anywhere
    ui_fields, build_config, relevant_sections   the pure UI model
    format_*          the pure formatting helpers
"""

from umf.ui.formatting import (
    format_log_tail,
    format_needs_decision,
    format_recon,
    format_run_summary,
    format_validation,
)
from umf.ui.launch import JsonConfigUI, ipywidgets_available, launch_ui
from umf.ui.model import (
    UiField,
    build_config,
    flatten_config,
    relevant_sections,
    ui_fields,
)

__all__ = [
    "launch_ui",
    "JsonConfigUI",
    "ipywidgets_available",
    "UiField",
    "ui_fields",
    "build_config",
    "flatten_config",
    "relevant_sections",
    "format_validation",
    "format_run_summary",
    "format_recon",
    "format_needs_decision",
    "format_log_tail",
]
