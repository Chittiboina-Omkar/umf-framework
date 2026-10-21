"""The rich notebook UI, built on ipywidgets.

This is the level-1 UI: a tabbed form that collects the config field by field,
validates it, and runs it, showing the run summary, reconciliation and logs. It
is a thin layer over the pure model and formatting helpers; it holds no logic of
its own beyond turning widget values into the flat field map the model expects.

ipywidgets is imported lazily inside the methods, so this module imports without
it; the launcher only constructs this class when ipywidgets is available, and
falls back otherwise. Because the UI depends on a live notebook, it is validated
there rather than in a headless test, while everything it relies on is tested.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from umf.core.config_loader import validate_config
from umf.core.constants import Step
from umf.core.logging_setup import get_logger
from umf.ui.formatting import format_recon, format_run_summary, format_validation
from umf.ui.model import UiField, build_config, relevant_sections, ui_fields

_log = get_logger("ui.notebook_ui")


class NotebookUI:
    """A tabbed ipywidgets UI for building and running a config.

    Attributes:
        framework_version: The framework build identifier.
    """

    def __init__(self, *, framework_version: str) -> None:
        """Create the UI and its widgets.

        Args:
            framework_version: The framework build identifier.

        Raises:
            ImportError: If ipywidgets is not installed (the launcher guards
                against this, but constructing directly without it fails
                clearly).
        """
        import ipywidgets as widgets  # lazy import

        self._widgets = widgets
        self.framework_version = framework_version
        self._inputs: Dict[str, Any] = {}
        self._fields: List[UiField] = ui_fields()
        self._output = widgets.Output()
        self._container = self._build()

    def _build(self) -> Any:
        """Build the tabbed form from the UI field model.

        Returns:
            The top-level widget container to display.
        """
        widgets = self._widgets

        # Group fields by section into separate tab pages.
        sections: Dict[str, List[UiField]] = {}
        for f in self._fields:
            sections.setdefault(f.section, []).append(f)

        pages = []
        titles = []
        for section, fields in sections.items():
            rows = []
            for f in fields:
                rows.append(self._field_widget(f))
            pages.append(widgets.VBox(rows))
            titles.append(section)

        tabs = widgets.Tab(children=pages)
        for i, title in enumerate(titles):
            tabs.set_title(i, title)

        validate_btn = widgets.Button(description="Validate", button_style="info")
        run_btn = widgets.Button(description="Run", button_style="success")
        validate_btn.on_click(self._on_validate)
        run_btn.on_click(self._on_run)

        return widgets.VBox([tabs, widgets.HBox([validate_btn, run_btn]), self._output])

    def _field_widget(self, f: UiField) -> Any:
        """Build a labelled input widget for one field.

        Args:
            f: The UI field.

        Returns:
            A widget row (label plus input), with the input registered for
            later collection.
        """
        widgets = self._widgets
        label = f.label + (" *" if f.mandatory else "")
        if f.allowed:
            widget = widgets.Dropdown(options=[""] + list(f.allowed), value="")
        else:
            placeholder = "" if f.example is None else f"e.g. {f.example}"
            widget = widgets.Text(value="", placeholder=placeholder)
        self._inputs[f.path] = widget
        return widgets.HBox(
            [widgets.Label(label, layout=widgets.Layout(width="220px")), widget]
        )

    def _collect(self) -> Dict[str, Any]:
        """Collect the current field values into a flat path->value map.

        Returns:
            The flat values, with blanks omitted.
        """
        values: Dict[str, Any] = {}
        for path, widget in self._inputs.items():
            value = widget.value
            if value not in (None, ""):
                values[path] = value
        return values

    def _on_validate(self, _button: Any) -> None:
        """Validate the current form and show the result."""
        config = build_config(self._collect())
        result = validate_config(config)
        with self._output:
            self._output.clear_output()
            print(format_validation(result.errors, result.confirmations))

    def _on_run(self, _button: Any) -> None:
        """Run the current form's config and show the summary.

        The run uses a dry-run registry by default here; a caller wiring the
        real executors passes them in via :meth:`run_with`.
        """
        from umf.engine.runner import run

        config = build_config(self._collect())
        with self._output:
            self._output.clear_output()
            try:
                summary = run(config, framework_version=self.framework_version)
                print(format_run_summary(summary))
            except Exception as exc:  # show the error in the UI, do not crash
                print(f"Run failed: {exc}")

    def display(self) -> None:
        """Display the UI in the notebook."""
        from IPython.display import display  # lazy import

        display(self._container)
