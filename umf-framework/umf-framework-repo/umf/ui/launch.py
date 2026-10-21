"""Launching the notebook UI, with graceful fallbacks.

The UI opens inside the notebook on whatever platform the framework is running
on. Because the available widget technology differs by platform, the launcher
picks the best mode it can and falls back cleanly:

    1. a rich UI built on ipywidgets, which works in most notebooks;
    2. a platform's native widgets, where ipywidgets is unavailable (added per
       platform in a later part); and
    3. a JSON-config object, which needs nothing beyond Python and so works
       everywhere, including a plain script or a test.

The JSON-config fallback is a real, usable path, not a stub: it validates a
config, lists the problems and the defaults to confirm, and runs. Everything it
does is pure Python over the model and formatting helpers and the runner, so it
is tested here. The rich UI is a thin layer that collects the same values and
calls the same helpers.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from umf.core.config_loader import validate_config
from umf.core.logging_setup import get_logger
from umf.ui.formatting import (
    format_recon,
    format_run_summary,
    format_validation,
)

_log = get_logger("ui.launch")


def ipywidgets_available() -> bool:
    """Return whether ipywidgets can be imported in this environment.

    Returns:
        True if the rich UI can be built, False if the launcher should fall
        back.
    """
    try:
        import ipywidgets  # noqa: F401  (probe import)

        return True
    except Exception:
        return False


class JsonConfigUI:
    """A minimal, dependency-free UI driven by a JSON config.

    This is the universal fallback and a perfectly usable interface on its own:
    give it a config, it validates and reports; call :meth:`run` and it executes
    through the runner. It holds no Spark or widget dependency, so it works in
    any notebook, script or test.

    Attributes:
        framework_version: The framework version, passed to runs.
    """

    def __init__(self, *, framework_version: str) -> None:
        """Create the fallback UI.

        Args:
            framework_version: The framework build identifier.
        """
        self.framework_version = framework_version
        self._config: Dict[str, Any] = {}

    def set_config(self, config: Dict[str, Any]) -> str:
        """Set and validate a config, returning a readable validation report.

        Args:
            config: The config dictionary.

        Returns:
            A readable validation report (problems to fix, defaults to confirm).
        """
        self._config = dict(config)
        result = validate_config(config)
        return format_validation(result.errors, result.confirmations)

    def run(
        self,
        *,
        executor_registry: Optional[Any] = None,
        control_plane: Optional[Any] = None,
        spark: Any = None,
        trigger: str = "manual",
    ) -> Dict[str, Any]:
        """Run the current config through the runner and return the summary.

        Args:
            executor_registry: The executor registry; a dry-run registry is used
                if omitted (validate the orchestration without processing data).
            control_plane: The control plane for logging and state.
            spark: The Spark session.
            trigger: What started the run.

        Returns:
            The run summary dictionary.
        """
        # Imported here to keep the UI import light and avoid any import cycle.
        from umf.engine.runner import run

        return run(
            self._config,
            framework_version=self.framework_version,
            executor_registry=executor_registry,
            control_plane=control_plane,
            spark=spark,
            trigger=trigger,
        )

    def describe_run(self, summary: Dict[str, Any]) -> str:
        """Return a readable description of a run summary.

        Args:
            summary: The run summary.

        Returns:
            A readable report.
        """
        return format_run_summary(summary)


def launch_ui(*, framework_version: str, prefer_rich: bool = True) -> Any:
    """Launch the best available UI and return a handle to it.

    Args:
        framework_version: The framework build identifier.
        prefer_rich: Whether to use the rich ipywidgets UI when available.
            Set to ``False`` to force the JSON-config fallback (useful in tests
            and scripts).

    Returns:
        A UI handle: the rich notebook UI when ipywidgets is available and
        preferred, otherwise a :class:`JsonConfigUI`.
    """
    if prefer_rich and ipywidgets_available():
        try:
            from umf.ui.notebook_ui import NotebookUI  # lazy import

            _log.info("launching rich notebook UI")
            return NotebookUI(framework_version=framework_version)
        except Exception as exc:  # fall back rather than fail
            _log.warning("rich UI unavailable, falling back to JSON config UI",
                         extra={"error": repr(exc)})

    _log.info("using JSON config UI (no widgets required)")
    return JsonConfigUI(framework_version=framework_version)
