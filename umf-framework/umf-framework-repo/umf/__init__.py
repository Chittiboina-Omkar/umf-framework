"""Universal Migration & Data Framework (UMF).

A single, parameter-driven framework that moves data from any supported source
(data warehouse, database, files, object storage, and more) to any supported
target (Delta or Iceberg tables, files, or vectors), across every major cloud
and on-premise Spark platform.

Design goals that every module in this package must respect:
    * Parameter-driven: behaviour is controlled by a JSON config, never by
      editing code for a specific client or table.
    * Safe-fail: anything unknown or risky stops safely, is highlighted, logged
      and alerted. The framework never changes data silently.
    * No partial data: a target is only written atomically, after
      reconciliation passes.
    * Portable: the core depends only on the Python standard library so it can
      run unchanged on Databricks, Microsoft Fabric, AWS Glue / EMR,
      GCP Dataproc, Synapse, on-premise Spark and locally. Platform specific
      behaviour lives behind adapters.

This file only exposes the stable public surface of the framework. Importing
from the sub-modules directly is also supported for advanced use.

Note:
    This is Part 1 (the core foundation). It contains configuration handling,
    validation, platform detection, logging and the run lifecycle. It does not
    yet read or write any data; those steps arrive in later parts and always
    build on the foundation defined here.

You can rename this package. If you do, update ``PACKAGE_NAME`` below and the
distribution name in your packaging metadata; nothing else depends on the name.
"""

from __future__ import annotations

# Semantic version of the framework itself. Every run records this value so you
# always know which framework build produced a given result. Keep it in sync
# with your packaging metadata (for example pyproject.toml).
__version__ = "0.1.0"

# Human readable package name, used in log headers and support bundles.
PACKAGE_NAME = "umf"

# Re-export the most commonly used names so callers can simply write
# ``from umf import load_config, RunContext`` without knowing the layout.
from umf.core.constants import (  # noqa: E402  (re-export after module docstring)
    Environment,
    ErrorCategory,
    LoadType,
    Platform,
    RunStatus,
    Severity,
    Step,
    StepStatus,
)
from umf.core.exceptions import (  # noqa: E402
    ConfigError,
    PlatformError,
    UmfError,
    ValidationError,
)
from umf.core.config_loader import load_config, validate_config  # noqa: E402
from umf.core.platform_detect import detect_platform  # noqa: E402
from umf.core.run_context import RunContext  # noqa: E402
from umf.core.logging_setup import get_logger, init_logging  # noqa: E402

__all__ = [
    "__version__",
    "PACKAGE_NAME",
    # enums
    "Environment",
    "ErrorCategory",
    "LoadType",
    "Platform",
    "RunStatus",
    "Severity",
    "Step",
    "StepStatus",
    # exceptions
    "UmfError",
    "ConfigError",
    "ValidationError",
    "PlatformError",
    # entry points
    "load_config",
    "validate_config",
    "detect_platform",
    "RunContext",
    "get_logger",
    "init_logging",
]
