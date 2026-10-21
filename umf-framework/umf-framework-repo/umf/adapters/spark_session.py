"""Layer L0 (part): obtaining a Spark session, portably.

Every data step needs a Spark session. On a managed platform (Databricks,
Fabric, Synapse, Glue, EMR, Dataproc) a session already exists and must be
reused; locally, one is created. This module hides that difference behind a
single function, :func:`get_or_create_spark`.

PySpark is imported lazily, inside the function, for two reasons:

    * the rest of the framework's pure-Python modules can be imported and tested
      without PySpark installed, and
    * importing PySpark is comparatively heavy, so we only pay for it when a step
      actually needs Spark.

If PySpark is not available when a session is genuinely required, a clear
:class:`PlatformError` is raised rather than an opaque ``ImportError`` deep in
the call stack.
"""

from __future__ import annotations

from typing import Any, Optional

from umf.core.exceptions import PlatformError
from umf.core.logging_setup import get_logger

_log = get_logger("adapters.spark_session")

# Cache the resolved session so repeated calls within a run reuse it. A Spark
# session is a heavyweight, process-wide object; creating more than one is
# wasteful and can cause confusing behaviour.
_SESSION: Optional[Any] = None


def get_or_create_spark(app_name: str = "umf") -> Any:
    """Return the active Spark session, creating a local one if none exists.

    On a managed platform the active session is returned (so the framework runs
    inside the platform's own configured Spark). With no active session, a local
    session is created, which is what happens on a laptop or in a test harness
    that has PySpark installed.

    Args:
        app_name: The Spark application name used only when creating a new local
            session. Ignored when an active session already exists.

    Returns:
        The active or newly created Spark session (a ``pyspark.sql.SparkSession``;
        typed as ``Any`` here so this module imports without PySpark).

    Raises:
        PlatformError: If PySpark is not installed, with a clear message, because
            a data step genuinely needs Spark and cannot proceed without it.
    """
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    try:
        from pyspark.sql import SparkSession  # imported lazily on purpose
    except ImportError as exc:
        raise PlatformError(
            "PySpark is required to run data steps but is not installed in this "
            "environment. Install it, or run only the pure-Python parts of the "
            "framework (config validation, type planning).",
            cause=exc,
        ) from exc

    # ``getActiveSession`` returns the platform's session when one exists.
    active = SparkSession.getActiveSession()
    if active is not None:
        _log.info("using active Spark session")
        _SESSION = active
        return _SESSION

    _log.info("creating a local Spark session", extra={"app_name": app_name})
    _SESSION = SparkSession.builder.appName(app_name).getOrCreate()
    return _SESSION


def set_session_for_testing(session: Optional[Any]) -> None:
    """Inject or clear the cached Spark session, for tests.

    Lets a test supply a lightweight fake session, or reset the cache between
    tests, without creating a real Spark session.

    Args:
        session: The session to cache, or ``None`` to clear the cache.
    """
    global _SESSION
    _SESSION = session
