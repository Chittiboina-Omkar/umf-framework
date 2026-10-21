"""Best-effort detection of the compute platform the framework runs on.

Why this module exists and why it is careful:
    There is no single, official API that tells you which Spark platform you are
    on. Each vendor exposes a different signal:

        * Databricks sets the environment variable ``DATABRICKS_RUNTIME_VERSION``
          (confirmed against Databricks' own tooling). It can be absent on
          clusters built from a custom Docker image, so we also look at a
          Databricks-specific Spark configuration key as a fallback.
        * Microsoft Fabric exposes a ``notebookutils`` module and
          Fabric / Synapse specific entries in ``sys.modules`` and Spark conf.
        * AWS Glue ships an ``awsglue`` Python module and passes a
          ``--JOB_NAME`` argument.
        * AWS EMR writes a job-flow descriptor file on the cluster and sets
          EMR-specific Spark configuration.
        * GCP Dataproc exposes Dataproc-specific environment variables and
          Spark configuration.
        * Azure Synapse Spark exposes Synapse-specific Spark configuration.

    Because every signal can change over time or be missing in edge cases
    (custom images, serverless variants), this module:

        1. checks several independent signals per platform,
        2. never crashes if a signal source is unavailable, and
        3. is always overridable by an explicit ``platform`` value in the config.

    The explicit config value wins. Detection is only used when the user leaves
    the platform as ``AUTO``. Each concrete platform must still be validated end
    to end during platform certification testing before you rely on it in
    production; detection identifying a platform is necessary but not sufficient.

This module imports nothing beyond the standard library, and treats PySpark as
optional, so it is safe to import and test anywhere.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Callable, List, Optional, Tuple

from umf.core.constants import Platform


# A detector is a function returning True when its platform's signals are
# present. Each is wrapped so that any unexpected error is swallowed and treated
# as "signal not present"; detection must never be the thing that breaks a run.
Detector = Callable[[Optional[Any]], bool]


def _safe(detector: Detector, spark: Optional[Any]) -> bool:
    """Run a detector, converting any error into a ``False`` result.

    Args:
        detector: The detection function to call.
        spark: The active Spark session, or ``None`` if not available.

    Returns:
        The detector's boolean result, or ``False`` if it raised.
    """
    try:
        return bool(detector(spark))
    except Exception:
        # Detection is best effort. A failing probe means "cannot confirm this
        # platform", never a crash. We deliberately do not log here because the
        # logging subsystem may not be initialised this early.
        return False


def _get_spark_conf(spark: Optional[Any], key: str) -> Optional[str]:
    """Read a Spark configuration value without raising if it is absent.

    Args:
        spark: The active Spark session, or ``None``.
        key: The Spark configuration key to read.

    Returns:
        The configuration value as a string, or ``None`` if there is no Spark
        session or the key is not set.
    """
    if spark is None:
        return None
    try:
        # ``get`` with a default avoids an exception when the key is missing.
        return spark.conf.get(key, None)
    except Exception:
        return None


def _any_spark_conf_startswith(spark: Optional[Any], prefix: str) -> bool:
    """Return True if any Spark configuration key starts with ``prefix``.

    Some platforms are best identified by a family of configuration keys (for
    example ``spark.databricks.*``) rather than one specific key.

    Args:
        spark: The active Spark session, or ``None``.
        prefix: The configuration key prefix to look for.

    Returns:
        True if at least one configuration key begins with ``prefix``.
    """
    if spark is None:
        return False
    try:
        # ``getAll`` exists on the underlying SparkConf via the JVM context.
        all_conf = spark.sparkContext.getConf().getAll()
        return any(k.startswith(prefix) for k, _ in all_conf)
    except Exception:
        return False


def _module_present(*names: str) -> bool:
    """Return True if any of the named modules is already imported.

    We check :data:`sys.modules` rather than trying to import the module,
    because importing can have side effects and we only want to know whether the
    platform has already loaded its own module.

    Args:
        *names: Module names to look for.

    Returns:
        True if any of the names is present in ``sys.modules``.
    """
    return any(name in sys.modules for name in names)


# --- individual platform detectors ---------------------------------------- #


def _is_databricks(spark: Optional[Any]) -> bool:
    """Detect Databricks (any cloud).

    Primary signal: the ``DATABRICKS_RUNTIME_VERSION`` environment variable.
    Fallback: Databricks-specific Spark configuration, which is present even on
    some custom-image clusters where the environment variable is missing.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    if _get_spark_conf(spark, "spark.databricks.clusterUsageTags.clusterId"):
        return True
    return _any_spark_conf_startswith(spark, "spark.databricks.")


def _is_fabric(spark: Optional[Any]) -> bool:
    """Detect Microsoft Fabric Spark.

    Signals: the ``notebookutils`` / ``trident`` modules that Fabric injects,
    and Fabric / Trident specific Spark configuration keys.
    """
    if _module_present("notebookutils", "trident", "synapse.ml"):
        return True
    if _any_spark_conf_startswith(spark, "spark.trident."):
        return True
    # Fabric reports this Azure service marker in some runtimes.
    return os.environ.get("AZURE_SERVICE", "").lower().find("fabric") >= 0


def _is_synapse(spark: Optional[Any]) -> bool:
    """Detect Azure Synapse Spark.

    Synapse exposes Synapse-specific Spark configuration and an Azure service
    marker. It is checked after Fabric because the two share some lineage.
    """
    if _any_spark_conf_startswith(spark, "spark.synapse."):
        return True
    marker = os.environ.get("AZURE_SERVICE", "")
    return "arcadia" in marker.lower() or "synapse" in marker.lower()


def _is_glue(spark: Optional[Any]) -> bool:
    """Detect AWS Glue.

    Signals: the ``awsglue`` module that Glue provides, a ``--JOB_NAME``
    argument that Glue passes to the job, or Glue-specific environment.
    """
    if _module_present("awsglue"):
        return True
    if any(arg.startswith("--JOB_NAME") for arg in sys.argv):
        return True
    return bool(os.environ.get("GLUE_VERSION") or os.environ.get("GLUE_PYTHON_VERSION"))


def _is_emr(spark: Optional[Any]) -> bool:
    """Detect AWS EMR.

    Signals: the EMR job-flow descriptor file that EMR writes on the cluster,
    EMR release environment variables, and EMR-specific Spark configuration.
    """
    if os.path.exists("/mnt/var/lib/info/job-flow.json"):
        return True
    if os.environ.get("EMR_RELEASE_LABEL") or os.environ.get("EMR_CLUSTER_ID"):
        return True
    return _any_spark_conf_startswith(spark, "spark.emr.")


def _is_dataproc(spark: Optional[Any]) -> bool:
    """Detect GCP Dataproc.

    Signals: Dataproc environment variables and Dataproc-specific Spark
    configuration. We avoid calling the GCE metadata server here because a
    network probe at import time is both slow and unreliable.
    """
    for key in os.environ:
        if key.startswith("DATAPROC"):
            return True
    if _get_spark_conf(spark, "spark.dataproc.metastore.uri"):
        return True
    return _any_spark_conf_startswith(spark, "spark.dataproc.")


def _is_local(spark: Optional[Any]) -> bool:
    """Detect a local or laptop run.

    A Spark session running in ``local[*]`` master, or no Spark session at all,
    is treated as local. This is the natural fallback for development and unit
    tests.
    """
    if spark is None:
        return True
    master = _get_spark_conf(spark, "spark.master") or ""
    return master.startswith("local")


# Ordered list of (platform, detector). Order matters: more specific platforms
# are checked before more generic ones. For example Fabric and Synapse are
# checked before the generic local fallback, and the cloud platforms are checked
# before ON_PREM, which has no reliable positive signal of its own.
_DETECTORS: List[Tuple[Platform, Detector]] = [
    (Platform.DATABRICKS, _is_databricks),
    (Platform.FABRIC, _is_fabric),
    (Platform.SYNAPSE, _is_synapse),
    (Platform.GLUE, _is_glue),
    (Platform.EMR, _is_emr),
    (Platform.DATAPROC, _is_dataproc),
    (Platform.LOCAL, _is_local),
]


def detect_platform(spark: Optional[Any] = None) -> Platform:
    """Identify the platform the framework is currently running on.

    The function checks each platform's signals in priority order and returns
    the first match. If nothing matches it returns :attr:`Platform.UNKNOWN`,
    which is the signal to the caller that an explicit platform must be supplied
    in the config.

    Args:
        spark: The active Spark session, if one exists. Passing it lets the
            detectors inspect Spark configuration, which is a stronger signal
            than environment variables alone. If ``None``, only non-Spark
            signals are used, which still identifies most platforms and always
            identifies a local run.

    Returns:
        The detected :class:`Platform`. Never raises; detection problems are
        treated as "not that platform".

    Example:
        >>> from umf.core.platform_detect import detect_platform
        >>> detect_platform()  # on a laptop with no Spark session
        <Platform.LOCAL: 'LOCAL'>
    """
    for platform, detector in _DETECTORS:
        if _safe(detector, spark):
            return platform
    return Platform.UNKNOWN


def resolve_platform(configured: Platform, spark: Optional[Any] = None) -> Platform:
    """Resolve the final platform from the configured value and detection.

    The configured value always wins unless it is :attr:`Platform.AUTO`, in
    which case detection is used. This keeps the user in control: an explicit
    value is honoured exactly, and detection is only a convenience.

    Args:
        configured: The platform from the config (often ``AUTO``).
        spark: The active Spark session, if any.

    Returns:
        The resolved platform.

    Raises:
        PlatformError: If the configured value is ``AUTO`` and detection cannot
            identify the platform. The user must then set ``platform``
            explicitly in the config.
    """
    # Imported here rather than at module top to keep the import graph of this
    # low-level module minimal and avoid any risk of a circular import.
    from umf.core.exceptions import PlatformError

    if configured != Platform.AUTO:
        return configured

    detected = detect_platform(spark)
    if detected == Platform.UNKNOWN:
        raise PlatformError(
            "Could not detect the platform automatically. Set 'platform' "
            "explicitly in the config to one of: "
            + ", ".join(
                p.value
                for p in Platform
                if p not in (Platform.AUTO, Platform.UNKNOWN)
            ),
            context={"configured": str(configured)},
        )
    return detected
