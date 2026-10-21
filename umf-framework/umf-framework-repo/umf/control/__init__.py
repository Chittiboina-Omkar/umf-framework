"""Control and log plane: durable state and the audit trail.

This sub-package holds everything to do with the framework's operational
tables and the services that read and write them:

    column_types     engine-neutral column types for portable table definitions
    table_defs       declarative definitions of every control and log table
    records          typed builders that produce table-shaped log records
    log_sink         where structured records are persisted (file / table / ...)
    watermark_store  reading and advancing watermarks safely
    config_store     versioned, audited config storage (replaces Git PR history)
    control_plane    one facade wiring the sink, watermarks and config store

The portable defaults here (JSON Lines files for logs, JSON files for state)
depend only on the standard library, so they run anywhere and in tests. The
table-backed implementations that write to Delta or Iceberg on a platform are
added in a later part and implement the same interfaces.
"""

from umf.control.control_plane import ControlPlane
from umf.control.log_sink import (
    ConsoleSink,
    JsonlFileSink,
    LogSink,
    MultiSink,
    NullSink,
)
from umf.control.config_store import ConfigStore, ConfigVersion, FileConfigStore
from umf.control.watermark_store import (
    FileWatermarkStore,
    InMemoryWatermarkStore,
    WatermarkChange,
    WatermarkStore,
)

__all__ = [
    "ControlPlane",
    "LogSink",
    "NullSink",
    "ConsoleSink",
    "JsonlFileSink",
    "MultiSink",
    "WatermarkStore",
    "InMemoryWatermarkStore",
    "FileWatermarkStore",
    "WatermarkChange",
    "ConfigStore",
    "FileConfigStore",
    "ConfigVersion",
]
