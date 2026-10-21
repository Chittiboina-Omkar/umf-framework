"""Step executors: the real work behind each pipeline step.

Each executor wires the earlier parts (source reader, type mapping, audit
columns, transforms, keys, data quality, reconciliation and the atomic commit)
into one coherent step. The runner drives them through the executor interface,
so a run is just the ordered execution of these steps across the DAG.

Part 8 implements:
    FilesToRawExecutor     files -> raw
    RawToCuratedExecutor   raw -> curated

and :func:`build_executor_registry`, which assembles them for a run. The
pure decision helpers in :mod:`umf.steps.common` are tested here; the Spark
data operations are validated in your environment.
"""

from umf.steps.common import build_recon_config, business_columns, plan_or_block
from umf.steps.files_to_raw import FilesToRawExecutor
from umf.steps.raw_to_curated import RawToCuratedExecutor
from umf.steps.registry import build_executor_registry, default_file_reader_factory

__all__ = [
    "FilesToRawExecutor",
    "RawToCuratedExecutor",
    "build_executor_registry",
    "default_file_reader_factory",
    "plan_or_block",
    "business_columns",
    "build_recon_config",
]
