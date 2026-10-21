"""Source readers (layer L1): describe a source's schema and read its data.

Each reader handles one source family and produces the same two things: a
described schema (as source columns, used by the type engine before any data is
read) and the data as a Spark DataFrame. Readers import Spark lazily so this
package is importable without PySpark.

Part 3 ships the file reader. Database and warehouse readers are added later,
implementing the same :class:`~umf.adapters.source.base.SourceReader` interface.
"""

from umf.adapters.source.base import (
    SchemaOrigin,
    SourceReader,
    SourceSchema,
)
from umf.adapters.source.files import (
    FileFormat,
    FileSourceConfig,
    FileSourceReader,
)

__all__ = [
    "SourceReader",
    "SourceSchema",
    "SchemaOrigin",
    "FileFormat",
    "FileSourceConfig",
    "FileSourceReader",
]
