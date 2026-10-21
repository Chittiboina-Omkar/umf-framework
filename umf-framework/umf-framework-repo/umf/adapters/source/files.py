"""A source reader for files: CSV, JSON, Parquet, ORC, Avro.

This reader covers the "files" source family. The important subtlety it handles
is where the schema comes from, because that differs sharply by format:

    * Parquet, ORC and Avro carry their own schema inside the file, so the
      schema is embedded and trustworthy (though character length is not stored).
    * CSV and JSON carry no type information. Their schema must come from a
      user-supplied contract, a sidecar schema file, or be inferred by sampling.
      Inference is a best guess; the reader records the origin as ``INFERRED`` so
      that strict mode can refuse to proceed on a guess.

The reader exposes :meth:`describe_schema` (used by the type engine before any
data is read) and :meth:`read` (the Spark DataFrame). Spark and the sidecar
parsing are imported lazily, so this module imports without PySpark and the
contract-driven schema path is testable with plain Python.

The per-format read options (delimiter, header, encoding, multiline, and so on)
are passed through from the config, so the many CSV quirks the design calls out
are handled by configuration rather than code changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from umf.adapters.source.base import SchemaOrigin, SourceReader, SourceSchema
from umf.core.constants import _StrEnum
from umf.core.exceptions import ConfigError, SchemaError
from umf.core.logging_setup import get_logger
from umf.typing.source_column import SourceColumn

_log = get_logger("adapters.source.files")


class FileFormat(_StrEnum):
    """Supported file formats."""

    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "PARQUET"
    ORC = "ORC"
    AVRO = "AVRO"


# Formats that carry their own schema inside the file.
_SELF_DESCRIBING = {FileFormat.PARQUET, FileFormat.ORC, FileFormat.AVRO}


@dataclass
class FileSourceConfig:
    """Configuration for the file reader.

    Attributes:
        path: The path or glob to read (on any storage the platform can reach).
        file_format: The file format.
        options: Format-specific read options passed straight to Spark (for
            example ``{"header": "true", "delimiter": "|", "encoding": "UTF-8",
            "multiLine": "true"}`` for CSV).
        contract: An optional, explicit list of :class:`SourceColumn` describing
            the schema. Required for CSV / JSON in strict mode; when provided,
            it is authoritative and no inference happens.
        strict: When true, an inferred schema is rejected for formats that carry
            no type information, forcing a contract or sidecar. This protects
            production from silently trusting guessed types.
    """

    path: str
    file_format: FileFormat
    options: Dict[str, str] = field(default_factory=dict)
    contract: Optional[Sequence[SourceColumn]] = None
    strict: bool = True


class FileSourceReader(SourceReader):
    """Reads files and describes their schema.

    Attributes:
        config: The file reader configuration.
    """

    source_name = "files"

    def __init__(self, config: FileSourceConfig) -> None:
        """Create the reader.

        Args:
            config: The file reader configuration.
        """
        self.config = config

    def describe_schema(self) -> SourceSchema:
        """Describe the schema according to the format and configuration.

        Decision order:
            1. If a contract is supplied, use it (origin ``CONTRACT``). This is
               authoritative and requires no Spark.
            2. If the format is self-describing (Parquet / ORC / Avro), read the
               schema from the file (origin ``EMBEDDED``) via Spark.
            3. Otherwise (CSV / JSON) the schema must be inferred (origin
               ``INFERRED``). In strict mode this is refused, because trusting
               guessed types in production is unsafe.

        Returns:
            The :class:`SourceSchema`.

        Raises:
            SchemaError: If the schema would have to be inferred but strict mode
                forbids it and no contract was supplied.
        """
        cfg = self.config

        if cfg.contract:
            _log.info(
                "using supplied schema contract",
                extra={"path": cfg.path, "columns": len(cfg.contract)},
            )
            return SourceSchema(
                columns=list(cfg.contract),
                origin=SchemaOrigin.CONTRACT,
                note="schema from user-supplied contract",
            )

        if cfg.file_format in _SELF_DESCRIBING:
            columns = self._read_embedded_schema()
            return SourceSchema(
                columns=columns,
                origin=SchemaOrigin.EMBEDDED,
                note=f"schema embedded in {cfg.file_format} file",
            )

        # CSV / JSON with no contract: would have to infer.
        if cfg.strict:
            raise SchemaError(
                f"{cfg.file_format} has no type information and strict mode is "
                "on, so a schema contract (or sidecar) is required. Provide one, "
                "or turn strict mode off to allow inference.",
                context={"path": cfg.path, "format": str(cfg.file_format)},
            )

        columns = self._infer_schema()
        return SourceSchema(
            columns=columns,
            origin=SchemaOrigin.INFERRED,
            note="schema inferred by sampling (types are a best guess)",
        )

    def read(self) -> Any:
        """Read the files into a Spark DataFrame.

        Returns:
            A ``pyspark.sql.DataFrame`` for the configured path and format.

        Raises:
            PlatformError: If PySpark is unavailable (raised by the session
                provider).
        """
        spark = self._spark()
        reader = spark.read.format(self._spark_format())
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        return reader.load(self.config.path)

    # --- internals -------------------------------------------------------- #

    def _spark(self) -> Any:
        """Return a Spark session via the L0 provider (lazy import)."""
        from umf.adapters.spark_session import get_or_create_spark

        return get_or_create_spark()

    def _spark_format(self) -> str:
        """Return the Spark data-source short name for the configured format.

        Returns:
            The lower-case Spark format name (for example ``"csv"``).
        """
        return str(self.config.file_format).lower()

    def _read_embedded_schema(self) -> List[SourceColumn]:
        """Read the schema of a self-describing file via Spark, without data.

        Spark can read the schema of Parquet / ORC / Avro cheaply (it does not
        scan the data). The Spark types are translated into
        :class:`SourceColumn` using their native type names, so the same type
        engine path is used as for any other source.

        Returns:
            The list of source columns.
        """
        spark = self._spark()
        reader = spark.read.format(self._spark_format())
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        # ``.load(path).schema`` reads only metadata for these formats.
        schema = reader.load(self.config.path).schema
        return [self._field_to_column(i, f) for i, f in enumerate(schema.fields)]

    def _infer_schema(self) -> List[SourceColumn]:
        """Infer a CSV / JSON schema by letting Spark sample the data.

        Returns:
            The list of source columns, with types as Spark inferred them. These
            are a best guess and the origin is recorded as ``INFERRED`` by the
            caller.
        """
        spark = self._spark()
        reader = spark.read.format(self._spark_format()).option("inferSchema", "true")
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        schema = reader.load(self.config.path).schema
        return [self._field_to_column(i, f) for i, f in enumerate(schema.fields)]

    @staticmethod
    def _field_to_column(ordinal: int, field_obj: Any) -> SourceColumn:
        """Convert one Spark ``StructField`` into a :class:`SourceColumn`.

        The Spark type is recorded by its ``simpleString`` (for example
        ``"decimal(10,2)"``), and precision / scale are extracted when the type
        is a decimal so the type engine can map it exactly.

        Args:
            ordinal: The column position.
            field_obj: A ``pyspark.sql.types.StructField``.

        Returns:
            The corresponding :class:`SourceColumn`.
        """
        # Imported lazily so this static helper does not force a PySpark import
        # at module load; it is only called from the Spark-backed paths.
        from pyspark.sql.types import DecimalType  # local import on purpose

        data_type = field_obj.dataType
        native = data_type.simpleString()
        precision = getattr(data_type, "precision", None) if isinstance(
            data_type, DecimalType
        ) else None
        scale = getattr(data_type, "scale", None) if isinstance(
            data_type, DecimalType
        ) else None
        return SourceColumn(
            name=field_obj.name,
            native_type=native,
            precision=precision,
            scale=scale,
            nullable=bool(field_obj.nullable),
            ordinal=ordinal,
        )
