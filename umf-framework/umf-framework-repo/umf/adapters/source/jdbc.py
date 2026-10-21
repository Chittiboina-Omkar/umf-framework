"""A source reader for warehouses and databases over JDBC.

This reader handles the "DWH / RDBMS" source family. It describes the source
schema and reads the data over a JDBC connection, splitting a large read into
the chunks planned by :mod:`umf.engine.chunking` so the extraction is parallel,
bounded and resumable.

Native types and the right mapper:
    The reader's ``source_name`` is the database family (for example
    ``"oracle"`` or ``"snowflake"``), so the type engine selects that family's
    mapper. The schema is described from the columns the driver reports; for the
    most faithful native precision and scale, a database-specific catalog
    metadata reader can be layered on later, but the JDBC-reported schema already
    preserves decimal precision and scale, which are the facts that matter most.

Credentials:
    The connection properties carry a user and a resolved password. The password
    is resolved from a vault reference by the caller (never stored in config) and
    passed in here only for the moment of the connection.

PySpark is imported lazily, so this module imports without Spark.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from umf.adapters.source.base import SchemaOrigin, SourceReader, SourceSchema
from umf.core.exceptions import ConfigError
from umf.core.logging_setup import get_logger
from umf.engine.chunking import Chunk
from umf.typing.source_column import SourceColumn

_log = get_logger("adapters.source.jdbc")


@dataclass
class JdbcSourceConfig:
    """Configuration for the JDBC reader.

    Attributes:
        url: The JDBC URL.
        driver: The JDBC driver class name.
        dwh_family: The database family (selects the type mapper), for example
            ``"oracle"`` or ``"snowflake"``.
        table: The source table to read (``schema.table``). Either this or
            ``query`` must be set.
        query: A custom SQL query to read instead of a table. Either this or
            ``table`` must be set.
        user: The connection user.
        password: The resolved connection password, supplied for the connection
            only and never logged or stored.
        fetch_size: JDBC fetch size, tuning how many rows the driver pulls per
            round trip.
        chunks: The chunks to read, from the chunking planner. A single chunk
            with an empty predicate means read the whole table.
        extra_options: Any additional JDBC options.
    """

    url: str
    driver: str
    dwh_family: str
    table: Optional[str] = None
    query: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    fetch_size: int = 10_000
    chunks: Sequence[Chunk] = field(default_factory=list)
    extra_options: Dict[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        """Check that enough is set to read.

        Raises:
            ConfigError: If neither a table nor a query is set.
        """
        if not self.table and not self.query:
            raise ConfigError("A JDBC source needs either 'table' or 'query'.")


class JdbcSourceReader(SourceReader):
    """Reads a warehouse / database table over JDBC.

    Attributes:
        config: The JDBC reader configuration.
    """

    def __init__(self, config: JdbcSourceConfig, spark: Any = None) -> None:
        """Create the reader.

        Args:
            config: The JDBC configuration.
            spark: An optional Spark session; obtained lazily if omitted.
        """
        config.validate()
        self.config = config
        self.source_name = config.dwh_family
        self._spark = spark

    def _session(self) -> Any:
        """Return the Spark session, obtaining it lazily."""
        if self._spark is None:
            from umf.adapters.spark_session import get_or_create_spark

            self._spark = get_or_create_spark()
        return self._spark

    def _base_reader(self) -> Any:
        """Build a Spark JDBC reader with the connection options set."""
        spark = self._session()
        reader = (
            spark.read.format("jdbc")
            .option("url", self.config.url)
            .option("driver", self.config.driver)
            .option("fetchsize", str(self.config.fetch_size))
        )
        if self.config.user:
            reader = reader.option("user", self.config.user)
        if self.config.password:
            reader = reader.option("password", self.config.password)
        for key, value in self.config.extra_options.items():
            reader = reader.option(key, value)
        return reader

    def describe_schema(self) -> SourceSchema:
        """Describe the source schema without reading the data.

        The schema is obtained by asking the driver for the result shape of a
        zero-row query, which is cheap. Decimal precision and scale are preserved
        by the JDBC driver and carried into the source columns.

        Returns:
            The :class:`~umf.adapters.source.base.SourceSchema`, with origin
            ``SOURCE_METADATA``.
        """
        from pyspark.sql.types import DecimalType  # lazy import

        subject = self.config.table or f"({self.config.query})"
        zero_rows = f"(SELECT * FROM {subject} WHERE 1=0) umf_schema"
        df = self._base_reader().option("dbtable", zero_rows).load()

        columns: List[SourceColumn] = []
        for i, field_obj in enumerate(df.schema.fields):
            dtype = field_obj.dataType
            precision = getattr(dtype, "precision", None) if isinstance(dtype, DecimalType) else None
            scale = getattr(dtype, "scale", None) if isinstance(dtype, DecimalType) else None
            columns.append(
                SourceColumn(
                    name=field_obj.name,
                    native_type=dtype.simpleString(),
                    precision=precision,
                    scale=scale,
                    nullable=bool(field_obj.nullable),
                    ordinal=i,
                )
            )
        return SourceSchema(columns=columns, origin=SchemaOrigin.SOURCE_METADATA)

    def read(self) -> Any:
        """Read the data over JDBC, using the planned chunks for parallelism.

        When several chunks are planned, their predicates are passed to Spark's
        JDBC reader as partition predicates, so the chunks are read in parallel.
        A single empty-predicate chunk reads the whole table in one partition.

        Returns:
            A ``pyspark.sql.DataFrame`` of the source data.
        """
        subject = self.config.table or f"({self.config.query}) umf_src"

        predicates = [c.predicate for c in self.config.chunks if c.predicate]
        if predicates:
            # Spark reads one partition per predicate, in parallel.
            spark = self._session()
            props = {
                "driver": self.config.driver,
                "fetchsize": str(self.config.fetch_size),
            }
            if self.config.user:
                props["user"] = self.config.user
            if self.config.password:
                props["password"] = self.config.password
            props.update(self.config.extra_options)
            return spark.read.jdbc(
                url=self.config.url,
                table=subject,
                predicates=predicates,
                properties=props,
            )

        # No chunk predicates: a single read of the whole subject.
        return self._base_reader().option("dbtable", subject).load()
