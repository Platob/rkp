"""Dependency-free base class for records."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from os import PathLike
    from types import EllipsisType
    from typing import IO

    import pyarrow as pa  # type: ignore[import-untyped]
    from pyiceberg.schema import (  # type: ignore[import-not-found,import-untyped]
        Schema as IcebergSchema,
    )
    from pyiceberg.types import (  # type: ignore[import-not-found,import-untyped]
        NestedField,
    )
    from pyspark.sql import (  # type: ignore[import-not-found,import-untyped]
        DataFrame,
        SparkSession,
    )
    from pyspark.sql.types import (  # type: ignore[import-not-found,import-untyped]
        StructType,
    )

    _CodecSource = (
        str | bytes | bytearray | memoryview | PathLike[str] | IO[str] | IO[bytes]
    )
    _CodecDestination = str | PathLike[str] | IO[str] | IO[bytes]
    _CodecBinaryDestination = str | PathLike[str] | IO[bytes]
    _CodecData = str | bytes | bytearray | memoryview

__all__ = ["Record"]


class Record:
    """Base class for dataclass records.

    The base imports no codec or protocol adapter at runtime.  The
    :func:`rkp.records.record` decorator installs serialization and interop
    methods on concrete record classes; type-only declarations below make
    that dynamic surface visible to static analyzers.
    """

    __slots__ = ()

    alias: ClassVar[str | None] = None

    @classmethod
    def from_dict(
        cls,
        datum: Mapping[str, Any],
        safe: bool = True,
        on_error: Literal["raise", "default"] = "raise",
    ) -> Self:
        """Build ``cls`` from a mapping.

        In safe mode values are checked and recursively converted from their
        annotations.  ``on_error="default"`` uses declared field defaults, an
        optional ``None``, or a sensible zero value when conversion fails.
        Unsafe mode forwards the mapping values to the generated dataclass
        initializer unchanged.
        """

        from .interop import dataclass_from_dict

        return dataclass_from_dict(cls, datum, safe=safe, on_error=on_error)

    # ``record`` installs these methods dynamically so disabled codecs can
    # still raise ``AttributeError`` at runtime and optional protocol modules
    # remain lazy.  Type checkers need the declarations here to expose the
    # complete method surface inherited by decorated record classes.
    if TYPE_CHECKING:

        @classmethod
        def load(
            cls,
            source: _CodecSource,
            *,
            format: str | None = None,
            encoding: str = "utf-8",
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            **kwargs: Any,
        ) -> Self: ...

        @classmethod
        def loads(
            cls,
            data: _CodecData,
            *,
            format: str | None = None,
            encoding: str = "utf-8",
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            **kwargs: Any,
        ) -> Self: ...

        def dump(
            self,
            destination: _CodecDestination,
            *,
            format: str | None = None,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> str | None: ...

        def dumps(
            self,
            *,
            format: str | None = None,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> str: ...

        def dump_bytes(
            self,
            destination: _CodecBinaryDestination,
            *,
            format: str | None = None,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> bytes | None: ...

        def dumps_bytes(
            self,
            *,
            format: str | None = None,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> bytes: ...

        @classmethod
        def load_json(
            cls,
            source: _CodecSource,
            *,
            encoding: str = "utf-8",
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            **kwargs: Any,
        ) -> Self: ...

        @classmethod
        def loads_json(
            cls,
            data: _CodecData,
            *,
            encoding: str = "utf-8",
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            **kwargs: Any,
        ) -> Self: ...

        def dump_json(
            self,
            destination: _CodecDestination,
            *,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> str | None: ...

        def dumps_json(self, *, encoding: str = "utf-8", **kwargs: Any) -> str: ...

        def dump_json_bytes(
            self,
            destination: _CodecBinaryDestination,
            *,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> bytes | None: ...

        def dumps_json_bytes(
            self,
            *,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> bytes: ...

        @classmethod
        def load_yaml(
            cls,
            source: _CodecSource,
            *,
            encoding: str = "utf-8",
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            **kwargs: Any,
        ) -> Self: ...

        @classmethod
        def loads_yaml(
            cls,
            data: _CodecData,
            *,
            encoding: str = "utf-8",
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            **kwargs: Any,
        ) -> Self: ...

        def dump_yaml(
            self,
            destination: _CodecDestination,
            *,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> str | None: ...

        def dumps_yaml(self, *, encoding: str = "utf-8", **kwargs: Any) -> str: ...

        def dump_yaml_bytes(
            self,
            destination: _CodecBinaryDestination,
            *,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> bytes | None: ...

        def dumps_yaml_bytes(
            self,
            *,
            encoding: str = "utf-8",
            **kwargs: Any,
        ) -> bytes: ...

        @classmethod
        def into_arrow_field(
            cls,
            name: str | None = None,
            *,
            nullable: bool = False,
        ) -> pa.Field: ...

        @classmethod
        def into_arrow_schema(cls) -> pa.Schema: ...

        @classmethod
        def from_arrow_batch(
            cls,
            batch: pa.RecordBatch,
            *,
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            validate_schema: bool = True,
        ) -> Iterator[Self]: ...

        @classmethod
        def from_arrow(
            cls,
            source: (
                pa.RecordBatch
                | pa.Table
                | pa.RecordBatchReader
                | Iterable[pa.RecordBatch]
            ),
            *,
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            validate_schema: bool = True,
        ) -> Iterator[Self]: ...

        @classmethod
        def into_arrow_batch(
            cls,
            records: Iterable[Self],
            *,
            schema: pa.Schema | None = None,
        ) -> pa.RecordBatch: ...

        @classmethod
        def into_arrow_batches(
            cls,
            records: Iterable[Self],
            *,
            batch_size: int = 65_536,
            schema: pa.Schema | None = None,
        ) -> Iterator[pa.RecordBatch]: ...

        @classmethod
        def into_arrow_reader(
            cls,
            records: Iterable[Self],
            *,
            batch_size: int = 65_536,
            schema: pa.Schema | None = None,
        ) -> pa.RecordBatchReader: ...

        @classmethod
        def into_iceberg_field(
            cls,
            name: str | None = None,
            *,
            nullable: bool = False,
            field_id_start: int = 1,
            format_version: int = 2,
            downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
        ) -> NestedField: ...

        @classmethod
        def into_iceberg_schema(
            cls,
            *,
            schema_id: int = 0,
            field_id_start: int = 1,
            identifier_field_ids: Iterable[int] | None = None,
            format_version: int = 2,
            downcast_ns_timestamp_to_us: bool | None | EllipsisType = ...,
        ) -> IcebergSchema: ...

        @classmethod
        def into_spark_schema(
            cls,
            *,
            prefer_timestamp_ntz: bool = True,
        ) -> StructType: ...

        @classmethod
        def into_spark_dataframe(
            cls,
            records: Iterable[Self],
            *,
            spark: SparkSession | None = None,
            batch_size: int = 65_536,
        ) -> DataFrame: ...

        @classmethod
        def from_spark(
            cls,
            dataframe: DataFrame,
            *,
            batch_size: int = 65_536,
            safe: bool = True,
            on_error: Literal["raise", "default"] = "raise",
            validate_schema: bool = True,
        ) -> Iterator[Self]: ...

        @classmethod
        def into_glue_table_input(
            cls,
            *,
            name: str | None = None,
            location: str | None = None,
            format: str = "parquet",
            description: str | None = None,
            parameters: Mapping[str, Any] | None = None,
            serde_parameters: Mapping[str, Any] | None = None,
            partition_keys: Iterable[str] | None = None,
            partition_projection: Mapping[str, Any] | None = None,
            partition_location_template: str | None = None,
            partition_projection_enabled: bool = True,
        ) -> dict[str, Any]: ...

        @classmethod
        def into_glue_ddl(
            cls,
            *,
            name: str | None = None,
            database: str | None = None,
            location: str | None = None,
            format: str = "parquet",
            if_not_exists: bool = True,
            description: str | None = None,
            properties: Mapping[str, Any] | None = None,
            serde_properties: Mapping[str, Any] | None = None,
            partition_keys: Iterable[str] | None = None,
            partition_projection: Mapping[str, Any] | None = None,
            partition_location_template: str | None = None,
            partition_projection_enabled: bool = True,
        ) -> str: ...

        @classmethod
        def into_glue_partition_projection(
            cls,
            projections: Mapping[str, Any] | None = None,
            *,
            partition_keys: Iterable[str] | None = None,
            location_template: str | None = None,
            enabled: bool = True,
        ) -> dict[str, str]: ...

        def into_glue_partition_values(
            self,
            *,
            partition_keys: Iterable[str] | None = None,
        ) -> list[str]: ...
