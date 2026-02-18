"""TableUtils: helpers for Unity Catalog table operations."""

from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    DataSourceFormat,
    TableInfo,
    TableType,
)
from google.protobuf.descriptor import Descriptor, FieldDescriptor

# Proto type to Unity Catalog scalar
# (reverse of zerobus generate_proto.py type mappings)
_PROTO_SCALAR_TO_UC: dict[int, ColumnTypeName] = {
    FieldDescriptor.TYPE_INT32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_SINT32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_SFIXED32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_UINT32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_FIXED32: ColumnTypeName.INT,
    FieldDescriptor.TYPE_INT64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_SINT64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_SFIXED64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_UINT64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_FIXED64: ColumnTypeName.LONG,
    FieldDescriptor.TYPE_STRING: ColumnTypeName.STRING,
    FieldDescriptor.TYPE_FLOAT: ColumnTypeName.FLOAT,
    FieldDescriptor.TYPE_DOUBLE: ColumnTypeName.DOUBLE,
    FieldDescriptor.TYPE_BOOL: ColumnTypeName.BOOLEAN,
    FieldDescriptor.TYPE_BYTES: ColumnTypeName.BINARY,
}
# TYPE_ENUM -> INT (store enum as integer in Delta)

# Spark DataType JSON for scalar ColumnTypeName
# (required by Create Table API).
_SCALAR_TYPE_JSON: dict[ColumnTypeName, str] = {
    ColumnTypeName.STRING: '{"type": "string"}',
    ColumnTypeName.INT: '{"type": "integer"}',
    ColumnTypeName.LONG: '{"type": "long"}',
    ColumnTypeName.FLOAT: '{"type": "float"}',
    ColumnTypeName.DOUBLE: '{"type": "double"}',
    ColumnTypeName.BOOLEAN: '{"type": "boolean"}',
    ColumnTypeName.BINARY: '{"type": "binary"}',
}


def _field_to_type_json(field: FieldDescriptor, depth: int = 0) -> str:
    """Return Spark DataType JSON for a field (for Create Table API type_json)."""
    if depth > 50:
        raise ValueError("Proto nesting too deep")
    kind = field.type
    if kind == FieldDescriptor.TYPE_MESSAGE:
        msg_desc = field.message_type
        fields_json = []
        for i, f in enumerate(msg_desc.fields, start=1):
            fj = _field_to_type_json(f, depth + 1)
            fields_json.append(
                f'{{"name": "{f.name}", "type": {fj}, "nullable": true}}'
            )
        return '{"type": "struct", "fields": [' + ", ".join(fields_json) + "]}"
    if kind == FieldDescriptor.TYPE_ENUM:
        return '{"type": "integer"}'
    scalar = _PROTO_SCALAR_TO_UC.get(kind)
    if scalar is not None:
        return _SCALAR_TYPE_JSON.get(scalar, '{"type": "string"}')
    return '{"type": "string"}'


def _field_to_uc_type_text(field: FieldDescriptor, depth: int = 0) -> str:
    """
    Return Unity Catalog type string for a field
    (e.g. STRING, STRUCT<...>, ARRAY<...>).
    """
    if depth > 50:
        raise ValueError("Proto nesting too deep")
    kind = field.type
    if kind == FieldDescriptor.TYPE_MESSAGE:
        msg_desc = field.message_type
        parts = []
        for f in msg_desc.fields:
            uc = _field_to_uc_type_text(f, depth + 1)
            parts.append(f"{f.name}:{uc}")
        return "STRUCT<" + ", ".join(parts) + ">"
    if kind == FieldDescriptor.TYPE_ENUM:
        return "INT"
    scalar = _PROTO_SCALAR_TO_UC.get(kind)
    if scalar is not None:
        return scalar.name
    raise ValueError(f"Unsupported proto field type: {field.name} (type={kind})")


def _descriptor_to_columns_impl(descriptor: Descriptor) -> list[ColumnInfo]:
    """
    Convert a message Descriptor to a list of ColumnInfo
    (UC column definitions).
    """
    result: list[ColumnInfo] = []
    for position, field in enumerate(descriptor.fields, start=1):
        name = field.name
        if field.label == FieldDescriptor.LABEL_REPEATED:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                elem_type = _field_to_uc_type_text(field)
                elem_json = _field_to_type_json(field)
                result.append(
                    ColumnInfo(
                        name=name,
                        type_name=ColumnTypeName.ARRAY,
                        type_text=f"ARRAY<{elem_type}>",
                        type_json=f'{{"type": "array", "elementType": {elem_json},"containsNull": true}}',  # noqa: E501
                        position=position,
                    )
                )
            else:
                scalar = _PROTO_SCALAR_TO_UC.get(
                    field.type,
                    ColumnTypeName.STRING
                    if field.type == FieldDescriptor.TYPE_ENUM
                    else None,
                )
                if field.type == FieldDescriptor.TYPE_ENUM:
                    scalar = ColumnTypeName.INT
                if scalar is None:
                    scalar = ColumnTypeName.STRING
                scalar_json = _SCALAR_TYPE_JSON.get(scalar, '{"type": "string"}')
                result.append(
                    ColumnInfo(
                        name=name,
                        type_name=ColumnTypeName.ARRAY,
                        type_text=f"ARRAY<{scalar.name}>",
                        type_json=f'{{"type": "array", "elementType": {scalar_json}, "containsNull": true}}',  # noqa: E501
                        position=position,
                    )
                )
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            type_text = _field_to_uc_type_text(field)
            type_json = _field_to_type_json(field)
            result.append(
                ColumnInfo(
                    name=name,
                    type_name=ColumnTypeName.STRUCT,
                    type_text=type_text,
                    type_json=type_json,
                    position=position,
                )
            )
        elif field.type == FieldDescriptor.TYPE_ENUM:
            result.append(
                ColumnInfo(
                    name=name,
                    type_name=ColumnTypeName.INT,
                    type_text="INT",
                    type_json=_SCALAR_TYPE_JSON[ColumnTypeName.INT],
                    position=position,
                )
            )
        else:
            type_name = _PROTO_SCALAR_TO_UC.get(field.type)
            if type_name is None:
                type_name = ColumnTypeName.STRING
            result.append(
                ColumnInfo(
                    name=name,
                    type_name=type_name,
                    type_text=type_name.name,
                    type_json=_SCALAR_TYPE_JSON.get(type_name, '{"type": "string"}'),
                    position=position,
                )
            )
    return result


class TableUtils:
    """Helpers to check table existence and other UC table operations."""

    @staticmethod
    def table_exists(
        workspace_client: WorkspaceClient,
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:
        """
        Return True if a table exists in the metastore for
        the given catalog, schema, and table name.
        """
        full_name = f"{catalog}.{schema}.{table}"
        response = workspace_client.tables.exists(full_name=full_name)
        return bool(getattr(response, "table_exists", False))

    @staticmethod
    def descriptor_to_columns(descriptor: Descriptor) -> list[ColumnInfo]:
        """Convert a protobuf message DESCRIPTOR to a list of Unity Catalog ColumnInfo.

        Uses the inverse of the [zerobus generate_proto type mappings]
        (https://github.com/databricks/zerobus-sdk-py/blob/main/zerobus/tools/generate_proto.py):
        int32->INT, int64->LONG, string->STRING, float->FLOAT, double->DOUBLE,
        bool->BOOLEAN, bytes->BINARY, enum->INT, message->STRUCT<...>,
        repeated->ARRAY<...>.

        Args:
            descriptor: A message Descriptor (e.g. Order.DESCRIPTOR from orders_pb2).

        Returns:
            List of ColumnInfo suitable for TableUtils.create_table(..., columns=...).
        """
        return _descriptor_to_columns_impl(descriptor)

    @staticmethod
    def pretty_print_columns(columns: list[ColumnInfo]) -> str:
        """Format a list of ColumnInfo as a readable table string.

        Args:
            columns: List of ColumnInfo (e.g. from descriptor_to_columns).

        Returns:
            A string with one column per line: name and type, aligned.
        """
        if not columns:
            return ""
        name_width = max(len(c.name) for c in columns)
        lines = []
        for c in columns:
            type_str = (
                c.type_text
                if c.type_text
                else (c.type_name.name if c.type_name else "?")
            )
            lines.append(f"  {c.name:<{name_width}}  {type_str}")
        return "\n".join(lines)

    @staticmethod
    def create_table(
        workspace_client: WorkspaceClient,
        catalog: str,
        schema: str,
        table: str,
        storage_location: str | None = None,
        *,
        table_type: TableType = TableType.MANAGED,
        data_source_format: DataSourceFormat = DataSourceFormat.DELTA,
        columns: list[ColumnInfo] | None = None,
        properties: dict[str, str] | None = None,
    ) -> TableInfo:
        """Create a table in the metastore using the WorkspaceClient.

        Uses the same credentials as the workspace client. Defaults create a
        managed Delta table. When storage_location is omitted (None), the backend
        manages the backing storage for managed tables. Pass storage_location only
        when you need to set it explicitly (e.g. for EXTERNAL tables).

        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            catalog: Unity Catalog catalog name.
            schema: Schema name within the catalog.
            table: Table name within the schema.
            storage_location: Optional storage root URL. If None, not passed
            (managed tables
                use backend-managed storage). Required for EXTERNAL
                tables—pass explicitly.
            table_type: TableType (default MANAGED).
            data_source_format: DataSourceFormat (default DELTA).
            columns: Optional list of ColumnInfo for the table schema.
            properties: Optional key-value properties for the table.

        Returns:
            TableInfo for the created table.
        """
        if storage_location is None and table_type != TableType.MANAGED:
            raise ValueError(
                "storage_location is required for non-managed "
                + "(e.g. EXTERNAL) tables."
            )
        # Only pass storage_location when provided; managed tables
        # use backend-managed storage when None.
        storage = storage_location if storage_location is not None else ""
        return workspace_client.tables.create(
            name=table,
            catalog_name=catalog,
            schema_name=schema,
            table_type=table_type,
            data_source_format=data_source_format,
            storage_location=storage,
            columns=columns,
            properties=properties,
        )
