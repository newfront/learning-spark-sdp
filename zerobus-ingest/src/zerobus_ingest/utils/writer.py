"""ZerobusWriter: abstract zerobus stream writing using ZerobusSdk."""

from __future__ import annotations

import logging
from typing import Any

from google.protobuf.descriptor import Descriptor
from zerobus.sdk.shared.definitions import (
    StreamConfigurationOptions,
    TableProperties,
)
from zerobus.sdk.sync import ZerobusSdk, ZerobusStream


class ZerobusWriter:
    """Wraps ZerobusSdk and ZerobusStream for writing records to a Zerobus stream."""

    def __init__(
        self,
        *,
        host: str,
        workplace_url: str,
        workspace_id: str,
        region: str,
        client_id: str,
        client_secret: str,
        catalog: str,
        schema: str,
        table: str,
        stream_options: StreamConfigurationOptions | None = None,
    ) -> None:
        self._host = host
        self._workplace_url = workplace_url
        self._workspace_id = workspace_id
        self._region = region
        self._client_id = client_id
        self._client_secret = client_secret
        self._table_name = f"{catalog}.{schema}.{table}"
        self._stream_options = stream_options or StreamConfigurationOptions()

        self._sdk: ZerobusSdk | None = None
        self._stream: ZerobusStream | None = None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ZerobusWriter:
        """Build a ZerobusWriter from a config dict (e.g. Config.databricks())."""
        return cls(
            host=config["host"],
            workplace_url=config["workspace_url"],
            workspace_id=config["workspace_id"],
            region=config["region"],
            client_id=config["zerobus_client_id"],
            client_secret=config["zerobus_client_secret"],
            catalog=config["catalog"],
            schema=config["schema"],
            table=config["table"],
        )

    def with_stream_options(self, options: StreamConfigurationOptions) -> ZerobusWriter:
        """
        Overwrite stream options.
        Call before any write() so they apply when the stream is created.
        """
        self._stream_options = options
        return self

    def generate_sdk(self) -> ZerobusSdk:
        """
        Build and return ZerobusSdk from config;
        log server_endpoint and unity_catalog_url.
        """
        server_endpoint = (
            f"{self._workspace_id}.zerobus.{self._region}.cloud.databricks.com"
        )
        unity_catalog_url = self._workplace_url
        logging.info("Server endpoint: %s", server_endpoint)
        logging.info("Unity catalog URL: %s", unity_catalog_url)
        self._sdk = ZerobusSdk(
            host=server_endpoint,
            unity_catalog_url=unity_catalog_url,
        )
        return self._sdk

    @staticmethod
    def get_descriptor(record: Any) -> Descriptor | None:
        """Return the DESCRIPTOR for a protobuf message, or None if the record has none.

        Useful in tests to assert the correct descriptor is used for TableProperties.
        """
        return getattr(record, "DESCRIPTOR", None)

    def _ensure_stream(self, descriptor: Descriptor | None = None) -> ZerobusStream:
        if self._stream is not None:
            return self._stream
        if self._sdk is None:
            self.generate_sdk()
        if descriptor is not None:
            table_properties = TableProperties(
                self._table_name, descriptor_proto=descriptor
            )
        else:
            table_properties = TableProperties(table_name=self._table_name)
        self._stream = self._sdk.create_stream(
            client_id=self._client_id,
            client_secret=self._client_secret,
            table_properties=table_properties,
            options=self._stream_options,
        )
        return self._stream

    def write(self, record: Any) -> Any:
        """
        Ingest a single record (protobuf Message or dict).
        Returns RecordAcknowledgment."""
        descriptor = self.get_descriptor(record)
        stream = self._ensure_stream(descriptor)
        return stream.ingest_record(record)

    def flush(self) -> None:
        """Flush the stream."""
        if self._stream is not None:
            self._stream.flush()

    def close(self) -> None:
        """Close the stream and release resources."""
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def __enter__(self) -> ZerobusWriter:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
