"""Utility helpers for zerobus-ingest."""

from pathlib import Path

from zerobus_ingest.utils.protobuf_utils import ProtobufUtils
from zerobus_ingest.utils.table_utils import TableUtils
from zerobus_ingest.utils.volume_utils import VolumeUtils
from zerobus_ingest.utils.writer import ZerobusWriter


def read_binary(path: Path | str) -> bytes:
    """Read a file as raw bytes. path can be a Path or path-like string."""
    return Path(path).read_bytes()


__all__ = ["read_binary", "ProtobufUtils", "TableUtils", "VolumeUtils", "ZerobusWriter"]
