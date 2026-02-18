"""ProtobufUtils: helpers for loading descriptors from binary FileDescriptorSets."""

from __future__ import annotations

from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor_pool import DescriptorPool


class ProtobufUtils:
    """Utilities for working with protobuf descriptors."""

    @staticmethod
    def descriptor_from_binary(descriptor: bytes, message_name: str) -> Descriptor:
        """Load a message Descriptor from a binary FileDescriptorSet.

        The bytes should be a serialized FileDescriptorSet (e.g. from
        `buf build -o descriptor.bin` or `protoc --descriptor_set_out=file.pb`).
        message_name is the full protobuf message name (e.g. 'orders.v1.Order').

        Returns the same type of Descriptor as MyMessage.DESCRIPTOR, suitable
        for use with TableUtils.descriptor_to_columns() and similar.
        """
        fs = descriptor_pb2.FileDescriptorSet()
        fs.ParseFromString(descriptor)
        pool = DescriptorPool()
        for fp in fs.file:
            pool.Add(fp)
        desc = pool.FindMessageTypeByName(message_name)
        if desc is None:
            raise ValueError(
                f"Message {message_name!r} not found in descriptor. "
                "Use the full protobuf message name (e.g. orders.v1.Order)."
            )
        return desc
