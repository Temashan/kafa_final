from __future__ import annotations

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


class AvroProductValueDecoder:
    def __init__(self, schema_registry_url: str):
        self._deserializer = AvroDeserializer(
            SchemaRegistryClient({"url": schema_registry_url}),
            from_dict=self._from_dict,
        )

    def __call__(self, raw_value, source_topic: str | None):
        if raw_value is None:
            return None
        if isinstance(raw_value, bytearray):
            raw_value = bytes(raw_value)
        if not isinstance(raw_value, bytes):
            return raw_value

        topic = source_topic or "products.raw"
        try:
            return self._deserializer(
                raw_value,
                SerializationContext(topic, MessageField.VALUE),
            )
        except Exception as exc:
            raise ValueError(f"Schema Registry decode failed: {exc}") from exc

    @staticmethod
    def _from_dict(payload: dict, _context) -> dict:
        return payload
