from __future__ import annotations

from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from services.ingestion_service.utils.normalize_types import normalize_types
from services.ingestion_service.utils.to_timestamp_millis import to_timestamp_millis


class KafkaProductProducer:
    def __init__(self, config: dict[str, Any], topic: str, *, schema_registry_url: str, schema_path: str | Path, ):
        self._producer = Producer(config)
        self._topic = topic
        schema_str = Path(schema_path).read_text(encoding="utf-8")
        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

        self._serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            conf={"auto.register.schemas": False},
        )

    def send(self, product: dict) -> None:
        product_id = product.get("product_id")
        if product_id is None:
            raise ValueError("product_id is required")

        prepared = self._prepare_product(product)
        encoded_value = self._serializer(prepared, SerializationContext(self._topic, MessageField.VALUE))

        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=str(product_id),
                    value=encoded_value,
                    callback=self._delivery_report,
                )
                self._producer.poll(0)
                break
            except BufferError:
                self._producer.poll(0.1)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"{msg.topic()} [{msg.partition()}] offset={msg.offset()}")

    @classmethod
    def _prepare_product(cls, product: dict) -> dict:
        prepared = normalize_types(product)

        for field in ("created_at", "updated_at"):
            if field in prepared:
                prepared[field] = to_timestamp_millis(prepared[field], field)

        return prepared
