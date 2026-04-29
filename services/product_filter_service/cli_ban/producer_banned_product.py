from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


class KafkaBannedProducer:
    def __init__(self, config: dict[str, Any], topic: str, *, schema_registry_url: str, schema_path: str | Path):
        self._producer = Producer(config)
        self._topic = topic
        schema_str = Path(schema_path).read_text(encoding="utf-8")
        self._schema_registry = SchemaRegistryClient({"url": schema_registry_url})
        self._serializer = AvroSerializer(
            self._schema_registry,
            schema_str,
            to_dict=lambda obj, ctx: obj,
            conf={"auto.register.schemas": True},
        )

    def send(self, product_id: str, *, banned: bool, name: str | None = None, reason: str | None = None, ) -> None:
        payload = {"banned": banned, "name": name, "reason": reason, }
        value = self._serializer(payload, SerializationContext(self._topic, MessageField.VALUE), )

        for _ in range(10):
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=str(product_id),
                    value=value,
                    callback=self._delivery_report,
                )
                self._producer.poll(0)
                return
            except BufferError:
                self._producer.poll(0.1)
        raise RuntimeError("Kafka produce failed")

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            print(f"Delivery failed: {err}")
