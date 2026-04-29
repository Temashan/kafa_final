from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

ValueDecoder = Callable[[Any, str | None], Any]


class ProductFilterService:
    def __init__(self, registry, *, value_decoder: ValueDecoder | None = None, ):
        self.registry = registry
        self.value_decoder = value_decoder

    def route_message(self, *, raw_key, raw_value, source_topic: str | None = None, ) -> dict:
        decoded_key = None
        if raw_key is not None:
            if isinstance(raw_key, bytes):
                decoded_key = raw_key.decode("utf-8", errors="replace")
            else:
                decoded_key = str(raw_key)
        try:
            decoded_value = self._decode_value(raw_value, source_topic=source_topic)
            product = self._parse_product(decoded_value)
            product_id = self._extract_product_id(product)
        except ValueError as exc:
            event_key = decoded_key or ""
            payload_raw = raw_value
            if isinstance(payload_raw, bytearray):
                payload_raw = bytes(payload_raw)
            if isinstance(payload_raw, bytes):
                payload_raw = payload_raw.decode("utf-8", errors="replace")
            return {
                "forwarded": False,
                "key": event_key,
                "payload": {
                    "product_id": event_key,
                    "reason": "invalid_product",
                    "error": str(exc),
                    "payload": payload_raw,
                },
                "log_message": f"Rejected invalid message: {exc}",
            }

        banned_entry = self.registry.get(product_id)
        ban_entry_payload = {"product_id": product_id}
        if isinstance(banned_entry, dict):
            ban_entry_payload["name"] = banned_entry.get("name")
            ban_entry_payload["reason"] = banned_entry.get("reason")

        if banned_entry is not None:
            return {
                "forwarded": False,
                "key": product_id,
                "payload": {
                    "product_id": product_id,
                    "reason": "banned_product",
                    "ban_entry": ban_entry_payload,
                    "product": product,
                },
                "log_message": f"Rejected banned product {product_id}",
            }

        return {
            "forwarded": True,
            "key": product_id,
            "payload": product,
            "log_message": f"Forwarded product {product_id}",
        }

    def _decode_value(self, value, *, source_topic: str | None):
        if value is None:
            raise ValueError("Kafka message payload is empty")
        if self.value_decoder is None:
            return value
        return self.value_decoder(value, source_topic)

    @staticmethod
    def _parse_product(raw_value) -> dict:
        if isinstance(raw_value, dict):
            payload = raw_value
        else:
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode("utf-8", errors="replace")
            elif not isinstance(raw_value, str):
                raw_value = str(raw_value)

            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Kafka message is not valid JSON: {exc}") from exc

            if not isinstance(payload, dict):
                raise ValueError("Kafka message must contain a JSON object")

        normalized = dict(payload)
        for field in ("created_at", "updated_at"):
            field_value = normalized.get(field)
            if isinstance(field_value, datetime):
                if field_value.tzinfo is None:
                    field_value = field_value.replace(tzinfo=timezone.utc)
                normalized[field] = field_value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return normalized

    @staticmethod
    def _extract_product_id(product: dict) -> str:
        product_id = product.get("product_id")
        if product_id is None or str(product_id).strip() == "":
            raise ValueError("product_id is required")
        return str(product_id)
