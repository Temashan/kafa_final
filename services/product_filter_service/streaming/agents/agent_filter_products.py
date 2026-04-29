import json
import logging

from services.product_filter_service.config import (
    BANNED_PRODUCTS_TOPIC,
    RAW_PRODUCTS_TOPIC,
    REJECTED_PRODUCTS_TOPIC,
    SCHEMA_REGISTRY_URL,
    VALIDATED_PRODUCTS_TOPIC,
)
from services.product_filter_service.streaming.agents.filter_service import ProductFilterService
from services.product_filter_service.streaming.agents.parsing import _decode_key, _to_bool
from services.product_filter_service.streaming.schema_decoder import AvroProductValueDecoder

logger = logging.getLogger(__name__)


def _extract_payload_dict(decoded_value, *, topic_name: str) -> dict:
    if isinstance(decoded_value, dict):
        payload = decoded_value.get("payload", decoded_value)
        if isinstance(payload, dict):
            return payload
        raise ValueError(f"{topic_name} payload must be a JSON object")

    if isinstance(decoded_value, bytes):
        decoded_value = decoded_value.decode("utf-8", errors="replace")

    if isinstance(decoded_value, str):
        try:
            parsed = json.loads(decoded_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{topic_name} payload is not valid JSON: {exc}") from exc

        if isinstance(parsed, dict):
            payload = parsed.get("payload", parsed)
            if isinstance(payload, dict):
                return payload
            raise ValueError(f"{topic_name} payload must be a JSON object")

    raise ValueError(f"{topic_name} payload must be a JSON object")


def _build_validated_sink_payload(payload: dict, *, key: str | None = None) -> dict:
    product_id = payload.get("product_id")
    if product_id is None:
        product_id = key or ""
    return {
        "product_id": str(product_id),
        "raw_json": payload,
    }


def _parse_banned_payload(raw_value, *, value_decoder):
    decoded_value = raw_value
    if value_decoder is not None:
        decoded_value = value_decoder(raw_value, BANNED_PRODUCTS_TOPIC)
    return _extract_payload_dict(decoded_value, topic_name=BANNED_PRODUCTS_TOPIC)


def register(app, tables):
    banned_products_topic = app.topic(
        BANNED_PRODUCTS_TOPIC,
        key_serializer="raw",
        value_serializer="raw",
    )
    raw_products_topic = app.topic(
        RAW_PRODUCTS_TOPIC,
        key_serializer="raw",
        value_serializer="raw",
    )
    validated_products_topic = app.topic(
        VALIDATED_PRODUCTS_TOPIC,
        key_type=str,
        value_serializer="json",
    )
    rejected_products_topic = app.topic(
        REJECTED_PRODUCTS_TOPIC,
        key_type=str,
        value_serializer="json",
    )

    value_decoder = AvroProductValueDecoder(SCHEMA_REGISTRY_URL)
    logger.info("Schema Registry decoder enabled for %s", RAW_PRODUCTS_TOPIC)

    banned_products = tables["banned_products"]
    filter_service = ProductFilterService(
        registry=banned_products,
        value_decoder=value_decoder,
    )

    @app.agent(raw_products_topic)
    async def process_products(stream):
        async for raw_key, raw_value in stream.items():
            routed = filter_service.route_message(
                raw_key=raw_key,
                raw_value=raw_value,
                source_topic=RAW_PRODUCTS_TOPIC,
            )

            if routed["forwarded"]:
                payload_to_send = _build_validated_sink_payload(
                    routed["payload"],
                    key=routed["key"],
                )
                await validated_products_topic.send(
                    key=routed["key"],
                    value=payload_to_send,
                )
            else:
                await rejected_products_topic.send(
                    key=routed["key"],
                    value=routed["payload"],
                )

            logger.info(routed["log_message"])

    @app.agent(banned_products_topic)
    async def sync_banned_products(stream):
        async for raw_key, raw_value in stream.items():
            product_id = _decode_key(raw_key)
            if not product_id:
                logger.warning("Skip banned event without product_id key")
                continue

            try:
                payload = _parse_banned_payload(raw_value, value_decoder=value_decoder)
            except ValueError as exc:
                logger.warning("Skip invalid banned event for key=%s: %s", product_id, exc)
                continue

            if _to_bool(payload.get("banned")):
                banned_products.upsert(
                    product_id,
                    name=payload.get("name"),
                    reason=payload.get("reason"),
                )
                logger.info("Marked product %s as banned", product_id)
            else:
                banned_products.remove(product_id)
                logger.info("Removed product %s from banned list", product_id)
