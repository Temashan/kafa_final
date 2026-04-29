import logging
import os
import ssl

import faust

from services.product_filter_service.config import FILTER_APP_ID, KAFKA_BROKER_URL, SSL_CA_FILE
from services.product_filter_service.streaming.agents import register_agents
from services.product_filter_service.streaming.tables import register_tables


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_broker_credentials() -> faust.SASLCredentials:
    username = os.getenv("FILTER_BROKER_USERNAME", "producer")
    password = os.getenv("FILTER_BROKER_PASSWORD", "producer-secret")
    ssl_context = _build_ssl_context()

    kwargs = {
        "username": username,
        "password": password,
    }
    if ssl_context is not None:
        kwargs["ssl_context"] = ssl_context

    return faust.SASLCredentials(**kwargs)


def _build_ssl_context():
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    if security_protocol != "SASL_SSL":
        return None

    ca_location = os.getenv("KAFKA_SSL_CA_LOCATION", str(SSL_CA_FILE))
    if not ca_location:
        return None

    ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=ca_location, )
    endpoint_identification_algorithm = os.getenv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https")
    ssl_context.check_hostname = endpoint_identification_algorithm.lower() == "https"
    return ssl_context


setup_logging()

app = faust.App(
    FILTER_APP_ID,
    broker=KAFKA_BROKER_URL,
    broker_credentials=_build_broker_credentials(),
    consumer_auto_offset_reset=os.getenv("FILTER_AUTO_OFFSET_RESET", "earliest"),
    topic_allow_declare=False,
    topic_disable_leader=True,
    value_serializer="raw",
    key_serializer="raw",
)

tables = register_tables(app)
register_agents(app, tables)


def main() -> None:
    app.main()


__all__ = ["app", "main"]

if __name__ == "__main__":
    main()
