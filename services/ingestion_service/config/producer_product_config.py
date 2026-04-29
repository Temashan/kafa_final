import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SSL_CA_FILE = PROJECT_ROOT / "ssl" / "ca" / "ca.crt"

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv(
        "BOOTSTRAP_SERVERS",
        os.getenv("BOOTSTRAP", "localhost:9094,localhost:9095,localhost:9096"),
    ),
    "client.id": "ingestion-service",
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    "sasl.username": os.getenv("KAFKA_USERNAME", "producer"),
    "sasl.password": os.getenv("KAFKA_PASSWORD", "producer-secret"),
    "ssl.ca.location": os.getenv("KAFKA_SSL_CA_LOCATION", str(SSL_CA_FILE)),
    "ssl.endpoint.identification.algorithm": os.getenv(
        "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM",
        "https",
    ),
    "linger.ms": 50,
    "batch.size": 32768,
    "acks": "all",
    "enable.idempotence": True,
    "retries": 10,
}

BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "5"))
PRODUCTS_FILE = Path(
    os.getenv(
        "PRODUCTS_FILE",
        str(PROJECT_ROOT / "data" / "products.json"),
    )
)
KAFKA_TOPIC = "products.raw"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
AVRO_SCHEMA_PATH = Path(
    os.getenv(
        "AVRO_SCHEMA_PATH",
        str(PROJECT_ROOT / "schemas" / "product.avsc"),
    )
)
