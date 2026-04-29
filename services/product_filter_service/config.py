import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Kafka
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9094,localhost:9095,localhost:9096")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka://kafka-0:9092;kafka://kafka-1:9092;kafka://kafka-2:9092", )
FILTER_APP_ID = os.getenv("FILTER_APP_ID", "products-filter-group")
FILTER_CONSUMER_GROUP = os.getenv("FILTER_CONSUMER_GROUP", FILTER_APP_ID)

# Topics
RAW_PRODUCTS_TOPIC = os.getenv("RAW_PRODUCTS_TOPIC", "products.raw")
VALIDATED_PRODUCTS_TOPIC = os.getenv("VALIDATED_PRODUCTS_TOPIC", "products.validated")
REJECTED_PRODUCTS_TOPIC = os.getenv("REJECTED_PRODUCTS_TOPIC", "products.rejected")
BANNED_PRODUCTS_TOPIC = os.getenv("BANNED_PRODUCTS_TOPIC", "products.banned")

# Schema Registry
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")  # добавил себе для локальной проверки

# Files
SSL_CA_FILE = PROJECT_ROOT / "ssl" / "ca" / "ca.crt"
BANNED_PRODUCT_SCHEMA_PATH = Path(
    os.getenv("BANNED_PRODUCT_SCHEMA_PATH", PROJECT_ROOT / "schemas" / "banned_product.avsc"))
