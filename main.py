from services.ingestion_service.config.producer_product_config import (
    AVRO_SCHEMA_PATH,
    BATCH_SIZE,
    KAFKA_CONFIG,
    KAFKA_TOPIC,
    PRODUCTS_FILE,
    SCHEMA_REGISTRY_URL,
)
from services.ingestion_service.producer import KafkaProductProducer
from services.ingestion_service.reader import ProductFileReader
from services.ingestion_service.service import ProductIngestionService


def main():
    reader = ProductFileReader(PRODUCTS_FILE)
    producer = KafkaProductProducer(
        KAFKA_CONFIG,
        KAFKA_TOPIC,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=AVRO_SCHEMA_PATH,
    )
    service = ProductIngestionService(producer=producer, batch_size=BATCH_SIZE)

    products_stream = reader.read_products()
    service.send_products(products_stream)


if __name__ == "__main__":
    main()