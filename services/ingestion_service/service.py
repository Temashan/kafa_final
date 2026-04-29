from typing import Iterable


class ProductIngestionService:
    def __init__(self, producer, batch_size: int = 5):
        self.producer = producer
        self.batch_size = batch_size

    def send_products(self, products: Iterable[dict]) -> None:
        sent_since_poll = 0

        try:
            for product in products:
                self._validate_product(product)
                self.producer.send(product)
                sent_since_poll += 1

                if sent_since_poll >= self.batch_size:
                    self.producer.poll()
                    sent_since_poll = 0
        finally:
            self.producer.flush()

    @staticmethod
    def _validate_product(product: dict) -> None:
        required_fields = ("product_id", "name", "price", "store_id")
        missing_fields = [field for field in required_fields if field not in product]

        if missing_fields:
            missing = ", ".join(missing_fields)
            raise ValueError(f"Missing required product fields: {missing}")
