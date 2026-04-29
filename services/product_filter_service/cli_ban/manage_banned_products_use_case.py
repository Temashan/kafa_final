class ManageBannedProductsUseCase:
    def __init__(self, producer):
        self._producer = producer

    def ban(self, product_id: str, *, name: str | None = None, reason: str | None = None) -> None:
        self._producer.send(product_id=product_id, banned=True, name=name, reason=reason)

    def unban(self, product_id: str) -> None:
        self._producer.send(product_id=product_id, banned=False, name=None, reason=None)

    def flush(self) -> None:
        self._producer.flush()
