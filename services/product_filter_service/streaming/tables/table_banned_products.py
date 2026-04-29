class BannedProductsTable:
    """
    Логику добавил, т.к через cli идет управление. Значит надо поработать в table с продуктом
    """

    def __init__(self, table):
        self._table = table

    def get(self, product_id: str) -> dict | None:
        payload = self._table.get(str(product_id))
        if isinstance(payload, dict):
            return payload
        return None

    def upsert(self, product_id: str, *, name: str | None = None, reason: str | None = None) -> None:
        self._table[str(product_id)] = {"name": name, "reason": reason}

    def remove(self, product_id: str) -> None:
        self._table.pop(str(product_id), None)


def create_banned_products_table(app):
    table = app.Table(
        "banned_products",
        partitions=1,
        key_type=str,
    )
    return BannedProductsTable(table)
