from services.product_filter_service.streaming.tables.table_banned_products import (
    create_banned_products_table,
)


def register_tables(app):
    return {"banned_products": create_banned_products_table(app), }
