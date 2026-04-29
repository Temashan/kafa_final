from services.product_filter_service.streaming.agents import agent_filter_products


def register_agents(app, tables):
    agent_filter_products.register(app, tables)
