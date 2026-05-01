import json
import os
from pathlib import Path
from typing import List, Dict, Any
from confluent_kafka import Consumer, Producer
from elasticsearch import Elasticsearch
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2] if Path(__file__).resolve().parents[2].exists() else Path.cwd()
SSL_CA_FILE = PROJECT_ROOT / "ssl" / "ca" / "ca.crt"

KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "localhost:9094,localhost:9095,localhost:9096"),
    "group.id": "client-api-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    "sasl.username": os.getenv("KAFKA_USERNAME", "producer"),
    "sasl.password": os.getenv("KAFKA_PASSWORD", "producer-secret"),
    "ssl.ca.location": str(SSL_CA_FILE) if SSL_CA_FILE.exists() else None,
}

KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_CONSUMER_CONFIG["bootstrap.servers"],
    "client.id": "client-api",
    "security.protocol": KAFKA_CONSUMER_CONFIG["security.protocol"],
    "sasl.mechanism": KAFKA_CONSUMER_CONFIG["sasl.mechanism"],
    "sasl.username": KAFKA_CONSUMER_CONFIG["sasl.username"],
    "sasl.password": KAFKA_CONSUMER_CONFIG["sasl.password"],
    "ssl.ca.location": KAFKA_CONSUMER_CONFIG["ssl.ca.location"],
}

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")

TOPIC_SEARCH_EVENTS = "client-search-events"
TOPIC_RECOMMENDATION_EVENTS = "client-recommendation-events"


class ClientAPI:
    """CLIENT API для маркетплейса"""

    def __init__(self):
        self.consumer = None
        self.producer = None
        self.es = None
        self.user_history = {}
        self.current_user = "anonymous"

        self._connect_kafka()
        self._connect_elasticsearch()

    def _connect_kafka(self):
        """Подключение к Kafka"""
        try:
            config = {k: v for k, v in KAFKA_CONSUMER_CONFIG.items() if v is not None}
            self.consumer = Consumer(config)

            config_producer = {k: v for k, v in KAFKA_PRODUCER_CONFIG.items() if v is not None}
            self.producer = Producer(config_producer)
            print("Подключен к Kafka")
        except Exception as e:
            print(f"Ошибка подключения к Kafka: {e}")
            print("Продолжаем без Kafka (поиск и рекомендации будут работать)")

    def _connect_elasticsearch(self):
        """Подключение к Elasticsearch"""
        try:
            es_host = ELASTICSEARCH_HOST
            if not es_host.startswith(('http://', 'https://')):
                es_host = f"http://{es_host}"

            self.es = Elasticsearch([es_host])

            if self.es.ping():
                print("Подключен к Elasticsearch")
                if self.es.indices.exists(index="products.validated"):
                    count = self.es.count(index="products.validated")
                    print(f"Индекс 'products.validated' содержит {count['count']} товаров")
                else:
                    print(f"Индекс 'products.validated' не найден")
                    print("Создайте его и загрузите данные")
            else:
                print("Не удалось подключиться к Elasticsearch")
                self.es = None
        except Exception as e:
            print(f"Ошибка подключения к Elasticsearch: {e}")
            self.es = None

    def search_product(self, query: str) -> List[Dict]:
        """
        Поиск товара по имени
        Команда: search <название>
        """
        print(f"\nПоиск: '{query}'")

        self._send_search_event(query)

        products = self._search_in_elasticsearch(query)

        if products:
            print(f"\nНайдено товаров: {len(products)}\n")
            for i, product in enumerate(products, 1):
                self._print_product(product, i)
                self.record_product_view(product)
        else:
            print("\nТовары не найдены\n")

        return products

    def _search_in_elasticsearch(self, query: str) -> List[Dict]:
        """Поиск в Elasticsearch"""
        if not self.es:
            print("Elasticsearch не доступен")
            return []

        try:
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["name^3", "description", "tags", "brand"],
                        "fuzziness": "AUTO",
                        "operator": "or"
                    }
                },
                "size": 10
            }

            response = self.es.search(index="products.validated", body=search_body)
            return [hit['_source'] for hit in response['hits']['hits']]
        except Exception as e:
            print(f"Ошибка поиска: {e}")
            return []

    def _print_product(self, product: Dict, index: int):
        """Красивый вывод товара"""
        name = product.get('name', 'Без названия')
        price = product.get('price', {})
        amount = price.get('amount', 'N/A')
        currency = price.get('currency', 'RUB')
        category = product.get('category', 'N/A')
        brand = product.get('brand', 'N/A')
        stock = product.get('stock', {})
        available = stock.get('available', 0)
        description = product.get('description', '')
        if len(description) > 100:
            description = description[:100] + "..."

        print(f"{index}. {name}")
        print(f"   Цена: {amount} {currency}")
        print(f"   Бренд: {brand}, Категория: {category}")
        print(f"   В наличии: {available}")
        if description:
            print(f"   {description}")
        print()

    def get_recommendations(self) -> List[Dict]:
        """
        Получение персонализированных рекомендаций
        Команда: recommend
        """
        print(f"\nПерсонализированные рекомендации для пользователя: {self.current_user}")

        self._send_recommendation_event()

        recommendations = self._generate_recommendations()

        if recommendations:
            print(f"\nРекомендуем для вас:\n")
            for i, product in enumerate(recommendations, 1):
                name = product.get('name', 'Без названия')
                price = product.get('price', {})
                amount = price.get('amount', 'N/A')
                currency = price.get('currency', 'RUB')
                reason = product.get('recommendation_reason', 'На основе ваших интересов')

                print(f"{i}. {name} - {amount} {currency}")
                print(f"   {reason}")
                print()
        else:
            print("\nРекомендации временно недоступны\n")

        return recommendations

    def _generate_recommendations(self) -> List[Dict]:
        """Генерация рекомендаций"""
        if not self.es:
            return []

        try:
            user_history = self.user_history.get(self.current_user, [])

            if user_history:
                categories = []
                brands = []
                for item in user_history[-5:]:
                    if item.get('category'):
                        categories.append(item['category'])
                    if item.get('brand'):
                        brands.append(item['brand'])

                categories = list(set(categories))
                brands = list(set(brands))

                if categories or brands:
                    should_clauses = []
                    if categories:
                        should_clauses.append({"terms": {"category": categories, "boost": 3}})
                    if brands:
                        should_clauses.append({"terms": {"brand": brands, "boost": 2}})

                    query = {
                        "query": {
                            "bool": {
                                "should": should_clauses,
                                "must_not": [
                                    {"ids": {"values": [item.get('product_id') for item in user_history if
                                                        item.get('product_id')]}}
                                ]
                            }
                        },
                        "size": 5
                    }
                else:
                    query = {
                        "query": {"match_all": {}},
                        "sort": [{"stock.reserved": {"order": "desc"}}],
                        "size": 5
                    }
            else:
                query = {
                    "query": {"match_all": {}},
                    "sort": [{"stock.reserved": {"order": "desc"}}],
                    "size": 5
                }

            response = self.es.search(index="products.validated", body=query)
            recommendations = []

            for hit in response['hits']['hits']:
                product = hit['_source']
                if not user_history:
                    product['recommendation_reason'] = "Популярный товар"
                else:
                    product['recommendation_reason'] = "Похож на просмотренные вами товары"
                recommendations.append(product)

            return recommendations

        except Exception as e:
            print(f"Ошибка генерации рекомендаций: {e}")
            return []

    def record_product_view(self, product: Dict):
        """Запись просмотра товара для персонализации"""
        if self.current_user not in self.user_history:
            self.user_history[self.current_user] = []

        product_id = product.get('product_id')
        if not any(item.get('product_id') == product_id for item in self.user_history[self.current_user]):
            self.user_history[self.current_user].append({
                'product_id': product_id,
                'name': product.get('name'),
                'category': product.get('category'),
                'brand': product.get('brand'),
                'timestamp': datetime.now().isoformat()
            })

            if len(self.user_history[self.current_user]) > 20:
                self.user_history[self.current_user] = self.user_history[self.current_user][-20:]

    def _send_search_event(self, query: str):
        """Отправка события поиска в Kafka"""
        if not self.producer:
            return

        try:
            event = {
                "user_id": self.current_user,
                "event_type": "search",
                "query": query,
                "timestamp": datetime.now().isoformat()
            }

            self.producer.produce(
                TOPIC_SEARCH_EVENTS,
                key=self.current_user,
                value=json.dumps(event).encode('utf-8')
            )
            self.producer.poll(0)
        except Exception as e:
            pass

    def _send_recommendation_event(self):
        """Отправка события запроса рекомендаций в Kafka"""
        if not self.producer:
            return

        try:
            event = {
                "user_id": self.current_user,
                "event_type": "get_recommendations",
                "timestamp": datetime.now().isoformat()
            }

            self.producer.produce(
                TOPIC_RECOMMENDATION_EVENTS,
                key=self.current_user,
                value=json.dumps(event).encode('utf-8')
            )
            self.producer.poll(0)
        except Exception as e:
            pass

    def set_user(self, user_id: str):
        """Установка ID пользователя"""
        self.current_user = user_id
        print(f"Пользователь: {self.current_user}")

    def run_cli(self):
        """Запуск интерактивного CLI"""
        self._print_welcome()

        user_input = input("Введите ваш ID (или нажмите Enter для анонимного режима): ").strip()
        if user_input:
            self.set_user(user_input)
        else:
            print(f"Анонимный режим. ID: {self.current_user}")

        while True:
            try:
                command = input("\nВведите команду: ").strip().lower()

                if not command:
                    continue

                if command in ["exit", "quit", "q"]:
                    print("\nДо свидания!")
                    break

                elif command.startswith("search "):
                    query = command[7:].strip()
                    if query:
                        self.search_product(query)
                    else:
                        print("Укажите название товара для поиска")
                        print("Пример: search планшет")

                elif command in ["recommend", "rec", "r"]:
                    self.get_recommendations()

                else:
                    print("Неизвестная команда")
                    print("Доступные команды: search <название>, recommend, exit")

            except KeyboardInterrupt:
                print("\n\nДо свидания!")
                break
            except Exception as e:
                print(f"Ошибка: {e}")

        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()

    def _print_welcome(self):
        """Приветственное сообщение"""
        print("\n" + "=" * 60)
        print("ДОБРО ПОЖАЛОВАТЬ В МАРКЕТПЛЕЙС")
        print("=" * 60)
        print("\nДоступные команды:")
        print("  search <название>  - поиск товара по имени")
        print("  recommend          - получить персонализированные рекомендации")
        print("  exit               - выход")
        print("-" * 40)
        print()


def main():
    """Точка входа"""
    client = ClientAPI()
    client.run_cli()


if __name__ == "__main__":
    main()