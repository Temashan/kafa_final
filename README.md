# Финальный проект

В итоговом проекте предстоит разработать аналитическую платформу для маркетплейса. 


## Запуск проекта

```bash
./start.sh
```

Скрипт проверит сертификаты, при необходимости запустит `generate.sh`

или
```bash
docker compose up -d
```

Остановить

```bash
./stop.sh
```

Или:
```bash
docker compose down
```

## Адреса

Kafka UI: `http://localhost:8080`
Elasticsearch: `http://localhost:9200`
Kafka Connect: `http://localhost:8083`
Grafana: `http://localhost:3000`  (`admin/admin`)
Prometheus: `http://localhost:9090`
Alertmanager: `http://localhost:9093`
HDFS UI: `http://localhost:9870`

Отправить товары в Kafka

```bash
uv run python main.py
```

Товары читаются из `data/products.json` и отправляются в topic `products.raw`.

### 2. Добавить запрещённый товар

```bash
uv run python -m services.product_filter_service.cli_ban.banned_cli ban 12346 --name "Nova X10" --reason "Запрещён к продаже"
```

Убрать товар из запрещённых:

```bash
uv run python -m services.product_filter_service.cli_ban.banned_cli unban 12346
```

- [1.md](1.md)
- [3.md](3.md)
- [4.md](4.md)
- [5.md](5.md)


### Проверить мониторинг

```text
http://localhost:9090/targets
```

Проверить алерт падения брокера:

```bash
docker compose stop kafka-1
```

Далее открыть в браузере:

```text
http://localhost:9090/alerts
http://localhost:9093
```

Затем обратно поднять брокер:

```bash
docker compose start kafka-1
```
![прометеус1.png](screenshots/%D0%BF%D1%80%D0%BE%D0%BC%D0%B5%D1%82%D0%B5%D1%83%D1%811.png)
![прометеус2.png](screenshots/%D0%BF%D1%80%D0%BE%D0%BC%D0%B5%D1%82%D0%B5%D1%83%D1%812.png)
![графана.png](screenshots/%D0%B3%D1%80%D0%B0%D1%84%D0%B0%D0%BD%D0%B0.png)
![алерт.png](screenshots/%D0%B0%D0%BB%D0%B5%D1%80%D1%82.png)