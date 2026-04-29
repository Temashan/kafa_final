

```bash
startDelaySeconds: 10
# Подождать 10 секунд после старта приложения перед началом отдачи метрик.
# Нужно, чтобы Kafka Connect и JVM успели нормально подняться.

lowercaseOutputName: true
# Все имена метрик приводить к нижнему регистру.
# Удобно для Prometheus, чтобы имена были единообразными.

lowercaseOutputLabelNames: true
# Все имена label-полей тоже приводить к нижнему регистру.
# Например, Connector -> connector.


rules:
# Ниже идут правила: какую JMX-метрику искать, как назвать ее в Prometheus
# и какого она типа (GAUGE / COUNTER).

  - pattern: 'java.lang<type=Memory><HeapMemoryUsage>(used|committed|max):'
    name: jvm_memory_heap_$1_bytes
    type: GAUGE
# Метрики heap-памяти JVM.
# used      -> сколько heap сейчас реально используется
# committed -> сколько heap уже выделено JVM
# max       -> максимальный размер heap
# $1 подставляет used/committed/max в имя метрики:
# jvm_memory_heap_used_bytes
# jvm_memory_heap_committed_bytes
# jvm_memory_heap_max_bytes
# Тип GAUGE, потому что значение может как расти, так и уменьшаться.

  - pattern: 'java.lang<type=Memory><NonHeapMemoryUsage>(used|committed|max):'
    name: jvm_memory_nonheap_$1_bytes
    type: GAUGE
# То же самое, но для non-heap памяти JVM.
# Это metaspace, code cache и прочая служебная память JVM.
# Полезно, если Java-процесс раздувается не только за счет heap.

  - pattern: 'kafka.connect<type=source-task-metrics, connector=(.+), task=(.+)><>(source-record-poll-total)'
    name: kafka_connect_source_task_source_record_poll_total
    labels:
      connector: '$1'
      task: '$2'
    type: COUNTER
# Сколько всего записей source task забрал из источника.
# Для Debezium это фактически сколько записей он прочитал из PostgreSQL/WAL.
# connector=(.+) -> имя коннектора, попадет в label connector
# task=(.+)      -> номер task, попадет в label task
# COUNTER, потому что значение только растет.

  - pattern: 'kafka.connect<type=source-task-metrics, connector=(.+), task=(.+)><>(source-record-write-total)'
    name: kafka_connect_source_task_source_record_write_total
    labels:
      connector: '$1'
      task: '$2'
    type: COUNTER
# Сколько всего записей source task записал дальше в Kafka.
# Очень важная CDC-метрика:
# poll = прочитал из источника
# write = записал в Kafka
# Если poll растет, а write нет — есть проблема дальше по цепочке.

  - pattern: 'kafka.connect<type=source-task-metrics, connector=(.+), task=(.+)><>(poll-batch-avg-time-ms)'
    name: kafka_connect_source_task_poll_batch_avg_time_ms
    labels:
      connector: '$1'
      task: '$2'
    type: GAUGE
# Среднее время обработки одного poll batch в миллисекундах.
# Показывает, насколько быстро task читает и обрабатывает пачки событий.
# Если метрика растет — connector может тормозить.

  - pattern: 'kafka.producer<type=producer-metrics, client-id=(.+)><>(record-size-avg)'
    name: kafka_producer_record_size_avg
    labels:
      client_id: '$1'
    type: GAUGE
# Средний размер одной отправляемой записи Kafka producer.
# Полезно понимать, насколько "тяжелые" сообщения отправляет Connect/Debezium.

  - pattern: 'kafka.producer<type=producer-metrics, client-id=(.+)><>(batch-size-avg)'
    name: kafka_producer_batch_size_avg
    labels:
      client_id: '$1'
    type: GAUGE
# Средний размер батча producer.
# Показывает, насколько эффективно producer группирует сообщения перед отправкой.
# Слишком маленький batch -> хуже эффективность.
# Слишком большой -> может вырасти задержка.

  - pattern: 'kafka.producer<type=producer-metrics, client-id=(.+)><>(records-per-request-avg)'
    name: kafka_producer_records_per_request_avg
    labels:
      client_id: '$1'
    type: GAUGE
# Среднее количество записей в одном запросе producer к Kafka broker.
# Чем выше значение, тем обычно лучше batching и эффективнее отправка.

  - pattern: 'kafka.producer<type=producer-topic-metrics, client-id=(.+), topic=(.+)><>(record-send-rate)'
    name: kafka_producer_topic_record_send_rate
    labels:
      client_id: '$1'
      topic: '$2'
    type: GAUGE
# Скорость отправки записей в Kafka по конкретному topic.
# Очень полезно для CDC:
# можно видеть, в какой topic сейчас идет поток событий и с какой скоростью.
# Например отдельно users и orders.
```

Как это запомнить по смыслу

Тут 3 блока метрик:

1. JVM

jvm_memory_heap_*

jvm_memory_nonheap_*

Показывают состояние Java-процесса Kafka Connect.

2. Kafka Connect source task

source-record-poll-total

source-record-write-total

poll-batch-avg-time-ms

Показывают, как Debezium читает изменения из PostgreSQL и отправляет их дальше.

3. Kafka producer

record-size-avg

batch-size-avg

records-per-request-avg

record-send-rate

Показывают, как Connect публикует события в Kafka.

Самое важное именно для Debezium

Если тебе надо оставить в голове только главное, то это:

source-record-poll-total — сколько событий прочитали из источника

source-record-write-total — сколько реально отправили в Kafka

poll-batch-avg-time-ms — насколько быстро connector обрабатывает пачки

record-send-rate — текущая скорость отправки в topic

jvm_memory_heap_used_bytes — не упирается ли Connect в память

По типам метрик

COUNTER
Только растет. Обычно считаем rate(...) или increase(...).

GAUGE
Может и расти, и падать. Смотрим как текущее значение.