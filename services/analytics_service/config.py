import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "dr-kafka-0:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "products.validated")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "recommendations")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
ANALYTICS_DEBUG = os.getenv("ANALYTICS_DEBUG", "true").strip().lower()
HDFS_PATH = os.getenv("HDFS_RAW_PATH", "hdfs://hdfs-namenode:9000/data/products_validated")
CHECKPOINT = os.getenv("CHECKPOINT_PATH", "hdfs://hdfs-namenode:9000/checkpoints/analytics")

TRIGGER = f"{os.getenv('TRIGGER_SECONDS', '20')} seconds"

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{os.getenv("KAFKA_USERNAME", "admin")}" '
        f'password="{os.getenv("KAFKA_PASSWORD", "admin-password")}";'
    ),
    "kafka.ssl.truststore.location": os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION"),
    "kafka.ssl.truststore.password": os.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD"),
    "kafka.ssl.truststore.type": os.getenv("KAFKA_SSL_TRUSTSTORE_TYPE", "JKS"),
    "kafka.ssl.endpoint.identification.algorithm": os.getenv(
        "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "HTTPS"
    ),
}
KAFKA_OPTIONS = {k: v for k, v in KAFKA_OPTIONS.items() if v is not None}
