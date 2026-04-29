#!/usr/bin/env sh
set -eu

if [ ! -f ssl/keystores/kafka-0.keystore.jks ] || \
   [ ! -f ssl/keystores/dr-kafka-0.keystore.jks ] || \
   [ ! -f ssl/mounts/kafka-0/kafka.keystore.jks ] || \
   [ ! -f ssl/mounts/dr-kafka-0/kafka.keystore.jks ]; then
  echo "TLS artifacts were not found. Generating certificates..."
  ./generate.sh
fi

docker compose up -d

echo "Kafka lab is starting."
echo "Kafka UI: http://localhost:8080"
echo "Grafana: http://localhost:3000  login: admin / admin"
echo "Prometheus: http://localhost:9090"
echo "Alertmanager: http://localhost:9093"
