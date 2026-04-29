#!/usr/bin/env sh
set -eu

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka-0:9092,kafka-1:9092,kafka-2:9092}"
ADMIN_PROPERTIES="${ADMIN_PROPERTIES:-/etc/kafka/config/admin.properties}"

/opt/kafka-scripts/wait-for-kafka.sh "${BOOTSTRAP_SERVERS}" "${ADMIN_PROPERTIES}"
/opt/kafka-scripts/create-topics.sh "${BOOTSTRAP_SERVERS}" "${ADMIN_PROPERTIES}"
/opt/kafka-scripts/configure-acls.sh "${BOOTSTRAP_SERVERS}" "${ADMIN_PROPERTIES}"

echo "Primary Kafka cluster bootstrap finished"
