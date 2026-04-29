#!/usr/bin/env sh
set -eu

SOURCE_BOOTSTRAP_SERVERS="${SOURCE_BOOTSTRAP_SERVERS:-kafka-0:9092,kafka-1:9092,kafka-2:9092}"
TARGET_BOOTSTRAP_SERVERS="${TARGET_BOOTSTRAP_SERVERS:-dr-kafka-0:9092}"
ADMIN_PROPERTIES="${ADMIN_PROPERTIES:-/etc/kafka/config/admin.properties}"

/opt/kafka-scripts/wait-for-kafka.sh "${SOURCE_BOOTSTRAP_SERVERS}" "${ADMIN_PROPERTIES}"
/opt/kafka-scripts/wait-for-kafka.sh "${TARGET_BOOTSTRAP_SERVERS}" "${ADMIN_PROPERTIES}"

exec /opt/bitnami/kafka/bin/connect-mirror-maker.sh /etc/kafka/config/mm2.properties
