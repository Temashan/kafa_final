#!/usr/bin/env sh
set -eu

BOOTSTRAP_SERVERS="${1:-${BOOTSTRAP_SERVERS:-}}"
ADMIN_PROPERTIES="${2:-${ADMIN_PROPERTIES:-}}"
MAX_RETRIES="${MAX_RETRIES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

: "${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS is required}"
: "${ADMIN_PROPERTIES:?ADMIN_PROPERTIES is required}"

for attempt in $(seq 1 "${MAX_RETRIES}"); do
  if /opt/bitnami/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_PROPERTIES}" >/dev/null 2>&1; then
    echo "Kafka is reachable via ${BOOTSTRAP_SERVERS}"
    exit 0
  fi

  echo "Waiting for Kafka (${attempt}/${MAX_RETRIES}) ..."
  sleep "${SLEEP_SECONDS}"
done

echo "Kafka bootstrap timeout for ${BOOTSTRAP_SERVERS}" >&2
exit 1
