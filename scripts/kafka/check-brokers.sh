#!/usr/bin/env sh
set -eu

PRIMARY_BOOTSTRAP_SERVERS="${PRIMARY_BOOTSTRAP_SERVERS:-kafka-0:9092,kafka-1:9092,kafka-2:9092}"
DR_BOOTSTRAP_SERVERS="${DR_BOOTSTRAP_SERVERS:-dr-kafka-0:9092}"
ADMIN_PROPERTIES="${ADMIN_PROPERTIES:-/etc/kafka/config/admin.properties}"
TOPIC_NAME="${TOPIC_NAME:-products.raw}"

metadata_quorum_bin="/opt/bitnami/kafka/bin/kafka-metadata-quorum.sh"
topics_bin="/opt/bitnami/kafka/bin/kafka-topics.sh"
versions_bin="/opt/bitnami/kafka/bin/kafka-broker-api-versions.sh"

cluster_check() {
  CLUSTER_NAME="$1"
  BOOTSTRAP_SERVERS="$2"

  echo
  echo "=== ${CLUSTER_NAME} cluster ==="
  echo "Bootstrap servers: ${BOOTSTRAP_SERVERS}"

  "${versions_bin}" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_PROPERTIES}"

  echo
  echo "--- Metadata quorum ---"
  "${metadata_quorum_bin}" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_PROPERTIES}" \
    describe \
    --status

  echo
  echo "--- Topics ---"
  TOPICS="$("${topics_bin}" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_PROPERTIES}" \
    --list)"

  if [ -n "${TOPICS}" ]; then
    printf '%s\n' "${TOPICS}"
  else
    echo "No topics found"
  fi

  echo
  echo "--- Topic ${TOPIC_NAME} ---"
  if printf '%s\n' "${TOPICS}" | grep -Fx "${TOPIC_NAME}" >/dev/null 2>&1; then
    "${topics_bin}" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --command-config "${ADMIN_PROPERTIES}" \
      --describe \
      --topic "${TOPIC_NAME}"
  else
    echo "Topic ${TOPIC_NAME} does not exist on ${CLUSTER_NAME}"
  fi
}

cluster_check "primary" "${PRIMARY_BOOTSTRAP_SERVERS}"
cluster_check "dr" "${DR_BOOTSTRAP_SERVERS}"
