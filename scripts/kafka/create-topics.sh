#!/usr/bin/env sh
set -eu

BOOTSTRAP_SERVERS="${1:-${BOOTSTRAP_SERVERS:-}}"
ADMIN_PROPERTIES="${2:-${ADMIN_PROPERTIES:-}}"
TOPIC_NAME="${TOPIC_NAME:-products.raw}"
VALIDATED_TOPIC_NAME="${VALIDATED_TOPIC_NAME:-products.validated}"
REJECTED_TOPIC_NAME="${REJECTED_TOPIC_NAME:-products.rejected}"
RECOMMENDATIONS_TOPIC_NAME="${RECOMMENDATIONS_TOPIC_NAME:-recommendations}"
CLIENT_REQUESTS_TOPIC_NAME="${CLIENT_REQUESTS_TOPIC_NAME:-client.requests}"
BANNED_TOPIC_NAME="${BANNED_TOPIC_NAME:-products.banned}"
BANNED_PRODUCTS_CHANGELOG_TOPIC="${BANNED_PRODUCTS_CHANGELOG_TOPIC:-products-filter-group-banned_products-changelog}"
FAUST_GROUP="${FILTER_GROUP_NAME:-products-filter-group}"
FAUST_ASSIGNOR_TOPIC="${FAUST_GROUP}-__assignor__leader"
PARTITIONS="${PARTITIONS:-3}"
CLIENT_REQUESTS_PARTITIONS="${CLIENT_REQUESTS_PARTITIONS:-3}"
RECOMMENDATIONS_PARTITIONS="${RECOMMENDATIONS_PARTITIONS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
MIN_INSYNC_REPLICAS="${MIN_INSYNC_REPLICAS:-2}"
MM2_CONFIG_TOPIC="${MM2_CONFIG_TOPIC:-mm2-configs.internal}"
MM2_OFFSET_TOPIC="${MM2_OFFSET_TOPIC:-mm2-offsets.internal}"
MM2_STATUS_TOPIC="${MM2_STATUS_TOPIC:-mm2-status.internal}"
MAX_RETRIES="${MAX_RETRIES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-3}"

: "${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS is required}"
: "${ADMIN_PROPERTIES:?ADMIN_PROPERTIES is required}"

create_topic() {
  if [ "$#" -lt 4 ]; then
    echo "create_topic requires at least 4 arguments" >&2
    return 1
  fi

  topic="$1"
  partitions="$2"
  replication_factor="$3"
  cleanup_policy="$4"
  shift 4

  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_PROPERTIES}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${replication_factor}" \
    --config "cleanup.policy=${cleanup_policy}" \
    "$@"
}

describe_topic() {
  topic="$1"

  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_PROPERTIES}" \
    --describe \
    --topic "${topic}"
}

for attempt in $(seq 1 "${MAX_RETRIES}"); do
  if create_topic "${TOPIC_NAME}" "${PARTITIONS}" "${REPLICATION_FACTOR}" delete --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${VALIDATED_TOPIC_NAME}" "${PARTITIONS}" "${REPLICATION_FACTOR}" delete --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${REJECTED_TOPIC_NAME}" "${PARTITIONS}" "${REPLICATION_FACTOR}" delete --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${RECOMMENDATIONS_TOPIC_NAME}" "${RECOMMENDATIONS_PARTITIONS}" "${REPLICATION_FACTOR}" delete --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${CLIENT_REQUESTS_TOPIC_NAME}" "${CLIENT_REQUESTS_PARTITIONS}" "${REPLICATION_FACTOR}" delete --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${MM2_CONFIG_TOPIC}" 1 3 compact \
    && create_topic "${MM2_OFFSET_TOPIC}" 25 3 compact \
    && create_topic "${MM2_STATUS_TOPIC}" 5 3 compact \
    && create_topic "${BANNED_TOPIC_NAME}" 1 "${REPLICATION_FACTOR}" compact --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${BANNED_PRODUCTS_CHANGELOG_TOPIC}" 1 "${REPLICATION_FACTOR}" compact --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "${FAUST_ASSIGNOR_TOPIC}" 1 "${REPLICATION_FACTOR}" compact --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}" \
    && create_topic "_schemas" 1 "${REPLICATION_FACTOR}" compact --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}"; then
    break
  fi

  if [ "${attempt}" = "${MAX_RETRIES}" ]; then
    echo "Application topics were not created after ${MAX_RETRIES} attempts" >&2
    exit 1
  fi

  echo "Topic creation is waiting for the full Kafka quorum (${attempt}/${MAX_RETRIES}) ..."
  sleep "${SLEEP_SECONDS}"
done

describe_topic "${TOPIC_NAME}"
describe_topic "${VALIDATED_TOPIC_NAME}"
describe_topic "${REJECTED_TOPIC_NAME}"
describe_topic "${RECOMMENDATIONS_TOPIC_NAME}"
describe_topic "${CLIENT_REQUESTS_TOPIC_NAME}"
describe_topic "${MM2_CONFIG_TOPIC}"
describe_topic "${MM2_OFFSET_TOPIC}"
describe_topic "${MM2_STATUS_TOPIC}"
describe_topic "${BANNED_TOPIC_NAME}"
describe_topic "${BANNED_PRODUCTS_CHANGELOG_TOPIC}"
describe_topic "${FAUST_ASSIGNOR_TOPIC}"
describe_topic "_schemas"
