#!/usr/bin/env sh
set -eu

BOOTSTRAP_SERVERS="${1:-${BOOTSTRAP_SERVERS:-}}"
ADMIN_PROPERTIES="${2:-${ADMIN_PROPERTIES:-}}"

RAW_TOPIC="${TOPIC_NAME:-products.raw}"
VALIDATED_TOPIC="${VALIDATED_TOPIC_NAME:-products.validated}"
REJECTED_TOPIC="${REJECTED_TOPIC_NAME:-products.rejected}"
BANNED_TOPIC="${BANNED_TOPIC_NAME:-products.banned}"
BANNED_CHANGELOG_TOPIC="${BANNED_PRODUCTS_CHANGELOG_TOPIC:-products-filter-group-banned_products-changelog}"

FAUST_GROUP="${FILTER_GROUP_NAME:-products-filter-group}"
CONSUMER_GROUP="${GROUP_NAME:-products-consumer-group}"
FAUST_ASSIGNOR_TOPIC="${FAUST_GROUP}-__assignor__leader"

MAX_RETRIES="${MAX_RETRIES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-3}"

: "${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS is required}"
: "${ADMIN_PROPERTIES:?ADMIN_PROPERTIES is required}"

apply_acls() {

  echo "=== PRODUCER ==="

  # пишем raw
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:producer \
    --operation Write --operation Describe \
    --topic "$RAW_TOPIC"

  # пишем banned (CLI/admin)
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:producer \
    --operation Write --operation Describe \
    --topic "$BANNED_TOPIC"


  echo "=== FAUST SERVICE ==="

  # читаем raw
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Read --operation Describe \
    --topic "$RAW_TOPIC"

  # читаем banned
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Read --operation Describe \
    --topic "$BANNED_TOPIC"

  # пишем validated / rejected
  for topic in "$VALIDATED_TOPIC" "$REJECTED_TOPIC"; do
    kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
      --add --allow-principal User:faust \
      --operation Write --operation Describe \
      --topic "$topic"
  done

  # FAUST INTERNAL TOPICS
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Read \
    --group "$FAUST_GROUP"

  # consumer group FAUST INTERNAL TOPICS
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Read --operation Write --operation Describe \
    --topic "products-stream-*"


  echo "=== FAUST EXTRA FIXES ==="

  # changelog topic (Table)
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Read --operation Write --operation Describe \
    --topic "$BANNED_CHANGELOG_TOPIC"

  # assignor (rebalance)
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Read --operation Write --operation Describe \
    --topic "$FAUST_ASSIGNOR_TOPIC"

  # describe configs (иногда нужен Faust)
  for topic in "$RAW_TOPIC" "$BANNED_TOPIC"; do
    kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
      --add --allow-principal User:faust \
      --operation DescribeConfigs \
      --topic "$topic"
  done

  # cluster metadata (безопасно добавить)
  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:faust \
    --operation Describe \
    --cluster


  echo "=== FINAL CONSUMER ==="

  for topic in "$VALIDATED_TOPIC" "$REJECTED_TOPIC"; do
    kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
      --add --allow-principal User:consumer \
      --operation Read --operation Describe \
      --topic "$topic"
  done

  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:consumer \
    --operation Read \
    --group "$CONSUMER_GROUP"


  echo "=== SCHEMA REGISTRY ==="

  kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_PROPERTIES" \
    --add --allow-principal User:ANONYMOUS \
    --operation Read --operation Write --operation Describe \
    --topic _schemas
}


for attempt in $(seq 1 "$MAX_RETRIES"); do
  if apply_acls; then
    echo "ACLs applied successfully"
    break
  fi

  if [ "$attempt" = "$MAX_RETRIES" ]; then
    echo "ACLs failed"
    exit 1
  fi

  echo "Waiting authorizer... ($attempt/$MAX_RETRIES)"
  sleep "$SLEEP_SECONDS"
done


echo "=== CURRENT ACLs ==="

kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --command-config "$ADMIN_PROPERTIES" \
  --list
