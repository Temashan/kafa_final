#!/bin/bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTORS_DIR="${CONNECTORS_DIR:-/etc/kafka-connect/config}"

echo "Waiting for Kafka Connect..."

until curl -s -f "$CONNECT_URL/connectors" > /dev/null; do
  sleep 2
done

echo "Kafka Connect is ready"

for CONFIG_FILE in "$CONNECTORS_DIR"/*.json; do
  if [ ! -f "$CONFIG_FILE" ]; then
    echo "No connector json files in $CONNECTORS_DIR"
    break
  fi

  CONNECTOR_NAME=$(sed -n 's/^[[:space:]]*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$CONFIG_FILE" | head -n 1)
  if [ -z "$CONNECTOR_NAME" ]; then
    echo "Skip file without connector name: $CONFIG_FILE"
    continue
  fi

  echo "Deploying $CONNECTOR_NAME"

  if curl -s -f "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null; then
    echo "Updating $CONNECTOR_NAME"

    curl -sS -X PUT \
      -H "Content-Type: application/json" \
      "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
      --data "@$CONFIG_FILE"
  else
    echo "Creating $CONNECTOR_NAME"

    curl -sS -X POST \
      -H "Content-Type: application/json" \
      "$CONNECT_URL/connectors" \
      --data "@$CONFIG_FILE"
  fi

  echo "Done $CONNECTOR_NAME"
done
