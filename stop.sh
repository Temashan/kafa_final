#!/usr/bin/env sh
set -eu

#docker compose down
docker compose down --remove-orphans --volumes || true

echo "Kafka lab is stopped"
