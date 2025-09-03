#!/usr/bin/env bash
set -euo pipefail

# Go to repo root (works if this script is in scripts/)
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "Stopping Kafka Connect..."
docker compose down -v

echo "Wiping moonlink data..."
rm -rf moonlink-data/*

echo "Building connector jar..."
mvn -q -DskipTests package

echo "Restarting Kafka Connect..."
docker compose up -d --force-recreate connect moonlink

echo "Waiting for Connect REST API..."
for i in {1..60}; do
  if curl -fsS http://localhost:8083/ > /dev/null; then
    break
  fi
  sleep 1
done

echo "Available connector plugins:"
if command -v jq >/dev/null 2>&1; then
  curl -fsS http://localhost:8083/connector-plugins | jq
else
  curl -fsS http://localhost:8083/connector-plugins
fi

echo "Checking if moonlink is running..."

for i in {1..60}; do
  if docker compose exec -T connect bash -lc "curl -sS http://moonlink:3030/health | cat" > /dev/null; then
    echo "Successfully connected to Moonlink"
    break
  fi
  sleep 1
done

curl -X POST -H "Content-Type:application/json" -d @examples/example-source-connector.json http://localhost:8083/connectors | jq
curl -X POST -H "Content-Type:application/json" -d @examples/moonlink-sink-connector.json http://localhost:8083/connectors | jq

# Check status
# sleep for 2 seconds to ensure the connector is ready
echo "Sleeping for 2 seconds to ensure the connector is ready..."
sleep 2

echo "Checking connector status..."
curl -s http://localhost:8083/connectors/example-source-connector/status | jq -C . | cat
curl -s http://localhost:8083/connectors/moonlink-sink-connector/status | jq -C . | cat