#!/usr/bin/env bash
set -euo pipefail

# Go to repo root (works if this script is in scripts/)
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "Stopping Kafka Connect..."
docker compose down -v

# Optional parameters (override via env or CLI args)
# Usage: S3_BUCKET=my-bucket AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=yyy S3_ENDPOINT=http://minio.local:9000 ./scripts/rebuild-connect.sh
S3_BUCKET="${S3_BUCKET:-}"
AWS_REGION="${AWS_REGION:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
S3_ENDPOINT="${S3_ENDPOINT:-}"

# Required table identifiers (override via env)
ML_DATABASE="${ML_DATABASE:-test_db}"
ML_TABLE="${ML_TABLE:-test_table}"

# REST base for Moonlink
REST_BASE="${REST_BASE:-http://localhost:3030}"

SRC_TABLE_NAME="${ML_DATABASE}.${ML_TABLE}"

echo "Dropping table ${SRC_TABLE_NAME} (if exists) on Moonlink..."
DROP_PAYLOAD=$(cat <<EOF
{ "database": "${ML_DATABASE}", "table": "${ML_TABLE}" }
EOF
)
curl -fsS -X DELETE "${REST_BASE}/tables/${SRC_TABLE_NAME}" \
  -H "content-type: application/json" \
  -d "${DROP_PAYLOAD}" || echo "Drop may have failed or table absent; continuing"

echo "Wiping moonlink data..."
rm -rf moonlink-data/*

echo "Building connector jar..."
mvn -q -DskipTests package

echo "Restarting Kafka Connect..."
docker compose up -d --force-recreate connect moonlink

echo "Waiting for Connect REST API..."
for i in {1..30}; do
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

for i in {1..30}; do
  if docker compose exec -T connect bash -lc "curl -sS http://moonlink:3030/health | cat" > /dev/null; then
    echo "Successfully connected to Moonlink"
    break
  fi
  sleep 1
done

# Create table on Moonlink (after services are up)
echo "Creating table ${SRC_TABLE_NAME} on Moonlink..."

# Default schema for demo; adjust as needed or pass via env VAR ML_SCHEMA_JSON
ML_SCHEMA_JSON=${ML_SCHEMA_JSON:-"[{\"name\":\"string-column\",\"data_type\":\"string\",\"nullable\":false},{\"name\":\"numeric-column\",\"data_type\":\"int32\",\"nullable\":false},{\"name\":\"boolean-column\",\"data_type\":\"boolean\",\"nullable\":true}]"}

if [[ -n "${S3_BUCKET}" && -n "${AWS_REGION}" && -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]]; then
  echo "Using S3 for both warehouse and WAL: bucket=${S3_BUCKET} region=${AWS_REGION} endpoint=${S3_ENDPOINT}"
  # AccessorConfig JSON for S3
  read -r -d '' ACCESSOR_S3 <<JSON || true
{\
  "storage_config": {\
    "s3": {\
      "access_key_id": "${AWS_ACCESS_KEY_ID}",\
      "secret_access_key": "${AWS_SECRET_ACCESS_KEY}",\
      "region": "${AWS_REGION}",\
      "bucket": "${S3_BUCKET}"${S3_ENDPOINT:+,\"endpoint\": \"${S3_ENDPOINT}\"}\
    }\
  }\
}
JSON
  TABLE_CONFIG=$(cat <<EOF
{\
  "mooncake": {\
    "append_only": true,\
    "row_identity": "None"\
  },\
  "iceberg": ${ACCESSOR_S3},\
  "wal": ${ACCESSOR_S3}\
}
EOF
)
else
  echo "Using local filesystem for warehouse and WAL (default)."
  # Leave table_config empty to let backend default to local fs under /tmp/moonlink
  TABLE_CONFIG=$(cat <<EOF
{\
  "mooncake": {\
    "append_only": true,\
    "row_identity": "None"\
  }\
}
EOF
)
fi

CREATE_PAYLOAD=$(cat <<EOF
{\
  "database": "${ML_DATABASE}",\
  "table": "${ML_TABLE}",\
  "schema": ${ML_SCHEMA_JSON},\
  "table_config": ${TABLE_CONFIG}\
}
EOF
)

curl -fsS -X POST "${REST_BASE}/tables/${SRC_TABLE_NAME}" \
  -H "content-type: application/json" \
  -d "${CREATE_PAYLOAD}" | jq -C . | cat

curl -X POST -H "Content-Type:application/json" -d @examples/example-source-connector.json http://localhost:8083/connectors | jq
curl -X POST -H "Content-Type:application/json" -d @examples/moonlink-sink-connector.json http://localhost:8083/connectors | jq

# Check status
# sleep for 2 seconds to ensure the connector is ready
echo "Sleeping for 2 seconds to ensure the connector is ready..."
sleep 2

echo "Checking connector status..."
curl -s http://localhost:8083/connectors/example-source-connector/status | jq -C . | cat
curl -s http://localhost:8083/connectors/moonlink-sink-connector/status | jq -C . | cat