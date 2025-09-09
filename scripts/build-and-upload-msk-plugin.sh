#!/usr/bin/env bash
set -euo pipefail

# Build shaded JAR, package MSK plugin ZIP, generate connector configs, and optionally upload to S3.
#
# Usage:
#   scripts/build-and-upload-msk-plugin.sh [--no-upload]
#
# Requirements:
#   - Java 11+, Maven
#   - jq (for reading profile-config.json)
#   - awscli (if uploading)
#
# Reads:
#   profile-config.json for:
#     - remote.moonlink_uri
#     - source.*
#     - sink.tasks_max
#     - table.{database,name}
#     - table.storage.s3.{bucket,region,access_key_id,secret_access_key}
#
# Outputs:
#   - target/moonlink-source-connector-1.0.jar
#   - build/msk-plugin/moonlink-connector-1.0.zip
#   - build/configs/source-connector.msk.json
#   - build/configs/sink-connector.msk.json

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

NO_UPLOAD=false
if [[ "${1:-}" == "--no-upload" ]]; then
  NO_UPLOAD=true
fi

PROFILE_JSON="$ROOT_DIR/profile-config.json"
if [[ ! -f "$PROFILE_JSON" ]]; then
  echo "profile-config.json not found at $PROFILE_JSON" >&2
  exit 1
fi

# Extract settings from profile-config.json
MOONLINK_URI=$(jq -r .remote.moonlink_uri "$PROFILE_JSON")
SRC_NAME=$(jq -r .source.connector_name "$PROFILE_JSON")
SRC_TASKS=$(jq -r .source.tasks_max "$PROFILE_JSON")
SRC_MPS=$(jq -r .source.messages_per_second "$PROFILE_JSON")
SRC_MSG_SIZE=$(jq -r .source.message_size_bytes "$PROFILE_JSON")
SRC_MAX_SECS=$(jq -r .source.max_duration_seconds "$PROFILE_JSON")
SRC_TOPIC=$(jq -r .source.output_topic "$PROFILE_JSON")
SINK_NAME=$(jq -r .sink.connector_name "$PROFILE_JSON")
DB_NAME=$(jq -r .table.database "$PROFILE_JSON")
TABLE_NAME=$(jq -r .table.name "$PROFILE_JSON")
S3_BUCKET=$(jq -r .table.storage.s3.bucket "$PROFILE_JSON")
S3_REGION=$(jq -r .table.storage.s3.region "$PROFILE_JSON")
AWS_ACCESS_KEY_ID_VAL=$(jq -r .table.storage.s3.access_key_id "$PROFILE_JSON")
AWS_SECRET_ACCESS_KEY_VAL=$(jq -r .table.storage.s3.secret_access_key "$PROFILE_JSON")

if [[ -z "$MOONLINK_URI" || "$MOONLINK_URI" == "null" ]]; then
  echo "remote.moonlink_uri missing in profile-config.json" >&2
  exit 1
fi

# Schema Registry: override here if needed
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-"http://172.31.39.39:8081"}

# 1) Build shaded JAR
mvn -q -DskipTests=true clean package

# 2) Create plugin ZIP
PLUGIN_DIR="$ROOT_DIR/build/msk-plugin/moonlink-kafka-connect"
mkdir -p "$PLUGIN_DIR"
cp "$ROOT_DIR/target/moonlink-source-connector-1.0.jar" "$PLUGIN_DIR/"
(
  cd "$ROOT_DIR/build/msk-plugin"
  zip -q -r moonlink-connector-1.0.zip moonlink-kafka-connect
)

# 3) Generate MSK connector JSON configs
CONFIG_DIR="$ROOT_DIR/build/configs"
mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_DIR/source-connector.msk.json" <<SRCJSON
{
  "name": "$SRC_NAME",
  "config": {
    "connector.class": "example.source.MyFirstKafkaConnector",
    "tasks.max": "$SRC_TASKS",
    "first.required.param": "Kafka",
    "second.required.param": "Connect",
    "task.messages.per.second": "$SRC_MPS",
    "message.size.bytes": "$SRC_MSG_SIZE",
    "task.max.duration.seconds": "$SRC_MAX_SECS",
    "output.topic": "$SRC_TOPIC",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "$SCHEMA_REGISTRY_URL",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "$SCHEMA_REGISTRY_URL"
  }
}
SRCJSON

cat > "$CONFIG_DIR/sink-connector.msk.json" <<SINKJSON
{
  "name": "$SINK_NAME",
  "config": {
    "connector.class": "moonlink.sink.connector.MoonlinkSinkConnector",
    "tasks.max": "1",
    "topics": "$SRC_TOPIC",
    "moonlink.uri": "$MOONLINK_URI",
    "moonlink.table.name": "$TABLE_NAME",
    "moonlink.database.name": "$DB_NAME",
    "schema.registry.url": "$SCHEMA_REGISTRY_URL",
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }
}
SINKJSON

# 4) Optional upload to S3
if [[ "$NO_UPLOAD" == false ]]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "aws CLI not found; attempting to install (Debian/Ubuntu)..." >&2
    sudo apt-get update -y && sudo apt-get install -y awscli
  fi
  export AWS_DEFAULT_REGION="$S3_REGION"
  export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID_VAL"
  export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY_VAL"
  aws s3 cp "$ROOT_DIR/build/msk-plugin/moonlink-connector-1.0.zip" "s3://$S3_BUCKET/msk-plugins/moonlink-connector-1.0.zip" --only-show-errors
  echo "Uploaded: s3://$S3_BUCKET/msk-plugins/moonlink-connector-1.0.zip"
else
  echo "Skipping upload (--no-upload). Artifact at build/msk-plugin/moonlink-connector-1.0.zip"
fi

# 5) Print summary
cat <<EOM

Artifacts:
- Shaded JAR: $ROOT_DIR/target/moonlink-source-connector-1.0.jar
- Plugin ZIP: $ROOT_DIR/build/msk-plugin/moonlink-connector-1.0.zip
- Source config: $CONFIG_DIR/source-connector.msk.json
- Sink config:   $CONFIG_DIR/sink-connector.msk.json
EOM

