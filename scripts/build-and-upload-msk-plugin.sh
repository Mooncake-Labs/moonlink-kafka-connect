#!/usr/bin/env bash
set -euo pipefail

# Build shaded JARs (loadgen + sink), package MSK plugin ZIPs, generate connector configs, and optionally upload to S3.
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
#   - target/<artifactId>-<version>-loadgen.jar
#   - target/<artifactId>-<version>-sink.jar
#   - build/msk-plugin/moonlink-loadgen-connector-<version>.zip
#   - build/msk-plugin/moonlink-sink-connector-<version>.zip
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
SRC_RUN_INDEFINITE=$(jq -r '.source.run_indefinitely // false' "$PROFILE_JSON")
SRC_MAX_SECS=$(jq -r .source.max_duration_seconds "$PROFILE_JSON")
SRC_TOPIC=$(jq -r .source.output_topic "$PROFILE_JSON")
SINK_NAME=$(jq -r .sink.connector_name "$PROFILE_JSON")
SINK_TASKS=$(jq -r .sink.tasks_max "$PROFILE_JSON")
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

# Schema Registry: read from profile-config.json, allow env override, fallback to example default
PROFILE_SR_URL=$(jq -r .remote.schema_registry_url "$PROFILE_JSON" 2>/dev/null || echo "null")
if [[ -z "${SCHEMA_REGISTRY_URL:-}" ]]; then
  if [[ -n "$PROFILE_SR_URL" && "$PROFILE_SR_URL" != "null" ]]; then
    SCHEMA_REGISTRY_URL="$PROFILE_SR_URL"
  else
    SCHEMA_REGISTRY_URL="http://172.31.39.39:8081"
  fi
fi

# 1) Build shaded JARs
mvn -q -DskipTests=true clean package

# Resolve artifact metadata for dynamic naming
ARTIFACT_ID=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.artifactId)
PROJECT_VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)

LOADGEN_JAR="$ROOT_DIR/target/${ARTIFACT_ID}-${PROJECT_VERSION}-loadgen.jar"
SINK_JAR="$ROOT_DIR/target/${ARTIFACT_ID}-${PROJECT_VERSION}-sink.jar"

if [[ ! -f "$LOADGEN_JAR" || ! -f "$SINK_JAR" ]]; then
  echo "Expected shaded jars not found. Looked for:" >&2
  echo "  $LOADGEN_JAR" >&2
  echo "  $SINK_JAR" >&2
  exit 1
fi

# Strip META-INF signatures from shaded jars (belt-and-suspenders for MSK)
zip -q -d "$LOADGEN_JAR" "META-INF/*.SF" "META-INF/*.DSA" "META-INF/*.RSA" 2>/dev/null || true
zip -q -d "$SINK_JAR" "META-INF/*.SF" "META-INF/*.DSA" "META-INF/*.RSA" 2>/dev/null || true

# 2) Create plugin ZIPs
PLUGIN_BASE_DIR="$ROOT_DIR/build/msk-plugin"
mkdir -p "$PLUGIN_BASE_DIR"

LOADGEN_PLUGIN_DIR="$PLUGIN_BASE_DIR/moonlink-kafka-connect-loadgen"
SINK_PLUGIN_DIR="$PLUGIN_BASE_DIR/moonlink-kafka-connect-sink"

rm -rf "$LOADGEN_PLUGIN_DIR" "$SINK_PLUGIN_DIR"
mkdir -p "$LOADGEN_PLUGIN_DIR" "$SINK_PLUGIN_DIR"

cp "$LOADGEN_JAR" "$LOADGEN_PLUGIN_DIR/"
cp "$SINK_JAR" "$SINK_PLUGIN_DIR/"

LOADGEN_ZIP="$PLUGIN_BASE_DIR/moonlink-loadgen-connector-${PROJECT_VERSION}.zip"
SINK_ZIP="$PLUGIN_BASE_DIR/moonlink-sink-connector-${PROJECT_VERSION}.zip"
(
  cd "$PLUGIN_BASE_DIR"
  zip -q -r "$(basename "$LOADGEN_ZIP")" "$(basename "$LOADGEN_PLUGIN_DIR")"
  zip -q -r "$(basename "$SINK_ZIP")" "$(basename "$SINK_PLUGIN_DIR")"
)

# 3) Generate MSK connector JSON configs
CONFIG_DIR="$ROOT_DIR/build/configs"
mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_DIR/source-connector.msk.json" <<SRCJSON
{
  "name": "$SRC_NAME",
  "config": {
    "connector.class": "example.loadgen.LoadGeneratorConnector",
    "tasks.max": "$SRC_TASKS",
    "task.messages.per.second": "$SRC_MPS",
    "message.size.bytes": "$SRC_MSG_SIZE",
    "task.max.duration.seconds": "$SRC_MAX_SECS",
    "task.run.indefinitely": "$SRC_RUN_INDEFINITE",
    "output.topic": "$SRC_TOPIC",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
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
    "tasks.max": "$SINK_TASKS",
    "topics": "$SRC_TOPIC",
    "moonlink.uri": "$MOONLINK_URI",
    "moonlink.table.name": "$TABLE_NAME",
    "moonlink.database.name": "$DB_NAME",
    "schema.registry.url": "$SCHEMA_REGISTRY_URL",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
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
  aws s3 cp "$LOADGEN_ZIP" "s3://$S3_BUCKET/msk-plugins/$(basename "$LOADGEN_ZIP")" --only-show-errors
  aws s3 cp "$SINK_ZIP" "s3://$S3_BUCKET/msk-plugins/$(basename "$SINK_ZIP")" --only-show-errors
  echo "Uploaded: s3://$S3_BUCKET/msk-plugins/$(basename "$LOADGEN_ZIP")"
  echo "Uploaded: s3://$S3_BUCKET/msk-plugins/$(basename "$SINK_ZIP")"
else
  echo "Skipping upload (--no-upload). Artifacts at:"
  echo "  $LOADGEN_ZIP"
  echo "  $SINK_ZIP"
fi

# 5) Print summary
cat <<EOM

Artifacts:
- Shaded JAR (loadgen): $LOADGEN_JAR
- Shaded JAR (sink):    $SINK_JAR
- Plugin ZIP (loadgen): $LOADGEN_ZIP
- Plugin ZIP (sink):    $SINK_ZIP
- Source config: $CONFIG_DIR/source-connector.msk.json
- Sink config:   $CONFIG_DIR/sink-connector.msk.json
EOM

