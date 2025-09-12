#!/usr/bin/env bash
set -euo pipefail
set -o errtrace

# Better error visibility
on_err() {
  local ec=$?
  local line=${BASH_LINENO[0]:-unknown}
  local cmd=${BASH_COMMAND:-unknown}
  echo "\n[ERROR] Script failed (exit=${ec}) at line ${line}" >&2
  echo "[ERROR] Command: ${cmd}" >&2
  docker compose ps || true
  echo "-- tail moonlink_service.log (if present) --"; tail -n 80 moonlink_service.log 2>/dev/null || true
}
trap on_err ERR

# Cleanup temp files even on error/exit
cleanup() {
  rm -f .df_count_output.log .duckdb_count_output.txt .duckdb_count_output.csv 2>/dev/null || true
  [[ -n "${DROP_TMP:-}" ]] && rm -f "${DROP_TMP}" || true
  [[ -n "${CREATE_TMP:-}" ]] && rm -f "${CREATE_TMP}" || true
  [[ -n "${SNAP_TMP:-}" ]] && rm -f "${SNAP_TMP}" || true
  [[ -n "${DUCKDB_SQL:-}" ]] && rm -f "${DUCKDB_SQL}" || true
}
trap cleanup EXIT

CURL_TIMEOUT_OPTS=(--connect-timeout 5 --max-time 30)

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CONFIG_PATH="${1:-${CONFIG:-}}"

if [[ -n "${CONFIG_PATH}" ]]; then
  if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required to read config ${CONFIG_PATH}" >&2
    exit 1
  fi
  # New schema only
  IS_LOCAL_EXECUTION=$(jq -r '.is_local_execution' "${CONFIG_PATH}")
  URI_MOONLINK=$(jq -r '.uris.moonlink' "${CONFIG_PATH}")
  URI_SCHEMA_REGISTRY=$(jq -r '.uris.schema_registry' "${CONFIG_PATH}")
  # Moonlink configuration block (new), with backward-compat fallback for data dir
  MOONLINK_DATA_DIR=$(jq -r '.local_moonlink.data_dir' "${CONFIG_PATH}")
  MOONLINK_FEATURES=$(jq -r '(.local_moonlink.build.features // []) | join(" ")' "${CONFIG_PATH}")
  MOONLINK_RELEASE=$(jq -r '.local_moonlink.build.release // false' "${CONFIG_PATH}")
  MOONLINK_DEBUG_RUN=$(jq -r '.local_moonlink.run.debug' "${CONFIG_PATH}")
  MOONLINK_OUT_LOG=$(jq -r '.local_moonlink.run.out_log_path' "${CONFIG_PATH}")
  MOONLINK_WIPE_DATA=$(jq -r '.local_moonlink.run.wipe_data_dir' "${CONFIG_PATH}")
  # Collect runtime extra args as newline-separated, to be built into an array later
  MOONLINK_RUN_ARGS_NL=$(jq -r '.local_moonlink.run.args // [] | .[]' "${CONFIG_PATH}" 2>/dev/null || true)
  ML_DATABASE=$(jq -r '.table.database' "${CONFIG_PATH}")
  ML_TABLE=$(jq -r '.table.name' "${CONFIG_PATH}")
  ML_SHOULD_RECREATE=$(jq -r '.table.should_recreate // true' "${CONFIG_PATH}")
  SOURCE_NAME=$(jq -r '.source.connector_name // "example-source-connector"' "${CONFIG_PATH}")
  SINK_NAME=$(jq -r '.sink.connector_name // "moonlink-sink-connector"' "${CONFIG_PATH}")
  SOURCE_TASKS_MAX=$(jq -r '.source.tasks_max' "${CONFIG_PATH}")
  SOURCE_PARTITIONS=$(jq -r '.source.partitions // 1' "${CONFIG_PATH}")
  SOURCE_MSGS_PER_SEC=$(jq -r '.source.messages_per_second' "${CONFIG_PATH}")
  SOURCE_MSG_SIZE_BYTES=$(jq -r '.source.message_size_bytes' "${CONFIG_PATH}")
  SOURCE_MAX_DURATION=$(jq -r '.source.max_duration_seconds' "${CONFIG_PATH}")
  SOURCE_RUN_INDEFINITELY=$(jq -r '.source.run_indefinitely // false' "${CONFIG_PATH}")
  SOURCE_TOPIC=$(jq -r '.source.output_topic' "${CONFIG_PATH}")
  SINK_TASKS_MAX=$(jq -r '.sink.tasks_max' "${CONFIG_PATH}")
  SCHEMA_JSON_COMPACT=$(jq -c '.table.schema' "${CONFIG_PATH}")
  PROFILING_CSV_PATH=$(jq -r '.profiling.csv_path // empty' "${CONFIG_PATH}")
  PROFILING_LOG_PATH=$(jq -r '.profiling.log_path // empty' "${CONFIG_PATH}")
  # Optional S3
  S3_BUCKET=$(jq -r '.table.storage.s3.bucket // empty' "${CONFIG_PATH}" || true)
  AWS_REGION=$(jq -r '.table.storage.s3.region // empty' "${CONFIG_PATH}" || true)
  AWS_ACCESS_KEY_ID=$(jq -r '.table.storage.s3.access_key_id // empty' "${CONFIG_PATH}" || true)
  AWS_SECRET_ACCESS_KEY=$(jq -r '.table.storage.s3.secret_access_key // empty' "${CONFIG_PATH}" || true)
  S3_ENDPOINT=$(jq -r '.table.storage.s3.endpoint // empty' "${CONFIG_PATH}" || true)
  __PM_SHOULD_DROP=$(jq -r '.table.drop_table_on_completion // true' "${CONFIG_PATH}")
else
  echo "ERROR: provide a config path as first arg" >&2
  exit 1
fi

# Validate partitions >= sink tasks and ensure partitions
if [[ -n "${SOURCE_TOPIC}" && -n "${SINK_TASKS_MAX}" ]]; then
  if [[ "${SOURCE_PARTITIONS}" -lt "${SINK_TASKS_MAX}" ]]; then
    echo "ERROR: source.partitions (${SOURCE_PARTITIONS}) must be >= sink.tasks_max (${SINK_TASKS_MAX})." >&2
    exit 1
  fi
fi

# Default/local execution toggle (env override allowed when no config)
LOCAL_MOONLINK="${LOCAL_MOONLINK:-false}"

# Compute Moonlink URIs for host (devcontainer) and Connect container
if [[ "${IS_LOCAL_EXECUTION}" == "true" ]]; then
  MOONLINK_URI_HOST="http://localhost:3030"
  MOONLINK_URI_CONNECT="http://host.docker.internal:3030"
  echo "Moonlink execution mode: local"
else
  MOONLINK_URI_HOST="${URI_MOONLINK}"
  MOONLINK_URI_CONNECT="${URI_MOONLINK}"
  echo "Moonlink execution mode: remote (${URI_MOONLINK})"
fi

S3_BUCKET="${S3_BUCKET:-${S3_BUCKET:-}}"
AWS_REGION="${AWS_REGION:-${AWS_REGION:-}}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-}}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-}}"
S3_ENDPOINT="${S3_ENDPOINT:-${S3_ENDPOINT:-}}"

ML_DATABASE="${ML_DATABASE:-${ML_DATABASE:-test_db}}"
ML_TABLE="${ML_TABLE:-${ML_TABLE:-test_table}}"
SRC_TABLE_NAME="${ML_DATABASE}.${ML_TABLE}"

# Connector names and params (fallbacks if no config)
SOURCE_NAME="${SOURCE_NAME:-example-source-connector}"
SINK_NAME="${SINK_NAME:-moonlink-sink-connector}"
SOURCE_TASKS_MAX="${SOURCE_TASKS_MAX:-1}"
SOURCE_PARTITIONS="${SOURCE_PARTITIONS:-1}"
SOURCE_MSGS_PER_SEC="${SOURCE_MSGS_PER_SEC:-60}"
SOURCE_MSG_SIZE_BYTES="${SOURCE_MSG_SIZE_BYTES:-200}"
SOURCE_MAX_DURATION="${SOURCE_MAX_DURATION:-10}"
SOURCE_RUN_INDEFINITELY="${SOURCE_RUN_INDEFINITELY:-false}"
SOURCE_TOPIC="${SOURCE_TOPIC:-source-1}"
SINK_TASKS_MAX="${SINK_TASKS_MAX:-1}"
RECREATE="${RECREATE:-${ML_SHOULD_RECREATE}}"
PROFILING_CSV_PATH="${PROFILING_CSV_PATH:-lsn_snapshot_latency.csv}"
PROFILING_LOG_PATH="${PROFILING_LOG_PATH:-profiling_prints.log}"

echo "Building connector jar..."
mkdir -p .mvn_tmp || true
export MAVEN_OPTS="${MAVEN_OPTS:-} -Djava.io.tmpdir=$(pwd)/.mvn_tmp"
mvn -q -DskipTests package

# Sanitize plugin JARs: strip any signature files that can trip Connect's verifier
# echo "Sanitizing plugin JARs (stripping META-INF signatures)..."
# find target -maxdepth 1 -type f -name "*.jar" ! -name "original-*" -print | while read -r J; do
#   zip -q -d "$J" "META-INF/*.SF" "META-INF/*.DSA" "META-INF/*.RSA" 2>/dev/null || true
# done

echo "Stopping any existing Compose services (keeping schema-registry up)..."
docker compose stop kafka connect || true
docker compose rm -f -v kafka connect || true

# Prepare shared plugin directory BEFORE starting Connect so it picks them up at boot
echo "Preparing shared plugin directory (.connect-plugins) for Connect..."
PLUGIN_HOST_DIR=".connect-plugins"
mkdir -p "${PLUGIN_HOST_DIR}/moonlink-kafka-connect-loadgen" "${PLUGIN_HOST_DIR}/moonlink-kafka-connect-sink"

# Identify only shaded loadgen and sink JARs
ARTIFACT_ID=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.artifactId)
PROJECT_VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
LOADGEN_JAR="target/${ARTIFACT_ID}-${PROJECT_VERSION}-loadgen.jar"
SINK_JAR="target/${ARTIFACT_ID}-${PROJECT_VERSION}-sink.jar"

if [[ -f "${LOADGEN_JAR}" ]]; then cp -f "${LOADGEN_JAR}" "${PLUGIN_HOST_DIR}/moonlink-kafka-connect-loadgen/"; fi
if [[ -f "${SINK_JAR}" ]]; then cp -f "${SINK_JAR}" "${PLUGIN_HOST_DIR}/moonlink-kafka-connect-sink/"; fi

echo "Starting local Kafka+Connect"
docker compose up -d --force-recreate kafka connect
docker compose up -d schema-registry

# Connect REST is available on the devcontainer localhost when running compose here
CONNECT_BASE="http://localhost:8083"
echo "Using Connect REST at ${CONNECT_BASE}"
echo "Verifying custom plugins are loaded..."
plugins_detected=false
for i in {1..30}; do
  PLUGINS_JSON=$(curl -sS "${CONNECT_BASE}/connector-plugins" || true)
  if echo "${PLUGINS_JSON}" | grep -q 'example.loadgen.LoadGeneratorConnector' && echo "${PLUGINS_JSON}" | grep -q 'moonlink.sink.connector.MoonlinkSinkConnector'; then
    echo "Custom plugins detected."
    plugins_detected=true
    break
  fi
  sleep 2
done
if [[ "${plugins_detected}" != "true" ]]; then
  echo "ERROR: Custom plugins were not detected in Connect after waiting. Aborting." >&2
  echo "Last /connector-plugins response:" >&2
  echo "${PLUGINS_JSON}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat) >&2
  exit 1
fi

if [[ "${IS_LOCAL_EXECUTION}" == "true" ]]; then
  echo "Local mode enabled: Building moonlink_service"
  # Build Moonlink locally inside devcontainer and start it on port 3030
  (
    set -e
    cd moonlink
    echo "Building moonlink_service"
    # Build flags
    cargo_args=(build --bin moonlink_service)
    if [[ "${MOONLINK_RELEASE}" == "true" ]]; then
      cargo_args+=(--release)
    fi
    if [[ -n "${MOONLINK_FEATURES}" ]]; then
      cargo_args+=(--features "${MOONLINK_FEATURES}")
    fi
    echo "Running: cargo ${cargo_args[*]}"
    cargo "${cargo_args[@]}" >/dev/null

    # Also build the moonlink_datafusion CLI used for read-only verification
    df_args=(build --bin moonlink_datafusion)
    if [[ "${MOONLINK_RELEASE}" == "true" ]]; then
      df_args+=(--release)
    fi
    if [[ -n "${MOONLINK_FEATURES}" ]]; then
      df_args+=(--features "${MOONLINK_FEATURES}")
    fi
    echo "Running: cargo ${df_args[*]}"
    cargo "${df_args[@]}" >/dev/null
    # Use configured data dir (relative to repo root) and map to moonlink/ when invoking
    DATA_DIR_REL="${MOONLINK_DATA_DIR}"

    # Stop any previously started local moonlink_service
    if [[ -f /tmp/moonlink_service.pid ]]; then
      kill "$(cat /tmp/moonlink_service.pid)" 2>/dev/null || true
      rm -f /tmp/moonlink_service.pid || true
    fi
      # Optionally wipe data directory
    if [[ "${MOONLINK_WIPE_DATA}" == "true" ]]; then
      echo "Wiping Moonlink data directory: ../${DATA_DIR_REL}"
      if [[ -n "${DATA_DIR_REL}" && "${DATA_DIR_REL}" != "/" && "${DATA_DIR_REL}" != "." ]]; then
        rm -rf "../${DATA_DIR_REL}" || true
      else
        echo "ERROR: Refusing to wipe unsafe data dir path: '${DATA_DIR_REL}'" >&2
        exit 1
      fi
    fi
    mkdir -p "../${DATA_DIR_REL}"
    echo "Starting moonlink_service locally on 0.0.0.0:3030..."
    # Determine binary path based on release flag
    BIN_PATH=./target/debug/moonlink_service
    if [[ "${MOONLINK_RELEASE}" == "true" ]]; then BIN_PATH=./target/release/moonlink_service; fi
    # Build runtime args
    run_args=("../${DATA_DIR_REL}")
    # Append extra args from config (space-safe)
    while IFS= read -r line; do
      [[ -n "$line" ]] && run_args+=("$line")
    done <<< "${MOONLINK_RUN_ARGS_NL}"
    # Optional verbose logging when debug=true
    if [[ "${MOONLINK_DEBUG_RUN}" == "true" ]]; then
      export RUST_LOG=trace
    fi
    # Ensure log directory exists
    mkdir -p "$(dirname "../${MOONLINK_OUT_LOG}")"
    nohup "${BIN_PATH}" "${run_args[@]}" \
      > "../${MOONLINK_OUT_LOG}" 2>&1 & echo $! > /tmp/moonlink_service.pid
  )
fi
echo "Using Moonlink REST at ${MOONLINK_URI_HOST}"


echo "Checking for Moonlink health from the host: ${MOONLINK_URI_HOST}..."
for i in {1..60}; do
  if curl -fsS "${MOONLINK_URI_HOST}/health" >/dev/null 2>&1; then
    echo "Moonlink responded from host."
    break
  fi
  sleep 1
done

if [[ "${IS_LOCAL_EXECUTION}" == "true" ]]; then
  echo "Waiting for Moonlink to respond from within the Connect container: ${MOONLINK_URI_CONNECT}..."
  for i in {1..60}; do
    if docker compose exec -T connect bash -lc "curl -fsS ${MOONLINK_URI_CONNECT}/health >/dev/null 2>&1"; then
      echo "Moonlink responded from connect container."
      break
    fi
    sleep 1
  done
fi

# Get existing table
echo "Checking existing tables on target..."
TABLES_JSON=$(curl -sS "${MOONLINK_URI_HOST}/tables" || echo '{}')
# echo "Tables response:"; echo "${TABLES_JSON}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
EXISTS=$(echo "${TABLES_JSON}" | jq -r --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '.tables | map(select(.database==$db and .table==$tbl)) | length > 0')

# Echo names of all existing tables
echo "Existing tables:"; echo "${TABLES_JSON}" | jq -r '.tables | map(.database + "." + .table) | join(", ")'

# Drop table if needed
if [[ "${RECREATE}" == "true" && "${EXISTS}" == "true" ]]; then
  echo "Dropping table ${SRC_TABLE_NAME} on target Moonlink because recreation is true..."
  DROP_PAYLOAD=$(jq -n --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '{database:$db, table:$tbl}')
  echo "DROP payload:"; echo "${DROP_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  DROP_TMP=$(mktemp)
  DROP_STATUS=$(curl -sS -o "${DROP_TMP}" -w "%{http_code}" \
    -X DELETE "${MOONLINK_URI_HOST}/tables/${SRC_TABLE_NAME}" \
    -H "content-type: application/json" -d "${DROP_PAYLOAD}")
  echo "HTTP_STATUS:${DROP_STATUS}"
  if [[ "${DROP_STATUS}" != 2* ]]; then
    echo "ERROR: Failed to drop table ${SRC_TABLE_NAME} (status=${DROP_STATUS}). Response body:" >&2
    cat "${DROP_TMP}" >&2 || true
    rm -f "${DROP_TMP}" || true
    exit 1
  fi
  cat "${DROP_TMP}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  rm -f "${DROP_TMP}" || true
else
  echo "Skipping drop (RECREATE=${RECREATE}, exists=${EXISTS})."
fi


# Add the table
if [[ -n "${SCHEMA_JSON_COMPACT:-}" ]]; then
  ML_SCHEMA_JSON="${SCHEMA_JSON_COMPACT}"
else
  ML_SCHEMA_JSON=${ML_SCHEMA_JSON:-"[{\"name\":\"string-column\",\"data_type\":\"string\",\"nullable\":false},{\"name\":\"numeric-column\",\"data_type\":\"int32\",\"nullable\":false},{\"name\":\"boolean-column\",\"data_type\":\"boolean\",\"nullable\":true}]"}
fi

if [[ -n "${S3_BUCKET}" && -n "${AWS_REGION}" && -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]]; then
  read -r -d '' ACCESSOR_S3 <<JSON || true
{
  "storage_config": {
    "s3": {
      "access_key_id": "${AWS_ACCESS_KEY_ID}",
      "secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
      "region": "${AWS_REGION}",
      "bucket": "${S3_BUCKET}"${S3_ENDPOINT:+,\"endpoint\": \"${S3_ENDPOINT}\"}
    }
  }
}
JSON
  read -r -d '' TABLE_CONFIG <<EOF || true
{
  "mooncake": {
    "append_only": true,
    "row_identity": "None"
  },
  "iceberg": ${ACCESSOR_S3},
  "wal": ${ACCESSOR_S3}
}
EOF
  
else
  read -r -d '' TABLE_CONFIG <<EOF || true
{
  "mooncake": {
    "append_only": true,
    "row_identity": "None"
  }
}
EOF
  
fi

if [[ "${EXISTS}" != "true" || "${RECREATE}" == "true" ]]; then
  echo "Creating table ${SRC_TABLE_NAME} on target Moonlink..."
  CREATE_PAYLOAD=$(jq -n \
    --arg db "${ML_DATABASE}" \
    --arg tbl "${ML_TABLE}" \
    --argjson schema "${ML_SCHEMA_JSON}" \
    --argjson table_cfg "${TABLE_CONFIG}" \
    '{database:$db, table:$tbl, schema:$schema, table_config:$table_cfg}')

  echo "CREATE payload:"; echo "${CREATE_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)

  CREATE_TMP=$(mktemp)
  CREATE_STATUS=$(curl -sS -o "${CREATE_TMP}" -w "%{http_code}" -X POST "${MOONLINK_URI_HOST}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${CREATE_PAYLOAD}")
  echo "HTTP_STATUS:${CREATE_STATUS}"
  if [[ "${CREATE_STATUS}" != 2* ]]; then
    echo "ERROR: Failed to create table ${SRC_TABLE_NAME} (status=${CREATE_STATUS}). Response body:" >&2
    cat "${CREATE_TMP}" >&2 || true
    rm -f "${CREATE_TMP}" || true
    exit 1
  fi
  cat "${CREATE_TMP}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  rm -f "${CREATE_TMP}" || true
else
  echo "Skipping create; table exists and RECREATE is false."
fi


# Setting the number of partitions for the source topic
ensure_topic_partitions() {
  local topic="$1"; local desired="$2"
  # Get current partitions; if topic missing, create it
  local desc; desc=$(docker compose exec -T kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --describe --topic '${topic}' 2>/dev/null" || true)
  local cur
  cur=$(echo "$desc" | awk -F ':' '/PartitionCount:/ {gsub(/ /, "", $0); sub(/.*PartitionCount:/, ""); sub(/,ReplicationFactor.*/, ""); print $0}' | head -n1)
  if [[ -z "$cur" ]]; then
    echo "Creating topic ${topic} with partitions=${desired}"
    docker compose exec -T kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic '${topic}' --partitions ${desired} --replication-factor 1" >/dev/null 2>&1 || true
    return
  fi
  if [[ "$desired" -gt "$cur" ]]; then
    echo "Altering topic ${topic} partitions: ${cur} -> ${desired}"
    docker compose exec -T kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --alter --topic '${topic}' --partitions ${desired}" >/dev/null 2>&1 || true
  fi
}

ensure_topic_partitions "${SOURCE_TOPIC}" "${SOURCE_PARTITIONS}"

# Registering connectors
echo "Registering source connector (${SOURCE_NAME})..."
SOURCE_PAYLOAD=$(jq -n \
  --arg name "${SOURCE_NAME}" \
  --arg tasks "${SOURCE_TASKS_MAX}" \
  --arg mps "${SOURCE_MSGS_PER_SEC}" \
  --arg size "${SOURCE_MSG_SIZE_BYTES}" \
  --arg dur "${SOURCE_MAX_DURATION}" \
  --arg run "${SOURCE_RUN_INDEFINITELY}" \
  --arg topic "${SOURCE_TOPIC}" \
  '{name: $name, config: {
    "connector.class": "example.loadgen.LoadGeneratorConnector",
    "first.required.param": "Kafka",
    "second.required.param": "Connect",
    "tasks.max": $tasks,
    "task.messages.per.second": $mps,
    "message.size.bytes": $size,
    "task.max.duration.seconds": $dur,
    "task.run.indefinitely": $run,
    "output.topic": $topic,
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "'"${URI_SCHEMA_REGISTRY:-http://schema-registry:8081}"'"
  }}')
echo "SOURCE connector payload:"; echo "${SOURCE_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
echo "${SOURCE_PAYLOAD}" | curl -sS -X POST -H "Content-Type:application/json" -d @- "${CONNECT_BASE}/connectors" -w "\nHTTP_STATUS:%{http_code}\n" | cat

echo "Registering sink connector (${SINK_NAME}, uri ${MOONLINK_URI_CONNECT})..."
SINK_PAYLOAD=$(jq -n \
  --arg name "${SINK_NAME}" \
  --arg tasks "${SINK_TASKS_MAX}" \
  --arg topics "${SOURCE_TOPIC}" \
  --arg uri "${MOONLINK_URI_CONNECT}" \
  --arg db "${ML_DATABASE}" \
  --arg tbl "${ML_TABLE}" \
  --arg schema "${ML_SCHEMA_JSON}" \
  --arg sr "http://schema-registry:8081" \
  '{name: $name, config: {
    "connector.class": "moonlink.sink.connector.MoonlinkSinkConnector",
    "tasks.max": $tasks,
    "topics": $topics,
    "moonlink.uri": $uri,
    "moonlink.table.name": $tbl,
    "moonlink.database.name": $db,
    "schema.registry.url": $sr,
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }}')
echo "SINK connector payload:"; echo "${SINK_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
echo "${SINK_PAYLOAD}" | curl -sS -X POST -H "Content-Type:application/json" -d @- "${CONNECT_BASE}/connectors" -w "\nHTTP_STATUS:%{http_code}\n" | cat

# Waiting for connectors to be RUNNING
echo "Sleeping for 2 seconds to ensure the connector is ready..."
sleep 2

echo "Waiting for both connectors to be RUNNING..."
SRC_STATUS=$(curl -sS "${CONNECT_BASE}/connectors/${SOURCE_NAME}/status" || true)
SNK_STATUS=$(curl -sS "${CONNECT_BASE}/connectors/${SINK_NAME}/status" || true)
src_conn_state=$(echo "${SRC_STATUS}" | jq -r '.connector.state // empty')
snk_conn_state=$(echo "${SNK_STATUS}" | jq -r '.connector.state // empty')
src_bad_tasks=$(echo "${SRC_STATUS}" | jq -r '[.tasks[] | select(.state != "RUNNING")] | length' 2>/dev/null || echo 1)
snk_bad_tasks=$(echo "${SNK_STATUS}" | jq -r '[.tasks[] | select(.state != "RUNNING")] | length' 2>/dev/null || echo 1)
if [[ "${src_conn_state}" != "RUNNING" || "${snk_conn_state}" != "RUNNING" || "${src_bad_tasks}" -ne 0 || "${snk_bad_tasks}" -ne 0 ]]; then
    echo "ERROR: One or both connectors are not RUNNING. Aborting." >&2
    echo "-- ${SOURCE_NAME} status --" >&2
    echo "${SRC_STATUS}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat) >&2 || true
    echo "-- ${SINK_NAME} status --" >&2
    echo "${SNK_STATUS}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat) >&2 || true
    exit 1
fi

echo "Statuses:"
curl -s "${CONNECT_BASE}/connectors/${SOURCE_NAME}/status" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
curl -s "${CONNECT_BASE}/connectors/${SINK_NAME}/status" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)

# Fail fast if either connector (or any of their tasks) is not RUNNING
src_ok=$(echo "${SRC_STATUS}" | jq -r '(.connector.state=="RUNNING") and ([(.tasks[] | .state=="RUNNING")] | all)')
snk_ok=$(echo "${SNK_STATUS}" | jq -r '(.connector.state=="RUNNING") and ([(.tasks[] | .state=="RUNNING")] | all)')
if [[ "${src_ok}" != "true" || "${snk_ok}" != "true" ]]; then
  echo "ERROR: One or both connectors are not RUNNING. Aborting." >&2
  echo "-- ${SOURCE_NAME} status --" >&2
  echo "${SRC_STATUS}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat) >&2 || true
  echo "-- ${SINK_NAME} status --" >&2
  echo "${SNK_STATUS}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat) >&2 || true
  # Attempt to print traces if present
  echo "-- Traces (if any) --" >&2
  echo "Source connector trace:" >&2
  echo "${SRC_STATUS}" | jq -r '.connector.trace // empty' 2>/dev/null >&2 || true
  echo "Sink connector trace:" >&2
  echo "${SNK_STATUS}" | jq -r '.connector.trace // empty' 2>/dev/null >&2 || true
  echo "Source task traces:" >&2
  echo "${SRC_STATUS}" | jq -r '.tasks[] | select(.state!="RUNNING") | .trace // empty' 2>/dev/null >&2 || true
  echo "Sink task traces:" >&2
  echo "${SNK_STATUS}" | jq -r '.tasks[] | select(.state!="RUNNING") | .trace // empty' 2>/dev/null >&2 || true

  # bring selected services down (keep schema-registry)
  docker compose stop kafka connect moonlink || true
  docker compose rm -f -v kafka connect moonlink || true
  exit 1
fi

echo "Start complete."

# ==========================
# Progress monitor (5s cadence) and final drop
# ==========================

__TARGET_TOTAL=0
if [[ "${SOURCE_RUN_INDEFINITELY}" != "true" && "${SOURCE_MAX_DURATION}" =~ ^[0-9]+$ && "${SOURCE_MSGS_PER_SEC}" =~ ^[0-9]+$ ]]; then
  __TARGET_TOTAL=$(( SOURCE_MSGS_PER_SEC * SOURCE_MAX_DURATION ))
fi

declare -A __pm_task_total_completed
declare -A __pm_task_last_lsn
__PM_TOTAL_COMPLETED=0
__PM_HIGHEST_LSN=0
__PM_START_TS=$(date +%s)
__PM_LAST_TS=${__PM_START_TS}
__PM_LAST_TOTAL=0

__pm_bar() {
  local current=$1; local total=$2; local width=${3:-40}
  if (( total <= 0 )); then
    printf "[ %-${width}s ] processed=%d highest_lsn=%d\n" "????????????????????????????????????????" "${current}" "${__PM_HIGHEST_LSN}"
    return
  fi
  local pct=$(( current * 100 / total ))
  (( pct>100 )) && pct=100
  local filled=$(( pct * width / 100 ))
  local bar=""
  for ((i=0;i<filled;i++)); do bar+="#"; done
  for ((i=filled;i<width;i++)); do bar+="-"; done
  printf "[ %s ] %3d%% (%d/%d) highest_lsn=%d\n" "${bar}" "${pct}" "${current}" "${total}" "${__PM_HIGHEST_LSN}"
}

while true; do
  __pm_lines=$(docker compose logs -t --since=7s connect 2>/dev/null | grep 'Sink task batch:' || true)
  if [[ -n "${__pm_lines}" ]]; then
    while IFS= read -r __pm_line; do
      __pm_task_id=$(echo "${__pm_line}" | sed -n 's/.*task_id=\([^ ]*\).*/\1/p')
      __pm_total_completed=$(echo "${__pm_line}" | sed -n 's/.* total_completed=\([0-9]\+\).*/\1/p')
      __pm_lsn=$(echo "${__pm_line}" | sed -n 's/.* last_lsn=\([0-9]\+\).*/\1/p')
      [[ -z "${__pm_task_id}" || -z "${__pm_total_completed}" ]] && continue
      __pm_task_total_completed["${__pm_task_id}"]=${__pm_total_completed}
      if [[ -n "${__pm_lsn}" ]]; then
        __pm_task_last_lsn["${__pm_task_id}"]=${__pm_lsn}
        if (( __pm_lsn > __PM_HIGHEST_LSN )); then __PM_HIGHEST_LSN=${__pm_lsn}; fi
      fi
    done <<< "${__pm_lines}"
  fi

  # Recompute total completed across tasks
  __PM_TOTAL_COMPLETED=0
  for __pm_tid in "${!__pm_task_total_completed[@]}"; do
    __PM_TOTAL_COMPLETED=$(( __PM_TOTAL_COMPLETED + __pm_task_total_completed["${__pm_tid}"] ))
  done

  if (( __TARGET_TOTAL > 0 )); then
    __pm_bar "${__PM_TOTAL_COMPLETED}" "${__TARGET_TOTAL}" 40
  else
    __pm_bar "${__PM_TOTAL_COMPLETED}" 0 40
  fi

  # Probe-level RPS (based on delta rows/delta time)
  __PM_NOW_TS=$(date +%s)
  __PM_DT=$(( __PM_NOW_TS - __PM_LAST_TS ))
  if (( __PM_DT < 1 )); then __PM_DT=1; fi
  __PM_DROWS=$(( __PM_TOTAL_COMPLETED - __PM_LAST_TOTAL ))
  __PM_PROBE_RPS=$(awk -v d="${__PM_DROWS}" -v t="${__PM_DT}" 'BEGIN{ if(t>0) printf "%.2f", d/t; else print "0.00" }')
  echo "probe_rps=${__PM_PROBE_RPS} delta_rows=${__PM_DROWS} window_s=${__PM_DT}"
  __PM_LAST_TS=${__PM_NOW_TS}
  __PM_LAST_TOTAL=${__PM_TOTAL_COMPLETED}

  if (( __TARGET_TOTAL > 0 && __PM_TOTAL_COMPLETED >= __TARGET_TOTAL )); then
    echo "Target reached. Highest LSN: ${__PM_HIGHEST_LSN}"
    break
  fi

  sleep 5
done

# Overall runtime and RPS
__PM_END_TS=$(date +%s)
__PM_TOTAL_S=$(( __PM_END_TS - __PM_START_TS ))
if (( __PM_TOTAL_S < 1 )); then __PM_TOTAL_S=1; fi
__PM_OVERALL_RPS=$(awk -v n="${__PM_TOTAL_COMPLETED}" -v s="${__PM_TOTAL_S}" 'BEGIN{ if(s>0) printf "%.2f", n/s; else print "0.00" }')
echo "overall_duration_s=${__PM_TOTAL_S} overall_rows=${__PM_TOTAL_COMPLETED} overall_rps=${__PM_OVERALL_RPS}"


# Create a snapshot at the highest observed LSN and then verify row count via DataFusion (local only)
HIGHEST_LSN="${__PM_HIGHEST_LSN}"
SNAP_PAYLOAD=$(jq -n --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" --argjson lsn "${HIGHEST_LSN}" '{database:$db, table:$tbl, lsn:$lsn}')
SNAP_TMP=$(mktemp)
echo "Creating snapshot at LSN ${HIGHEST_LSN} for ${ML_DATABASE}.${ML_TABLE}..."
SNAP_HTTP_STATUS=$(curl -sS "${CURL_TIMEOUT_OPTS[@]}" -o "${SNAP_TMP}" -w "%{http_code}" \
  -X POST "${MOONLINK_URI_HOST}/tables/${SRC_TABLE_NAME}/snapshot" \
  -H "content-type: application/json" -d "${SNAP_PAYLOAD}")
SNAP_CURL_EC=$?
if [[ ${SNAP_CURL_EC} -ne 0 || "${SNAP_HTTP_STATUS}" != 2* ]]; then
  if [[ ${SNAP_CURL_EC} -eq 28 ]]; then
    echo "Timeout creating snapshot (curl ec=28). Retrying once..."
    SNAP_HTTP_STATUS=$(curl -sS "${CURL_TIMEOUT_OPTS[@]}" -o "${SNAP_TMP}" -w "%{http_code}" \
      -X POST "${MOONLINK_URI_HOST}/tables/${SRC_TABLE_NAME}/snapshot" \
      -H "content-type: application/json" -d "${SNAP_PAYLOAD}")
    SNAP_CURL_EC=$?
    if [[ ${SNAP_CURL_EC} -ne 0 || "${SNAP_HTTP_STATUS}" != 2* ]]; then
      echo "ERROR: Snapshot creation failed after retry (status=${SNAP_HTTP_STATUS}, curl_ec=${SNAP_CURL_EC}). Response body:" >&2
      cat "${SNAP_TMP}" >&2 || true
      rm -f "${SNAP_TMP}" || true
      # bring selected services down (keep schema-registry)
      docker compose stop kafka connect moonlink || true
      docker compose rm -f -v kafka connect moonlink || true
      exit 1
    fi
  else
    echo "ERROR: Snapshot creation failed (status=${SNAP_HTTP_STATUS}, curl_ec=${SNAP_CURL_EC}). Response body:" >&2
    cat "${SNAP_TMP}" >&2 || true
    rm -f "${SNAP_TMP}" || true
    # bring selected services down (keep schema-registry)
    docker compose stop kafka connect moonlink || true
    docker compose rm -f -v kafka connect moonlink || true
    exit 1
  fi
fi
cat "${SNAP_TMP}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
rm -f "${SNAP_TMP}" || true

if [[ "${IS_LOCAL_EXECUTION}" == "true" ]]; then
  echo "Verifying row count via DataFusion CLI..."
  DF_BIN_PATH="./moonlink/target/debug/moonlink_datafusion"
  if [[ "${MOONLINK_RELEASE}" == "true" ]]; then DF_BIN_PATH="./moonlink/target/release/moonlink_datafusion"; fi
  SOCKET_PATH="./${MOONLINK_DATA_DIR%/}/moonlink.sock"
  if [[ ! -S "${SOCKET_PATH}" ]]; then
    echo "ERROR: Moonlink socket not found at ${SOCKET_PATH}" >&2
    exit 1
  fi
  SQL="SELECT COUNT(*) FROM mooncake.'${ML_DATABASE}'.'${ML_TABLE}';"
  DF_OUT=$(printf "%s\n" "${SQL}" | "${DF_BIN_PATH}" "${SOCKET_PATH}" 2>&1 | tee .df_count_output.log)
  ROW_COUNT=$(echo "${DF_OUT}" | sed -n 's/^|[[:space:]]*\([0-9][0-9]*\)[[:space:]]*|$/\1/p' | head -n1)
  if [[ -z "${ROW_COUNT}" ]]; then
    ROW_COUNT=$(echo "${DF_OUT}" | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {print $1}' | tail -n1)
  fi
  if [[ -z "${ROW_COUNT}" ]]; then
    echo "ERROR: Failed to parse COUNT(*) from DataFusion output. Full output:" >&2
    cat .df_count_output.log >&2 || true
    exit 1
  fi
  echo "DataFusion COUNT(*)=${ROW_COUNT}; sink reported completed=${__PM_TOTAL_COMPLETED}"
  if [[ "${ROW_COUNT}" -ne "${__PM_TOTAL_COMPLETED}" ]]; then
    echo "ERROR: Row count mismatch between DataFusion (${ROW_COUNT}) and sink completed (${__PM_TOTAL_COMPLETED})." >&2
    exit 1
  fi
fi

# DuckDB Iceberg verification (if S3 is configured and duckdb is available)
if [[ -n "${S3_BUCKET}" && -n "${AWS_REGION}" && -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]]; then
  echo "Verifying row count via DuckDB iceberg_scan..."
  DUCKDB_BIN="${HOME}/.duckdb/cli/latest/duckdb"
  if [[ ! -x "${DUCKDB_BIN}" ]] && command -v duckdb >/dev/null 2>&1; then
    DUCKDB_BIN="$(command -v duckdb)"
  fi
  if [[ ! -x "${DUCKDB_BIN}" ]]; then
    echo "ERROR: DuckDB not found at ${HOME}/.duckdb/cli/latest/duckdb and not in PATH. Pre-install DuckDB in the devcontainer (e.g., curl https://install.duckdb.org | sh)." >&2
    exit 1
  fi

    TBL_JSON=$(curl -sS "${MOONLINK_URI_HOST}/tables")
    ICEBERG_URI=$(echo "${TBL_JSON}" | jq -r --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '.tables | map(select(.database==$db and .table==$tbl)) | .[0].iceberg_warehouse_location // empty')
    if [[ -z "${ICEBERG_URI}" ]]; then
      echo "ERROR: Could not resolve iceberg_warehouse_location for ${ML_DATABASE}.${ML_TABLE}." >&2
      exit 1
    else
      echo "ICEBERG_URI=${ICEBERG_URI}"
    fi

    # Build full Iceberg table URI if the location is only a warehouse root
    if [[ "${ICEBERG_URI}" == *"/${ML_DATABASE}/${ML_TABLE}" ]]; then
      ICEBERG_TABLE_URI="${ICEBERG_URI}"
    else
      ICEBERG_TABLE_URI="${ICEBERG_URI%/}/${ML_DATABASE}/${ML_TABLE}"
    fi
    echo "ICEBERG_TABLE_URI=${ICEBERG_TABLE_URI}"

    DUCKDB_SQL=$(mktemp)
    {
      echo "INSTALL iceberg;"
      echo "LOAD iceberg;"
      echo "SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';"
      echo "SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';"
      echo "SET s3_region='${AWS_REGION}';"
      if [[ -n "${S3_ENDPOINT}" ]]; then
        echo "SET s3_endpoint='${S3_ENDPOINT}';"
        echo "SET s3_url_style='path';"
      fi
      echo "COPY (SELECT COUNT(*) FROM iceberg_scan('${ICEBERG_TABLE_URI}')) TO STDOUT (FORMAT CSV, HEADER false);"
      echo ".quit"
    } > "${DUCKDB_SQL}"

    "${DUCKDB_BIN}" :memory: < "${DUCKDB_SQL}" > .duckdb_count_output.txt 2>&1 || true
    rm -f "${DUCKDB_SQL}" || true
    DUCKDB_COUNT=$(awk '/^[0-9]+$/{val=$0} END{if(val!="") print val; else print "NA"}' .duckdb_count_output.txt)
    if ! [[ "${DUCKDB_COUNT}" =~ ^[0-9]+$ ]]; then
      echo "ERROR: Failed to parse COUNT(*) from DuckDB output. Full output:" >&2
      cat .duckdb_count_output.txt >&2 || true
      exit 1
    fi
    echo "DuckDB COUNT(*)=${DUCKDB_COUNT}; sink reported completed=${__PM_TOTAL_COMPLETED}"
    if [[ "${DUCKDB_COUNT}" -ne "${__PM_TOTAL_COMPLETED}" ]]; then
      echo "ERROR: Row count mismatch between DuckDB (${DUCKDB_COUNT}) and sink completed (${__PM_TOTAL_COMPLETED})." >&2
      exit 1
    fi
fi

if [[ "${__PM_SHOULD_DROP}" == "true" ]]; then
  __PM_DROP_PAYLOAD=$(jq -n --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '{database:$db, table:$tbl}')
  __PM_DROP_STATUS=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${MOONLINK_URI_HOST}/tables/${ML_DATABASE}.${ML_TABLE}" -H 'content-type: application/json' -d "${__PM_DROP_PAYLOAD}" || true)
  echo "Final drop table status: ${__PM_DROP_STATUS}"
else
  echo "Skipping final drop (table.drop_table_on_completion=false)"
fi

echo "Test cycle completed successfully."