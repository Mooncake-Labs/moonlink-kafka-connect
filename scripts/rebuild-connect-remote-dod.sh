#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CONFIG_PATH="${1:-${CONFIG:-}}"

if [[ -n "${CONFIG_PATH}" ]]; then
  if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required to read config ${CONFIG_PATH}" >&2
    exit 1
  fi
  REST_BASE=$(jq -r '.remote.moonlink_uri' "${CONFIG_PATH}")
  LOCAL_MOONLINK=$(jq -r '.remote.local_execution // false' "${CONFIG_PATH}")
  ML_DATABASE=$(jq -r '.table.database' "${CONFIG_PATH}")
  ML_TABLE=$(jq -r '.table.name' "${CONFIG_PATH}")
  SOURCE_NAME=$(jq -r '.source.connector_name // "example-source-connector"' "${CONFIG_PATH}")
  SINK_NAME=$(jq -r '.sink.connector_name // "moonlink-sink-connector"' "${CONFIG_PATH}")
  SOURCE_TASKS_MAX=$(jq -r '.source.tasks_max' "${CONFIG_PATH}")
  SOURCE_MSGS_PER_SEC=$(jq -r '.source.messages_per_second' "${CONFIG_PATH}")
  SOURCE_MSG_SIZE_BYTES=$(jq -r '.source.message_size_bytes' "${CONFIG_PATH}")
  SOURCE_MAX_DURATION=$(jq -r '.source.max_duration_seconds' "${CONFIG_PATH}")
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
else
  REST_BASE="${REMOTE_BASE:-${REST_BASE:-}}"
  if [[ -z "${REST_BASE}" ]]; then
    echo "ERROR: Set REMOTE_BASE (e.g. http://ec2-xx-xx-xx-xx.compute-1.amazonaws.com:3030) or provide a config path as first arg" >&2
    exit 1
  fi
fi

# Default/local execution toggle (env override allowed when no config)
LOCAL_MOONLINK="${LOCAL_MOONLINK:-false}"

# Determine host-facing Moonlink REST and connector-facing Moonlink URI
if [[ "${LOCAL_MOONLINK}" == "true" ]]; then
  REST_BASE_HOST="http://localhost:3030"
  SINK_MOONLINK_URI="http://moonlink:3030"
  echo "Moonlink execution mode: local"
else
  REST_BASE_HOST="${REST_BASE}"
  SINK_MOONLINK_URI="${REST_BASE}"
  echo "Moonlink execution mode: remote (${REST_BASE})"
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
SOURCE_MSGS_PER_SEC="${SOURCE_MSGS_PER_SEC:-60}"
SOURCE_MSG_SIZE_BYTES="${SOURCE_MSG_SIZE_BYTES:-200}"
SOURCE_MAX_DURATION="${SOURCE_MAX_DURATION:-10}"
SOURCE_TOPIC="${SOURCE_TOPIC:-source-1}"
SINK_TASKS_MAX="${SINK_TASKS_MAX:-1}"
RECREATE="${RECREATE:-true}"
PROFILING_CSV_PATH="${PROFILING_CSV_PATH:-lsn_snapshot_latency.csv}"
PROFILING_LOG_PATH="${PROFILING_LOG_PATH:-profiling_prints.log}"

echo "Building connector jar..."
mvn -q -DskipTests package

echo "Stopping any existing Compose stack..."
docker compose down -v || true

if [[ "${LOCAL_MOONLINK}" == "true" ]]; then
  echo "Starting local Kafka+Connect+Moonlink..."
  HOST_ARCH=$(uname -m || echo "")
  # Use Dockerfile.amd64 on x86_64 hosts to avoid cross-compiling aarch64 OpenSSL
  if [[ "${HOST_ARCH}" == "x86_64" || "${HOST_ARCH}" == "amd64" ]]; then
    echo "Detected host arch ${HOST_ARCH}; using Dockerfile.amd64 for moonlink build"
    TMP_OVERRIDE=$(mktemp)
    cat >"${TMP_OVERRIDE}" <<'YAML'
services:
  moonlink:
    build:
      dockerfile: moonlink/Dockerfile.amd64
YAML
    docker compose -f docker-compose.yml -f "${TMP_OVERRIDE}" up -d --force-recreate kafka connect moonlink || {
      echo "Compose up failed with amd64 override; falling back to default compose." >&2
      docker compose up -d --force-recreate kafka connect moonlink
    }
    rm -f "${TMP_OVERRIDE}" || true
  else
    docker compose up -d --force-recreate kafka connect moonlink
  fi
else
  echo "Starting local Kafka+Connect..."
  docker compose up -d --force-recreate kafka connect
fi

# Resolve Connect REST for Docker-outside-of-Docker (access host-published port from container)
echo "Resolving Connect REST base for Docker-outside-of-Docker..."
CONNECT_HOST="${CONNECT_HOST:-}"
if [[ -z "${CONNECT_HOST}" ]]; then
  if getent hosts host.docker.internal >/dev/null 2>&1; then
    CONNECT_HOST="host.docker.internal"
  else
    CONNECT_HOST=$(ip route | awk '/default/ {print $3}' | head -n1 || true)
  fi
fi
if [[ -z "${CONNECT_HOST}" ]]; then
  echo "Warning: could not determine host gateway; defaulting to 127.0.0.1"
  CONNECT_HOST="127.0.0.1"
fi
CONNECT_BASE="http://${CONNECT_HOST}:8083"
echo "Using Connect REST at ${CONNECT_BASE}"

# In local mode, host-facing Moonlink REST should use the same host gateway as Connect
if [[ "${LOCAL_MOONLINK}" == "true" ]]; then
  REST_BASE_HOST="http://${CONNECT_HOST}:3030"
fi

# Ensure custom connector JARs are available inside the Connect container in DoD
echo "Ensuring custom plugins are present inside Connect container..."
CONNECT_CID=$(docker compose ps -q connect || true)
if [[ -n "${CONNECT_CID}" ]]; then
  docker compose exec -T connect bash -lc "mkdir -p /usr/share/java/my-first-kafka-connector" || true
  # Copy locally built JARs into the running container (bind mounts may not work in DoD)
  for J in target/*.jar; do
    if [[ -f "$J" ]]; then
      echo "Copying $J to container...";
      docker cp "$J" "${CONNECT_CID}:/usr/share/java/my-first-kafka-connector/" || true
    fi
  done
  echo "Restarting Connect to pick up newly copied plugins..."
  docker compose restart connect >/dev/null 2>&1 || true
  echo "Verifying custom plugins are loaded..."
  for i in {1..30}; do
    PLUGINS_JSON=$(curl -sS "${CONNECT_BASE}/connector-plugins" || true)
    if echo "${PLUGINS_JSON}" | grep -q 'example.source.MyFirstKafkaConnector' && echo "${PLUGINS_JSON}" | grep -q 'moonlink.sink.connector.MoonlinkSinkConnector'; then
      echo "Custom plugins detected."
      break
    fi
    sleep 2
  done
fi

if [[ "${LOCAL_MOONLINK}" == "true" ]]; then
  echo "Waiting for Moonlink (container) to be healthy..."
  for i in {1..60}; do
    if docker compose exec -T connect bash -lc "curl -fsS http://moonlink:3030/health >/dev/null 2>&1"; then
      echo "Moonlink responded from connect container."
      break
    fi
    sleep 1
  done
fi

echo "Checking Moonlink health at ${REST_BASE_HOST}..."
for i in {1..60}; do
  if curl -fsS "${REST_BASE_HOST}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "${REST_BASE_HOST}/health" | (command -v jq >/dev/null 2>&1 && jq || cat)

echo "Checking existing tables on target..."
TABLES_JSON=$(curl -sS "${REST_BASE_HOST}/tables" || echo '{}')
# echo "Tables response:"; echo "${TABLES_JSON}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
EXISTS=$(echo "${TABLES_JSON}" | jq -r --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '.tables | map(select(.database==$db and .table==$tbl)) | length > 0')

if [[ "${RECREATE}" == "true" && "${EXISTS}" == "true" ]]; then
  echo "Dropping table ${SRC_TABLE_NAME} on target Moonlink (RECREATE=true)..."
  read -r -d '' DROP_PAYLOAD <<EOF
{ "database": "${ML_DATABASE}", "table": "${ML_TABLE}" }
EOF
  
  echo "DROP payload:"; echo "${DROP_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  curl -sS -X DELETE "${REST_BASE_HOST}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${DROP_PAYLOAD}" \
    -w "\nHTTP_STATUS:%{http_code}\n" | cat || echo "(non-2xx on drop)"
else
  echo "Skipping drop (RECREATE=${RECREATE}, exists=${EXISTS})."
fi

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
read -r -d '' CREATE_PAYLOAD <<EOF || true
{
  "database": "${ML_DATABASE}",
  "table": "${ML_TABLE}",
  "schema": ${ML_SCHEMA_JSON},
  "table_config": ${TABLE_CONFIG}
}
EOF
  
# echo "CREATE payload:"; echo "${CREATE_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  curl -sS -X POST "${REST_BASE_HOST}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${CREATE_PAYLOAD}" \
    -w "\nHTTP_STATUS:%{http_code}\n" | cat
else
  echo "Skipping create; table exists and RECREATE is false."
fi

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

# Ensure source topic partitions >= sink tasks
if [[ -n "${SOURCE_TOPIC}" && -n "${SINK_TASKS_MAX}" ]]; then
  ensure_topic_partitions "${SOURCE_TOPIC}" "${SINK_TASKS_MAX}"
fi

echo "Registering source connector (${SOURCE_NAME})..."
SOURCE_PAYLOAD=$(jq -n \
  --arg name "${SOURCE_NAME}" \
  --arg tasks "${SOURCE_TASKS_MAX}" \
  --arg mps "${SOURCE_MSGS_PER_SEC}" \
  --arg size "${SOURCE_MSG_SIZE_BYTES}" \
  --arg dur "${SOURCE_MAX_DURATION}" \
  --arg topic "${SOURCE_TOPIC}" \
  '{name: $name, config: {
    "connector.class": "example.source.MyFirstKafkaConnector",
    "first.required.param": "Kafka",
    "second.required.param": "Connect",
    "tasks.max": $tasks,
    "task.messages.per.second": $mps,
    "message.size.bytes": $size,
    "task.max.duration.seconds": $dur,
    "output.topic": $topic
  }}')
echo "SOURCE connector payload:"; echo "${SOURCE_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
echo "${SOURCE_PAYLOAD}" | curl -sS -X POST -H "Content-Type:application/json" -d @- "${CONNECT_BASE}/connectors" -w "\nHTTP_STATUS:%{http_code}\n" | cat

echo "Registering sink connector (${SINK_NAME}, uri ${SINK_MOONLINK_URI})..."
SINK_PAYLOAD=$(jq -n \
  --arg name "${SINK_NAME}" \
  --arg tasks "${SINK_TASKS_MAX}" \
  --arg topics "${SOURCE_TOPIC}" \
  --arg uri "${SINK_MOONLINK_URI}" \
  --arg db "${ML_DATABASE}" \
  --arg tbl "${ML_TABLE}" \
  --arg schema "${ML_SCHEMA_JSON}" \
  '{name: $name, config: {
    "connector.class": "moonlink.sink.connector.MoonlinkSinkConnector",
    "tasks.max": $tasks,
    "topics": $topics,
    "moonlink.uri": $uri,
    "moonlink.table.name": $tbl,
    "moonlink.database.name": $db,
    "moonlink.json.schema": $schema
  }}')
echo "SINK connector payload:"; echo "${SINK_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
echo "${SINK_PAYLOAD}" | curl -sS -X POST -H "Content-Type:application/json" -d @- "${CONNECT_BASE}/connectors" -w "\nHTTP_STATUS:%{http_code}\n" | cat

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

  # bring the docker down
  docker down -v kafka connect
  exit 1


fi

echo "Start complete."


# ==========================
# LSN-based profiling with synchronous snapshot latency
# ==========================

if [[ "${SOURCE_MAX_DURATION}" =~ ^[0-9]+$ && "${SOURCE_MAX_DURATION}" -gt 0 ]]; then
  TARGET_MESSAGES=$(( SOURCE_MSGS_PER_SEC * SOURCE_MAX_DURATION ))
  echo "Profiling (LSN-based) enabled: target_messages=${TARGET_MESSAGES}" 

  # Helpers
  plog() {
    # Print to console and append to profiler log file
    echo "$1"
    if [[ -n "${PROFILING_LOG_PATH}" ]]; then echo "$1" >> "${PROFILING_LOG_PATH}"; fi
  }
  get_latest_lsn_and_publish_ms() {
    # Find the latest line carrying an LSN and parse both LSN and the docker ISO timestamp (publish time)
    local line
    line=$(docker compose logs -t --since=90s connect 2>/dev/null | \
      grep -E 'Inserted row, lsn=|Sink poll completed:' | tail -n 1)
    local lsn token
    token=$(echo "${line}" | grep -Eo 'lsn=[0-9]+|last_lsn=[0-9]+' | tail -n 1)
    lsn=$(echo "${token}" | sed -E 's/^[a-z_]+=//')
    # Parse the docker-compose ISO8601 timestamp prefix (e.g., 2025-09-07T00:39:44.868793584Z)
    local iso
    iso=$(echo "${line}" | grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z' | head -n 1)
    local publish_ms
    if [[ -n "${iso}" ]]; then
      publish_ms=$(date -d "${iso}" +%s%3N 2>/dev/null || echo "")
    else
      publish_ms=""
    fi
    echo "${lsn},${publish_ms}"
  }

  saw_source_finished=false
  final_lsn=0
  start_ms=$(date +%s%3N)
  last_check_ms=${start_ms}
  SAMPLE_INTERVAL_SEC=5

  echo "snapshot_end_ms,lsn,publish_ms,snapshot_total_latency_ms,snapshot_request_delay_ms,snapshot_http_ms" > "${PROFILING_CSV_PATH}"

  target_reached=false
  last_known_lsn=0
  while true; do
    loop_start_ms=$(date +%s%3N)

    # Check if source reported finished; cache latest observed lsn
    latest_source_line=$(docker compose logs -t --since=90s connect 2>/dev/null | grep -E 'Source task finished:' | tail -n 1 || true)
    if [[ -n "${latest_source_line}" && "${saw_source_finished}" = false ]]; then
      cur_lsn=$(get_latest_lsn_and_publish_ms | cut -d',' -f1)
      cur_lsn=${cur_lsn:-0}
      plog "Detected source finish: ${latest_source_line} (latest_lsn=${cur_lsn})"
      saw_source_finished=true
      final_lsn=${cur_lsn}
    fi

    # Per-5s live report: aggregate latest per-task batch logs
    batch_lines=$(docker compose logs -t --since=90s connect 2>/dev/null | grep -E 'Sink task batch:' || true)
    task_ids=$(echo "${batch_lines}" | sed -n 's/.*task_id=\([^ ]*\).*/\1/p' | sort -u)
    total_completed_sum=0
    max_last_lsn=0
    report_lines=""
    total_processed_in_latest=0
    total_tasks_count=0
    for tid in ${task_ids}; do
      line=$(echo "${batch_lines}" | grep -E "task_id=${tid} " | tail -n 1)
      processed_batch=$(echo "${line}" | sed -n 's/.* processed=\([0-9]\+\).*/\1/p')
      tc=$(echo "${line}" | sed -n 's/.* total_completed=\([0-9]\+\).*/\1/p')
      rps=$(echo "${line}" | sed -n 's/.* rps=\([0-9.\-]\+\).*/\1/p')
      elapsed_ms=$(echo "${line}" | sed -n 's/.* elapsed_ms=\([0-9]\+\).*/\1/p')
      http_ms_batch=$(echo "${line}" | sed -n 's/.* http_ms_batch=\([0-9.]\+\).*/\1/p')
      ser_ms_batch=$(echo "${line}" | sed -n 's/.* serialize_ms_batch=\([0-9.]\+\).*/\1/p')
      last_lsn_task=$(echo "${line}" | sed -n 's/.* last_lsn=\([0-9]\+\).*/\1/p')
      total_completed_sum=$(( total_completed_sum + ${tc:-0} ))
      total_processed_in_latest=$(( total_processed_in_latest + ${processed_batch:-0} ))
      total_tasks_count=$(( total_tasks_count + 1 ))
      if [[ -n "${last_lsn_task}" && ${last_lsn_task} -gt ${max_last_lsn} ]]; then max_last_lsn=${last_lsn_task}; fi
      report_lines+=$(printf "  task=%s processed=%s total_completed=%s rps=%s elapsed_ms=%s http_ms_batch=%s serialize_ms_batch=%s last_lsn=%s\n" "${tid}" "${processed_batch:-0}" "${tc:-0}" "${rps:-0}" "${elapsed_ms:-0}" "${http_ms_batch:-0}" "${ser_ms_batch:-0}" "${last_lsn_task:-0}")
    done
    percent=$(awk -v cur="${total_completed_sum}" -v tot="${TARGET_MESSAGES}" 'BEGIN{ if(tot>0) printf "%.2f", (cur*100.0)/tot; else print 0 }')
    if [[ ${total_tasks_count} -gt 0 ]]; then avg_batch_size=$(awk -v p=${total_processed_in_latest} -v t=${total_tasks_count} 'BEGIN{ printf "%.2f", (t>0?p/t:0) }'); else avg_batch_size=0; fi
    plog ""
    plog "Live report: total_completed=${total_completed_sum}/${TARGET_MESSAGES} (${percent}%) max_last_lsn=${max_last_lsn} avg_batch_size=${avg_batch_size}"
    printf "%s" "${report_lines}"

    if [[ ${total_completed_sum} -ge ${TARGET_MESSAGES} ]]; then
      final_lsn=${max_last_lsn}
      plog "All expected messages ingested. final_lsn=${final_lsn}."
      break
    fi

    # Sleep to align to a 5-second cadence using wall-clock
    next_due_ms=$(( last_check_ms + (SAMPLE_INTERVAL_SEC * 1000) ))
    now_ms=$(date +%s%3N)
    sleep_ms=$(( next_due_ms - now_ms ))
    if [[ ${sleep_ms} -gt 0 ]]; then
      sleep $(awk -v ms=${sleep_ms} 'BEGIN { printf "%.3f", ms/1000 }')
    fi
    last_check_ms=$(date +%s%3N)
  done

  end_ms=$(date +%s%3N)
  total_s=$(awk -v start=${start_ms} -v end=${end_ms} 'BEGIN { printf "%.3f", (end-start)/1000 }')
  # Derive sink-side averages from logs over the run window
  run_window_s=$(( ( (end_ms - start_ms) / 1000 ) + 5 ))
  if [[ ${run_window_s} -lt 30 ]]; then run_window_s=30; fi
  sink_lines=$(docker compose logs -t --since=${run_window_s}s connect 2>/dev/null | grep -E 'Sink poll completed:' || true)
  sink_avg_http_ms=$(echo "${sink_lines}" | grep -Eo 'avg_http_ms=[0-9.]+' | sed 's/.*=//' | awk '{sum+=$1; n++} END{ if(n>0) printf "%.2f", sum/n; else print 0}')
  sink_avg_ser_ms=$(echo "${sink_lines}" | grep -Eo 'avg_serialize_ms=[0-9.]+' | sed 's/.*=//' | awk '{sum+=$1; n++} END{ if(n>0) printf "%.2f", sum/n; else print 0}')
  # Aggregate client-side JSON vs SEND timing from logs
  http_timing_lines=$(docker compose logs -t --since=${run_window_s}s connect 2>/dev/null | grep -E 'HTTP timings: op=insertpb' || true)
  sink_avg_thr_rps=$(echo "${sink_lines}" | grep -Eo 'throughput_rps=[0-9.]+' | sed 's/.*=//' | awk '{sum+=$1; n++} END{ if(n>0) printf "%.2f", sum/n; else print 0}')
  sink_avg_avg_thr_rps=$(echo "${sink_lines}" | grep -Eo 'avg_throughput_rps=[0-9.]+' | sed 's/.*=//' | awk '{sum+=$1; n++} END{ if(n>0) printf "%.2f", sum/n; else print 0}')
  # Final cumulative throughput (rows/s) based on total wall time
  final_cum_rps=$(awk -v n="${TARGET_MESSAGES}" -v s="${total_s}" 'BEGIN{ if(s>0) printf "%.2f", n/s; else print 0 }')
  final_cum_mb_s=$(awk -v r="${final_cum_rps}" -v b="${SOURCE_MSG_SIZE_BYTES}" 'BEGIN{ printf "%.3f", (r*b)/1000000 }')
  sink_total_completed=$(echo "${sink_lines}" | tail -n 1 | sed -n 's/.*total_completed=\([0-9]\+\).*/\1/p')
  sink_avg_mb_s=$(awk -v rps="${sink_avg_thr_rps}" -v b="${SOURCE_MSG_SIZE_BYTES}" 'BEGIN { printf "%.3f", (rps*b)/1000000 }')
  backlog_msgs=""
  if [[ -n "${sink_total_completed}" ]]; then
    backlog_msgs=$(( TARGET_MESSAGES - sink_total_completed ))
    if [[ ${backlog_msgs} -lt 0 ]]; then backlog_msgs=0; fi
  fi
  plog ""
  plog "================ Profiling Context ================="
  msg_size_mb=$(awk -v b="${SOURCE_MSG_SIZE_BYTES}" 'BEGIN{ printf "%.3f", b/1000000 }')
  expected_total_msgs=${TARGET_MESSAGES}
  total_data_mb=$(awk -v n="${TARGET_MESSAGES}" -v b="${SOURCE_MSG_SIZE_BYTES}" 'BEGIN{ printf "%.3f", (n*b)/1000000 }')
  total_data_gb=$(awk -v n="${TARGET_MESSAGES}" -v b="${SOURCE_MSG_SIZE_BYTES}" 'BEGIN{ printf "%.3f", (n*b)/1000000000 }')
  expected_mbps=$(awk -v m="${SOURCE_MSGS_PER_SEC}" -v b="${SOURCE_MSG_SIZE_BYTES}" 'BEGIN{ printf "%.3f", (m*b)/1000000 }')
  start_human=$(date -d @$((${start_ms}/1000)) +"%Y-%m-%d %H:%M:%S")
  end_human=$(date -d @$((${end_ms}/1000)) +"%Y-%m-%d %H:%M:%S")
  plog "Moonlink URI: ${SINK_MOONLINK_URI}"
  plog "Table: ${SRC_TABLE_NAME}  Topic: ${SOURCE_TOPIC}"
  plog "Source tasks: ${SOURCE_TASKS_MAX}  Sink tasks: ${SINK_TASKS_MAX}"
  plog "Configured MPS: ${SOURCE_MSGS_PER_SEC}  Message size: ${SOURCE_MSG_SIZE_BYTES} bytes (~${msg_size_mb} MB)  Max duration: ${SOURCE_MAX_DURATION}s"
  plog "Expected total messages: ${expected_total_msgs}  Expected data: ~${total_data_mb} MB (~${total_data_gb} GB)"
  plog "Expected data rate: ~${expected_mbps} MB/s"
  plog "Sample interval: ${SAMPLE_INTERVAL_SEC}s  Start: ${start_human}  End: ${end_human}"
  plog "=================================================="
  plog "================ LSN Profiling Summary ================"
  plog "Final observed LSN: ${final_lsn}"
  plog "Total wall time: ${total_s}s"
  # End-of-ingestion per-task completion times
  batch_lines_all=$(docker compose logs -t --since=${run_window_s}s connect 2>/dev/null | grep -E 'Sink task batch:' || true)
  task_ids=$(echo "${batch_lines_all}" | sed -n 's/.*task_id=\([^ ]*\).*/\1/p' | sort -u)
  total_batches_processed=0
  total_batches_count=0
  for tid in ${task_ids}; do
    last_line=$(echo "${batch_lines_all}" | grep -E "task_id=${tid} " | tail -n 1)
    iso=$(echo "${last_line}" | grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z' | head -n 1)
    done_ms=""
    if [[ -n "${iso}" ]]; then done_ms=$(date -d "${iso}" +%s%3N 2>/dev/null || echo ""); fi
    if [[ -n "${done_ms}" ]]; then dur_ms=$(( done_ms - start_ms )); else dur_ms=""; fi
    tc=$(echo "${last_line}" | sed -n 's/.* total_completed=\([0-9]\+\).*/\1/p')
    plog "Task ${tid} finished: total_completed=${tc} duration_ms=${dur_ms}"
    # accumulate batch sizes
    sizes_for_task=$(echo "${batch_lines_all}" | grep -E "task_id=${tid} " | sed -n 's/.* processed=\([0-9]\+\).*/\1/p')
    for s in ${sizes_for_task}; do total_batches_processed=$(( total_batches_processed + s )); total_batches_count=$(( total_batches_count + 1 )); done
  done
  if [[ ${total_batches_count} -gt 0 ]]; then final_avg_batch=$(awk -v p=${total_batches_processed} -v n=${total_batches_count} 'BEGIN{ printf "%.2f", (n>0?p/n:0) }'); else final_avg_batch=0; fi
  plog "Average batch size (overall): ${final_avg_batch}"
  if command -v awk >/dev/null 2>&1; then
    avg_latency=$(awk -F',' 'NR>1 {sum+=$4; n++} END { if(n>0) printf("%.2f", sum/n); else print 0 }' "${PROFILING_CSV_PATH}")
    avg_req_lag=$(awk -F',' 'NR>1 {sum+=$5; n++} END { if(n>0) printf("%.2f", sum/n); else print 0 }' "${PROFILING_CSV_PATH}")
    plog "Average snapshot latency: ${avg_latency} ms"
    plog "Average request lag: ${avg_req_lag} ms"
  fi
  plog "Sink avg HTTP roundtrip: ${sink_avg_http_ms} ms  avg serialize: ${sink_avg_ser_ms} ms"
  plog "Sink instantaneous avg throughput: ${sink_avg_thr_rps} rows/s (~${sink_avg_mb_s} MB/s)"
  plog "Final cumulative throughput: ${final_cum_rps} rows/s (~${final_cum_mb_s} MB/s)"
  # Final snapshot once: measure from highest LSN publish to snapshot complete
  lsn_publish=$(get_latest_lsn_and_publish_ms)
  lsn=$(echo "${lsn_publish}" | cut -d',' -f1)
  publish_ms=$(echo "${lsn_publish}" | cut -d',' -f2)
  if [[ -z "${lsn}" || "${lsn}" == "" ]]; then lsn=${final_lsn}; fi
  snap_req_start_ms=$(date +%s%3N)
  SNAP_PAYLOAD=$(jq -n --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" --argjson lsn ${lsn} '{database:$db, table:$tbl, lsn:$lsn}')
  curl -sS -X POST "${REST_BASE_HOST}/tables/${SRC_TABLE_NAME}/snapshot" -H "content-type: application/json" -d "${SNAP_PAYLOAD}" > /dev/null || true
  snap_req_end_ms=$(date +%s%3N)
  if [[ -z "${publish_ms}" ]]; then publish_ms=${snap_req_start_ms}; fi
  snapshot_total_latency_ms=$(( snap_req_end_ms - publish_ms ))
  snapshot_request_delay_ms=$(( snap_req_start_ms - publish_ms ))
  snapshot_http_ms=$(( snap_req_end_ms - snap_req_start_ms ))
  echo "${snap_req_end_ms},${lsn},${publish_ms},${snapshot_total_latency_ms},${snapshot_request_delay_ms},${snapshot_http_ms}" >> "${PROFILING_CSV_PATH}"
  plog "Final snapshot: lsn=${lsn} total_latency_ms=${snapshot_total_latency_ms} request_delay_ms=${snapshot_request_delay_ms} http_ms=${snapshot_http_ms}"
  plog "======================================================"
  # Auto-stop after profiling (concise output)
  DROP_PAYLOAD=$(jq -n --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '{database:$db, table:$tbl}')
  DROP_STATUS=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${REST_BASE_HOST}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${DROP_PAYLOAD}" || true)
  SRC_DEL=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${CONNECT_BASE}/connectors/${SOURCE_NAME}" || true)
  SNK_DEL=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${CONNECT_BASE}/connectors/${SINK_NAME}" || true)
  if docker compose down -v >/dev/null 2>&1; then DC_STATUS="OK"; else DC_STATUS="FAIL"; fi
  plog "Stopped (table_drop=${DROP_STATUS}, delete_src=${SRC_DEL}, delete_sink=${SNK_DEL}, compose=${DC_STATUS})."
else
  echo "Profiling disabled: SOURCE_MAX_DURATION is not a positive integer (${SOURCE_MAX_DURATION})."
fi



