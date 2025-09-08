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

echo "Building connector jar..."
mvn -q -DskipTests package

echo "Starting local Kafka+Connect..."
docker compose up -d --force-recreate kafka connect

echo "Waiting for Connect REST API..."
for i in {1..30}; do
  if curl -fsS http://localhost:8083/ > /dev/null; then break; fi; sleep 1; done

echo "Checking remote Moonlink health at ${REST_BASE}..."
curl -fsS "${REST_BASE}/health" | (command -v jq >/dev/null 2>&1 && jq || cat)

echo "Checking existing tables on remote..."
TABLES_JSON=$(curl -sS "${REST_BASE}/tables" || echo '{}')
echo "Tables response:"; echo "${TABLES_JSON}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
EXISTS=$(echo "${TABLES_JSON}" | jq -r --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" '.tables | map(select(.database==$db and .table==$tbl)) | length > 0')

if [[ "${RECREATE}" == "true" && "${EXISTS}" == "true" ]]; then
  echo "Dropping table ${SRC_TABLE_NAME} on remote Moonlink (RECREATE=true)..."
  DROP_PAYLOAD=$(cat <<EOF
{ "database": "${ML_DATABASE}", "table": "${ML_TABLE}" }
EOF
)
  echo "DROP payload:"; echo "${DROP_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  curl -sS -X DELETE "${REST_BASE}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${DROP_PAYLOAD}" \
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

if [[ "${EXISTS}" != "true" || "${RECREATE}" == "true" ]]; then
  echo "Creating table ${SRC_TABLE_NAME} on remote Moonlink..."
CREATE_PAYLOAD=$(cat <<EOF
{\
  "database": "${ML_DATABASE}",\
  "table": "${ML_TABLE}",\
  "schema": ${ML_SCHEMA_JSON},\
  "table_config": ${TABLE_CONFIG}\
}
EOF
)
echo "CREATE payload:"; echo "${CREATE_PAYLOAD}" | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
  curl -sS -X POST "${REST_BASE}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${CREATE_PAYLOAD}" \
    -w "\nHTTP_STATUS:%{http_code}\n" | cat
else
  echo "Skipping create; table exists and RECREATE is false."
fi

echo "Preflight: fetch schema from remote before starting sink..."
curl -sS "${REST_BASE}/schema/${ML_DATABASE}/${ML_TABLE}" -w "\nHTTP_STATUS:%{http_code}\n" | cat || true

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
echo "${SOURCE_PAYLOAD}" | curl -fsS -X POST -H "Content-Type:application/json" -d @- http://localhost:8083/connectors | (command -v jq >/dev/null 2>&1 && jq -C . || cat)

echo "Registering sink connector (${SINK_NAME}, remote ${REST_BASE})..."
SINK_PAYLOAD=$(jq -n \
  --arg name "${SINK_NAME}" \
  --arg tasks "${SINK_TASKS_MAX}" \
  --arg topics "${SOURCE_TOPIC}" \
  --arg uri "${REST_BASE}" \
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
echo "${SINK_PAYLOAD}" | curl -fsS -X POST -H "Content-Type:application/json" -d @- http://localhost:8083/connectors | (command -v jq >/dev/null 2>&1 && jq -C . || cat)

echo "Sleeping for 2 seconds to ensure the connector is ready..."
sleep 2

echo "Statuses:"
curl -s http://localhost:8083/connectors/${SOURCE_NAME}/status | (command -v jq >/dev/null 2>&1 && jq -C . || cat)
curl -s http://localhost:8083/connectors/${SINK_NAME}/status | (command -v jq >/dev/null 2>&1 && jq -C . || cat)

echo "Start complete."


# ==========================
# LSN-based profiling with synchronous snapshot latency
# ==========================

if [[ "${SOURCE_MAX_DURATION}" =~ ^[0-9]+$ && "${SOURCE_MAX_DURATION}" -gt 0 ]]; then
  TARGET_MESSAGES=$(( SOURCE_MSGS_PER_SEC * SOURCE_MAX_DURATION ))
  echo "Profiling (LSN-based) enabled: target_messages=${TARGET_MESSAGES}" 

  # Helpers
  get_latest_lsn_and_publish_ms() {
    # Find the latest line carrying an LSN and parse both LSN and the docker ISO timestamp (publish time)
    local line
    line=$(docker compose logs -t --since=90s connect 2>/dev/null | \
      grep -E 'Inserted row, lsn=|Sink poll completed: ' | tail -n 1)
    local lsn
    lsn=$(echo "${line}" | grep -Eo 'lsn=[0-9]+' | sed 's/lsn=//')
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

  echo "snapshot_end_ms,lsn,publish_ms,latency_ms" > lsn_snapshot_latency.csv

  while true; do
    loop_start_ms=$(date +%s%3N)

    # Check if source reported finished; if yes, print latest LSN alongside it
    latest_source_line=$(docker compose logs -t --since=90s connect 2>/dev/null | grep -E 'Source task finished:' | tail -n 1 || true)
    if [[ -n "${latest_source_line}" && "${saw_source_finished}" = false ]]; then
      cur_lsn=$(get_latest_lsn_and_publish_ms | cut -d',' -f1)
      cur_lsn=${cur_lsn:-0}
      echo "Detected source finish: ${latest_source_line} (latest_lsn=${cur_lsn})"
      saw_source_finished=true
      final_lsn=${cur_lsn}
    fi

    # Sample latest LSN and its publish time, then trigger synchronous snapshot
    lsn_publish=$(get_latest_lsn_and_publish_ms)
    lsn=$(echo "${lsn_publish}" | cut -d',' -f1)
    publish_ms=$(echo "${lsn_publish}" | cut -d',' -f2)
    lsn=${lsn:-0}
    snap_req_start_ms=$(date +%s%3N)
    SNAP_PAYLOAD=$(jq -n --arg db "${ML_DATABASE}" --arg tbl "${ML_TABLE}" --argjson lsn ${lsn} '{database:$db, table:$tbl, lsn:$lsn}')
    curl -sS -X POST "${REST_BASE}/tables/${SRC_TABLE_NAME}/snapshot" -H "content-type: application/json" -d "${SNAP_PAYLOAD}" > /dev/null || true
    snap_req_end_ms=$(date +%s%3N)
    # Latency defined as when snapshot completed minus when LSN was published by sink
    if [[ -z "${publish_ms}" ]]; then
      publish_ms=${snap_req_start_ms}
      echo "Warning: publish_ms is empty, using snap_req_start_ms instead"
    fi
    latency_ms=$(( snap_req_end_ms - publish_ms ))
    now_ms=${snap_req_end_ms}
    echo "${now_ms},${lsn},${publish_ms},${latency_ms}" >> lsn_snapshot_latency.csv
    echo "[${now_ms}] snapshot lsn=${lsn} publish_ms=${publish_ms} latency_ms=${latency_ms}"

    # If we observed source finished and snapshot at final_lsn has completed (lsn >= final_lsn once), we can stop
    if [[ "${saw_source_finished}" = true && ${lsn} -ge ${final_lsn} ]]; then
      echo "Final LSN ${final_lsn} reached; stopping profiling"
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
  echo
  echo "================ LSN Profiling Summary ================"
  echo "Final observed LSN: ${final_lsn}"
  echo "Total wall time: ${total_s}s"
  if command -v awk >/dev/null 2>&1; then
    avg_latency=$(awk -F',' 'NR>1 {sum+=$4; n++} END { if(n>0) printf("%.2f", sum/n); else print 0 }' lsn_snapshot_latency.csv)
    echo "Average snapshot latency: ${avg_latency} ms"
  fi
  echo "Per-sample snapshot latency saved to lsn_snapshot_latency.csv (timestamp_ms, latest_lsn, latency_ms)"
  echo "======================================================"
  # Auto-stop after profiling (concise output)
  DROP_PAYLOAD=$(cat <<EOF
{ "database": "${ML_DATABASE}", "table": "${ML_TABLE}" }
EOF
)
  DROP_STATUS=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${REST_BASE}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${DROP_PAYLOAD}" || true)
  SRC_DEL=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE http://localhost:8083/connectors/${SOURCE_NAME} || true)
  SNK_DEL=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE http://localhost:8083/connectors/${SINK_NAME} || true)
  if docker compose down -v >/dev/null 2>&1; then DC_STATUS="OK"; else DC_STATUS="FAIL"; fi
  echo "Stopped (table_drop=${DROP_STATUS}, delete_src=${SRC_DEL}, delete_sink=${SNK_DEL}, compose=${DC_STATUS})."
else
  echo "Profiling disabled: SOURCE_MAX_DURATION is not a positive integer (${SOURCE_MAX_DURATION})."
fi

