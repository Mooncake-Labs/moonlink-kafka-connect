#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

QUIET=false
if [[ "${1:-}" == "--quiet" ]]; then
  QUIET=true
  shift || true
fi

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
else
  REST_BASE="${REMOTE_BASE:-${REST_BASE:-}}"
  if [[ -z "${REST_BASE}" ]]; then
    echo "ERROR: Set REMOTE_BASE (e.g. http://ec2-xx-xx-xx-xx.compute-1.amazonaws.com:3030) or provide a config path as first arg" >&2
    exit 1
  fi
fi

ML_DATABASE="${ML_DATABASE:-${ML_DATABASE:-test_db}}"
ML_TABLE="${ML_TABLE:-${ML_TABLE:-test_table}}"
SRC_TABLE_NAME="${ML_DATABASE}.${ML_TABLE}"

if [[ "${QUIET}" != true ]]; then echo "Dropping table ${SRC_TABLE_NAME} on remote Moonlink..."; fi
DROP_PAYLOAD=$(cat <<EOF
{ "database": "${ML_DATABASE}", "table": "${ML_TABLE}" }
EOF
)
DROP_STATUS=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${REST_BASE}/tables/${SRC_TABLE_NAME}" -H "content-type: application/json" -d "${DROP_PAYLOAD}" || true)
if [[ "${QUIET}" != true ]]; then echo "Table drop HTTP status: ${DROP_STATUS}"; fi

SOURCE_NAME="${SOURCE_NAME:-example-source-connector}"
SINK_NAME="${SINK_NAME:-moonlink-sink-connector}"
if [[ "${QUIET}" != true ]]; then echo "Deleting connectors (${SOURCE_NAME}, ${SINK_NAME})..."; fi
SRC_DEL=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE http://localhost:8083/connectors/${SOURCE_NAME} || true)
SNK_DEL=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE http://localhost:8083/connectors/${SINK_NAME} || true)

if [[ "${QUIET}" != true ]]; then echo "Stopping local Kafka+Connect..."; fi
if docker compose down -v >/dev/null 2>&1; then
  DC_STATUS="OK"
else
  DC_STATUS="FAIL"
fi

if [[ "${QUIET}" == true ]]; then
  echo "Stop complete (table_drop=${DROP_STATUS}, delete_src=${SRC_DEL}, delete_sink=${SNK_DEL}, compose=${DC_STATUS})."
else
  echo "Stop complete (table_drop=${DROP_STATUS}, delete_src=${SRC_DEL}, delete_sink=${SNK_DEL}, compose=${DC_STATUS})."
fi


