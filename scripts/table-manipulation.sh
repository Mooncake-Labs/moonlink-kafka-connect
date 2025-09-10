# create table
MOONLINK_URI=$(jq -r '.remote.moonlink_uri' /workspaces/moonlink-kafka-connect/profile-config.json)
DATABASE=$(jq -r '.table.database' /workspaces/moonlink-kafka-connect/profile-config.json)
TABLE_SHORT_NAME=$(jq -r '.table.name' /workspaces/moonlink-kafka-connect/profile-config.json)
SOURCE_TABLE_NAME="$DATABASE.$TABLE_SHORT_NAME"
CREATE_PAYLOAD=$(jq -c '{database: .table.database, table: .table.name, schema: .table.schema, table_config: { mooncake: { append_only: true, row_identity: "None" }, iceberg: { storage_config: { s3: { bucket: .table.storage.s3.bucket, region: .table.storage.s3.region, access_key_id: .table.storage.s3.access_key_id, secret_access_key: .table.storage.s3.secret_access_key, endpoint: .table.storage.s3.endpoint } } }, wal: { storage_config: { s3: { bucket: .table.storage.s3.bucket, region: .table.storage.s3.region, access_key_id: .table.storage.s3.access_key_id, secret_access_key: .table.storage.s3.secret_access_key, endpoint: .table.storage.s3.endpoint } } } } }' /workspaces/moonlink-kafka-connect/profile-config.json)
echo "Create table payload:"
echo "$CREATE_PAYLOAD" | jq -C '.'
echo "Creating table $SOURCE_TABLE_NAME"
curl -sS -X POST "$MOONLINK_URI/tables/$SOURCE_TABLE_NAME" -H 'Content-Type: application/json' -d "$CREATE_PAYLOAD" | jq -C '.' | cat

# drop table
# Usage: reads database/table from profile-config.json and drops the table remotely
MOONLINK_URI=$(jq -r '.remote.moonlink_uri' /workspaces/moonlink-kafka-connect/profile-config.json)
DATABASE=$(jq -r '.table.database' /workspaces/moonlink-kafka-connect/profile-config.json)
TABLE_SHORT_NAME=$(jq -r '.table.name' /workspaces/moonlink-kafka-connect/profile-config.json)
SOURCE_TABLE_NAME="$DATABASE.$TABLE_SHORT_NAME"
DROP_PAYLOAD=$(jq -c '{database: .table.database, table: .table.name}' /workspaces/moonlink-kafka-connect/profile-config.json)
echo "Drop table payload:"
echo "$DROP_PAYLOAD" | jq -C '.'
curl -sS -X DELETE "$MOONLINK_URI/tables/$SOURCE_TABLE_NAME" -H 'Content-Type: application/json' -d "$DROP_PAYLOAD" | jq -C '.' | cat