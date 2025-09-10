package moonlink.sink.connector;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import moonlink.client.MoonlinkClient;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.ConnectException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;


public class MoonlinkSinkTask extends SinkTask {

    private final Logger log = LoggerFactory.getLogger(MoonlinkSinkTask.class);

    private MoonlinkSinkConnectorConfig config;
    private MoonlinkClient client;
    private long latestLsn;
    private String taskId;

    private String schemaRegistryUrl;
    private Set<Integer> seenSchemaIds;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        config = new MoonlinkSinkConnectorConfig(props);
        log.info("Moonlink Sink Task starting with props: {}", props);
        try {
            client = new MoonlinkClient(config.getString(MoonlinkSinkConnectorConfig.MOONLINK_URI));
            schemaRegistryUrl = config.getString(MoonlinkSinkConnectorConfig.SCHEMA_REGISTRY_URL);
            seenSchemaIds = new HashSet<>();
            latestLsn = 0L;
            // Try to capture the Kafka Connect task id if present
            this.taskId = props.get("task.id");
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize Moonlink client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Moonlink Sink Task received {} records", records.size());
        long batchStartNs = System.nanoTime();
        int processedThisBatch = 0;
        long batchMaxLsn = latestLsn;
        for (SinkRecord record : records) {
            log.debug("Processing record: topic={}, partition={}, offset={}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            try {
                String database = config.getString(MoonlinkSinkConnectorConfig.DATABASE_NAME);
                String table = config.getString(MoonlinkSinkConnectorConfig.TABLE_NAME);
                String srcTableName = database + "." + table;
                Object value = record.value();
                if (value == null) {
                    throw new DataException("Record was null");
                }
                if (!(value instanceof byte[])) {
                    throw new DataException("Expected byte[] value with ByteArrayConverter");
                }
                byte[] bytes = (byte[]) value;

                // Validate Confluent Avro magic byte (0) and extract schema id (big-endian int)
                if (bytes.length < 5 || bytes[0] != 0) {
                    throw new DataException("Invalid Confluent Avro payload: missing magic byte or too short");
                }
                int schemaId = ByteBuffer.wrap(bytes, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt();

                ensureSchemaKnown(schemaId, database, table, srcTableName);

                // Strip Confluent wire format header (magic byte + schema id)
                byte[] avroDatum = Arrays.copyOfRange(bytes, 5, bytes.length);

                var resp = client.insertRowAvroRaw(srcTableName, avroDatum);
                Long respLsn = resp.lsn;
                if (respLsn != null) {
                    if (respLsn > batchMaxLsn) batchMaxLsn = respLsn;
                    if (respLsn > latestLsn) latestLsn = respLsn;
                }
                log.debug("Inserted row, lsn={}", respLsn);
                processedThisBatch++;
            } catch (Exception e) {
                throw new DataException("Failed to ingest record", e);
            }
        }
        long batchElapsedNs = System.nanoTime() - batchStartNs;
        double batchElapsedMs = batchElapsedNs / 1_000_000.0;
        double batchThroughputRps = processedThisBatch / Math.max(0.001, (batchElapsedNs / 1_000_000_000.0));
        log.info(
            "Sink task batch: task_id={} processed={} elapsed_ms={} rps={} last_lsn={}",
            taskId,
            processedThisBatch,
            String.format("%.3f", batchElapsedMs),
            String.format("%.2f", batchThroughputRps),
            batchMaxLsn
        );
    }

    @Override
    public void flush(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
        // TODO: Implement flush logic
        log.info("Moonlink Sink Task flush called with offsets: {}", offsets);
    }

    @Override
    public void stop() {
        log.info("Moonlink Sink Task stopping");
    }
    private void ensureSchemaKnown(int schemaId, String database, String table, String srcTableName) {
        if (seenSchemaIds.contains(schemaId)) {
            return;
        }
        try {
            java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
            String url = schemaRegistryUrl + "/schemas/ids/" + schemaId;
            java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url)).GET().build();
            java.net.http.HttpResponse<String> res = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                throw new DataException("Schema Registry returned status " + res.statusCode() + " for schema id " + schemaId);
            }
            JsonNode node = jsonMapper.readTree(res.body());
            JsonNode schemaNode = node.get("schema");
            if (schemaNode == null || schemaNode.isNull()) {
                throw new DataException("Schema Registry response missing 'schema' for id " + schemaId);
            }
            String schemaJson = schemaNode.asText();
            log.info("Registering Avro schema id {} with Moonlink", schemaId);
            client.setAvroSchema(database, table, srcTableName, schemaJson, (long) schemaId);
            seenSchemaIds.add(schemaId);
        } catch (Exception ex) {
            throw new DataException("Failed to register Avro schema id " + schemaId + " with Moonlink", ex);
        }
    }
}


