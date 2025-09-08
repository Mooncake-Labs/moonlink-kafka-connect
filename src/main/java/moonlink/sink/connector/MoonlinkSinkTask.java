package moonlink.sink.connector;

import java.util.Collection;
import java.util.Map;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import moonlink.client.MoonlinkClient;
import moonlink.client.Dto;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.arrow.vector.types.pojo.Schema;
import moonlink.client.MoonlinkRowConverter;

public class MoonlinkSinkTask extends SinkTask {

    private final Logger log = LoggerFactory.getLogger(MoonlinkSinkTask.class);

    private MoonlinkSinkConnectorConfig config;
    private MoonlinkClient client;
    private long sinkStartTimeMs;
    private long sinkLastLogSecond;
    private int sinkCompletedThisSecond;
    private long sinkTotalCompleted;
    private long latestLsn;
    private long httpCallTimeTotalMs;
    private long serializeTimeTotalMs;
    private long httpCallCount;
    private long serializeCount;

    private Schema arrowSchema;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new MoonlinkSinkConnectorConfig(props);
        log.info("Moonlink Sink Task starting with props: {}", props);
        try {
            client = new MoonlinkClient(config.getString(MoonlinkSinkConnectorConfig.MOONLINK_URI));
            // Build converter from configured table schema JSON
            // String schemaJson = config.getString(MoonlinkSinkConnectorConfig.TABLE_SCHEMA_JSON);
            // com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            // Dto.FieldSchema[] fields = mapper.readValue(schemaJson, Dto.FieldSchema[].class);
            // schemaFields = java.util.Arrays.asList(fields);

            // Deserialize Arrow schema passed from connector as JSON
            String arrowSchemaJson = props.get("arrow.schema.json");
            if (arrowSchemaJson == null || arrowSchemaJson.isEmpty()) {
                throw new ConnectException("Missing 'arrow.schema.json' in task properties");
            }
            arrowSchema = Schema.fromJSON(arrowSchemaJson);
            sinkStartTimeMs = System.currentTimeMillis();
            sinkLastLogSecond = sinkStartTimeMs / 1000L;
            sinkCompletedThisSecond = 0;
            sinkTotalCompleted = 0L;
            latestLsn = 0L;
            httpCallTimeTotalMs = 0L;
            serializeTimeTotalMs = 0L;
            httpCallCount = 0L;
            serializeCount = 0L;
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
            log.info("Processing record: topic={}, partition={}, offset={}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            try {
                String database = config.getString(MoonlinkSinkConnectorConfig.DATABASE_NAME);
                String table = config.getString(MoonlinkSinkConnectorConfig.TABLE_NAME);
                String srcTableName = database + "." + table;
                Object value = record.value();
                if (value == null) {
                    throw new DataException("Record was null");
                }

                // Build MoonlinkRow protobuf from Arrow schema and record value
                long serializeStartNs = System.nanoTime();
                moonlink.Row.MoonlinkRow row = MoonlinkRowConverter.convert(value, arrowSchema).build();
                byte[] serializedRow = row.toByteArray();
                long serializeElapsedMs = (System.nanoTime() - serializeStartNs) / 1_000_000L;
                serializeTimeTotalMs += serializeElapsedMs;
                serializeCount++;

                long httpStartNs = System.nanoTime();
                var resp = client.insertRowProtobuf(srcTableName, serializedRow);
                long httpElapsedMs = (System.nanoTime() - httpStartNs) / 1_000_000L;
                httpCallTimeTotalMs += httpElapsedMs;
                httpCallCount++;
                Long respLsn = resp.lsn;
                if (respLsn != null) {
                    if (respLsn > batchMaxLsn) batchMaxLsn = respLsn;
                    if (respLsn > latestLsn) latestLsn = respLsn;
                }
                log.info("Inserted row, lsn={}", respLsn);
                processedThisBatch++;
                sinkCompletedThisSecond++;
                sinkTotalCompleted++;
            } catch (Exception e) {
                throw new DataException("Failed to ingest record", e);
            }
        }
        long batchElapsedNs = System.nanoTime() - batchStartNs;
        long batchElapsedMs = batchElapsedNs / 1_000_000L;
        double batchThroughputRps = processedThisBatch * 1000.0 / Math.max(1L, batchElapsedMs);
        long sinceStartMs = System.currentTimeMillis() - sinkStartTimeMs;
        double avgThroughputRps = sinkTotalCompleted * 1000.0 / Math.max(1L, sinceStartMs);
        double avgHttpMs = httpCallCount > 0 ? (httpCallTimeTotalMs * 1.0 / httpCallCount) : 0.0;
        double avgSerializeMs = serializeCount > 0 ? (serializeTimeTotalMs * 1.0 / serializeCount) : 0.0;
        log.info(
            "Sink poll completed: processed={}, elapsed_ms={}, throughput_rps={}, total_completed={}, avg_throughput_rps={}, last_lsn={}, avg_http_ms={}, avg_serialize_ms={}",
            processedThisBatch,
            batchElapsedMs,
            String.format("%.2f", batchThroughputRps),
            sinkTotalCompleted,
            String.format("%.2f", avgThroughputRps),
            batchMaxLsn,
            String.format("%.2f", avgHttpMs),
            String.format("%.2f", avgSerializeMs)
        );
        logSinkStatsIfNeeded(System.currentTimeMillis());
    }

    private void logSinkStatsIfNeeded(long nowMs) {
        long currentSecond = nowMs / 1000L;
        if (currentSecond > sinkLastLogSecond) {
            long elapsed = (nowMs - sinkStartTimeMs) / 1000L;
            log.info("Sink task stats: elapsed={}s, completed_last_second={}, total_completed={} rps={}",
                elapsed, sinkCompletedThisSecond, sinkTotalCompleted, sinkTotalCompleted * 1000.0 / Math.max(1L, elapsed));
            sinkCompletedThisSecond = 0;
            sinkLastLogSecond = currentSecond;
        }
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
}


