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


public class MoonlinkSinkTask extends SinkTask {

    private final Logger log = LoggerFactory.getLogger(MoonlinkSinkTask.class);

    private MoonlinkSinkConnectorConfig config;
    private MoonlinkClient client;
    private long sinkStartTimeMs;
    private long sinkLastLogSecond;
    private int sinkCompletedThisSecond;
    private long sinkTotalCompleted;
    private long latestLsn;
    private long httpCallTimeTotalNs;
    private long serializeTimeTotalNs;
    private long httpCallCount;
    private long serializeCount;
    private String taskId;

    private String schemaRegistryUrl;

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
            schemaRegistryUrl = config.getString(MoonlinkSinkConnectorConfig.SCHEMA_REGISTRY_URL);
            sinkStartTimeMs = System.currentTimeMillis();
            sinkLastLogSecond = sinkStartTimeMs / 1000L;
            sinkCompletedThisSecond = 0;
            sinkTotalCompleted = 0L;
            latestLsn = 0L;
            httpCallTimeTotalNs = 0L;
            serializeTimeTotalNs = 0L;
            httpCallCount = 0L;
            serializeCount = 0L;
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
        long batchHttpNsTotal = 0L;
        long batchSerializeNsTotal = 0L;
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
                if (!(value instanceof byte[])) {
                    throw new DataException("Expected byte[] value with ByteArrayConverter");
                }
                byte[] bytes = (byte[]) value;

                // Validate Confluent Avro magic byte (0) and extract schema id (big-endian int)
                if (bytes.length < 5 || bytes[0] != 0) {
                    throw new DataException("Invalid Confluent Avro payload: missing magic byte or too short");
                }
                int schemaId = ByteBuffer.wrap(bytes, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt();

                // Try to fetch schema from Schema Registry to validate connectivity
                boolean schemaFound = false;
                try {
                    java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
                    String url = schemaRegistryUrl + "/schemas/ids/" + schemaId;
                    java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url)).GET().build();
                    java.net.http.HttpResponse<String> res = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
                    schemaFound = res.statusCode() == 200;
                } catch (Exception ex) {
                    schemaFound = false;
                }
                log.info("Schema id {} found in registry: {}", schemaId, schemaFound ? "yes" : "no");

                long httpStartNs = System.nanoTime();
                var resp = client.insertRowAvroRaw(srcTableName, bytes);
                long httpElapsedNs = (System.nanoTime() - httpStartNs);
                httpCallTimeTotalNs += httpElapsedNs;
                batchHttpNsTotal += httpElapsedNs;
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
        double batchElapsedMs = batchElapsedNs / 1_000_000.0;
        double batchThroughputRps = processedThisBatch / Math.max(0.001, (batchElapsedNs / 1_000_000_000.0));
        long sinceStartMs = System.currentTimeMillis() - sinkStartTimeMs;
        double avgThroughputRps = sinkTotalCompleted * 1000.0 / Math.max(1L, sinceStartMs);
        double avgHttpMs = httpCallCount > 0 ? ((httpCallTimeTotalNs / (double) httpCallCount) / 1_000_000.0) : 0.0;
        double avgSerializeMs = serializeCount > 0 ? ((serializeTimeTotalNs / (double) serializeCount) / 1_000_000.0) : 0.0;
        // Clean per-batch log (consumed by profiler)
        log.info(
            "Sink task batch: task_id={} processed={} elapsed_ms={} rps={} total_completed={} last_lsn={} http_ms_batch={} serialize_ms_batch={} avg_http_ms={} avg_serialize_ms={}",
            taskId,
            processedThisBatch,
            String.format("%.3f", batchElapsedMs),
            String.format("%.2f", batchThroughputRps),
            sinkTotalCompleted,
            batchMaxLsn,
            String.format("%.3f", (batchHttpNsTotal / 1_000_000.0)),
            String.format("%.3f", (batchSerializeNsTotal / 1_000_000.0)),
            String.format("%.2f", avgHttpMs),
            String.format("%.2f", avgSerializeMs)
        );
        // Legacy summary line (kept for compatibility)
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


