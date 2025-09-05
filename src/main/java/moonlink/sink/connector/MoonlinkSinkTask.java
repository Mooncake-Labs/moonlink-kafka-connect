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
    private int sinkReceivedThisSecond;
    private long sinkTotalReceived;

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
            sinkReceivedThisSecond = 0;
            sinkTotalReceived = 0L;
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize Moonlink client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Moonlink Sink Task received {} records", records.size());
        long nowMs = System.currentTimeMillis();
        sinkReceivedThisSecond += records.size();
        sinkTotalReceived += records.size();
        logSinkStatsIfNeeded(nowMs);
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
                moonlink.Row.MoonlinkRow row = MoonlinkRowConverter.convert(value, arrowSchema).build();
                byte[] serializedRow = row.toByteArray();
                var resp = client.insertRowProtobuf(srcTableName, serializedRow);
                log.info("Inserted row, lsn={}", resp.lsn);
            } catch (Exception e) {
                throw new DataException("Failed to ingest record", e);
            }
        }
    }

    private void logSinkStatsIfNeeded(long nowMs) {
        long currentSecond = nowMs / 1000L;
        if (currentSecond > sinkLastLogSecond) {
            long elapsed = (nowMs - sinkStartTimeMs) / 1000L;
            log.info("Sink task stats: elapsed={}s, recv_last_second={}, total_received={}",
                elapsed, sinkReceivedThisSecond, sinkTotalReceived);
            sinkReceivedThisSecond = 0;
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


