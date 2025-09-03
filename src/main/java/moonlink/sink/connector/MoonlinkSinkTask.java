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
import org.apache.kafka.connect.errors.ConnectException;

public class MoonlinkSinkTask extends SinkTask {

    private final Logger log = LoggerFactory.getLogger(MoonlinkSinkTask.class);

    private MoonlinkSinkConnectorConfig config;
    private MoonlinkClient client;
    private List<Dto.FieldSchema> schemaFields;

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
            String schemaJson = config.getString(MoonlinkSinkConnectorConfig.TABLE_SCHEMA_JSON);
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            Dto.FieldSchema[] fields = mapper.readValue(schemaJson, Dto.FieldSchema[].class);
            schemaFields = java.util.Arrays.asList(fields);
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize Moonlink client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Moonlink Sink Task received {} records", records.size());
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

                // For now, build a mock MoonlinkRow protobuf matching the configured schema
                byte[] serializedRow = buildMockMoonlinkRowBytes(schemaFields);
                var resp = client.insertRowProtobuf(srcTableName, serializedRow);
                log.info("Inserted row, lsn={}", resp.lsn);
            } catch (Exception e) {
                throw new DataException("Failed to ingest record", e);
            }
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

    private static byte[] buildMockMoonlinkRowBytes(List<Dto.FieldSchema> schemaFields) {
        moonlink.Row.MoonlinkRow.Builder rowBuilder = moonlink.Row.MoonlinkRow.newBuilder();
        for (Dto.FieldSchema f : schemaFields) {
            String dt = f.dataType == null ? "" : f.dataType.toLowerCase();
            moonlink.Row.RowValue.Builder rv = moonlink.Row.RowValue.newBuilder();
            if (dt.equals("int32")) {
                rv.setInt32(1);
            } else if (dt.equals("int64")) {
                rv.setInt64(1L);
            } else if (dt.equals("float32")) {
                rv.setFloat32(1.0f);
            } else if (dt.equals("float64")) {
                rv.setFloat64(1.0d);
            } else if (dt.equals("boolean") || dt.equals("bool")) {
                rv.setBool(true);
            } else if (dt.equals("string") || dt.equals("text")) {
                rv.setBytes(com.google.protobuf.ByteString.copyFromUtf8("mock"));
            } else if (dt.equals("date32")) {
                rv.setInt32(1); // days since epoch
            } else if (dt.startsWith("decimal(")) {
                // 16 zero bytes for decimal128 two's complement
                rv.setDecimal128Be(com.google.protobuf.ByteString.copyFrom(new byte[16]));
            } else {
                rv.setNull(moonlink.Row.Null.newBuilder().build());
            }
            rowBuilder.addValues(rv.build());
        }
        moonlink.Row.MoonlinkRow row = rowBuilder.build();
        return row.toByteArray();
    }
}


