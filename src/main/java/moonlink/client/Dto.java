package moonlink.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.util.List;
import java.util.Map;
import java.io.IOException;

public final class Dto {
    public static final class FieldSchema {
        public String name;
        @JsonProperty("data_type")
        public String dataType;
        public boolean nullable;
    }

    public static final class CreateTableRequest {
        public String database;
        public String table;
        public List<FieldSchema> schema;
        @JsonProperty("table_config")
        public Map<String, Object> tableConfig;
    }

    public static final class CreateTableResponse {
        public String database;
        public String table;
        public long lsn;
    }

    public static final class TableStatus {
        public String database;
        public String table;
        @JsonProperty("commit_lsn")
        public Long commitLsn;
        @JsonProperty("flush_lsn")
        public Long flushLsn;
        public Long cardinality;
        @JsonProperty("iceberg_warehouse_location")
        public String icebergWarehouseLocation;
    }

    public static final class ListTablesResponse {
        public List<TableStatus> tables;
    }

    public enum RequestMode {
        @JsonProperty("async")
        Async,
        @JsonProperty("sync")
        Sync
    }

    public static final class IngestRequest {
        public String operation;
        public Object data;
        @JsonProperty("request_mode")
        public RequestMode requestMode;
    }

    public static final class IngestResponse {
        public String table;
        public String operation;
        public Long lsn;
    }

    public static final class IngestProtobufRequest {
        public String operation;
        @JsonSerialize(using = ByteArrayAsArraySerializer.class)
        public byte[] data;
        @JsonProperty("request_mode")
        public RequestMode requestMode;
    }

    // Custom serializer for byte[] to JSON array to send protobuf request to moonlink
    public static final class ByteArrayAsArraySerializer extends JsonSerializer<byte[]> {
        @Override
        public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartArray(value, value.length);
            for (byte b : value) {
                gen.writeNumber(b & 0xFF);
            }
            gen.writeEndArray();
        }
    }
}


