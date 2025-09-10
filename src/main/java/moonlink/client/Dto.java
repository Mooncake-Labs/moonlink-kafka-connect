package moonlink.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public final class Dto {
    public static final class SetAvroSchemaRequest {
        public String database;
        public String table;
        @JsonProperty("kafka_schema")
        public String kafkaSchema;
        @JsonProperty("schema_id")
        public long schemaId;
    }

    public static final class SetAvroSchemaResponse {
        public String database;
        public String table;
        @JsonProperty("schema_id")
        public long schemaId;
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

    public static final class IngestResponse {
        public String table;
        public String operation;
        public Long lsn;
    }
}


