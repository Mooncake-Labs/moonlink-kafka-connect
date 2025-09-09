package moonlink.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.connect.errors.ConnectException;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MoonlinkClient {
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String baseUrl;

    public MoonlinkClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(5)).build();
        this.mapper = new ObjectMapper();
    }

    public Dto.ListTablesResponse listTables() throws Exception {
        var req = HttpRequest.newBuilder(URI.create(baseUrl + "/tables"))
                .GET()
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        return mapper.readValue(res.body(), Dto.ListTablesResponse.class);
    }

    public boolean tableExists(String database, String table) throws Exception {
        Dto.ListTablesResponse resp = listTables();
        List<Dto.TableStatus> tables = resp.tables;
        if (tables == null) return false;
        for (Dto.TableStatus t : tables) {
            if (Objects.equals(t.database, database) && Objects.equals(t.table, table)) return true;
        }
        return false;
    }

    public Dto.CreateTableResponse createTable(Dto.CreateTableRequest body) throws Exception {
        var json = mapper.writeValueAsString(body);
        String srcTableName = body.database + "." + body.table;
        var req = HttpRequest.newBuilder(URI.create(baseUrl + "/tables/" + srcTableName))
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        return mapper.readValue(res.body(), Dto.CreateTableResponse.class);
    }

    public Dto.IngestResponse insertRow(String table, Map<String, Object> row) throws Exception {
        Dto.IngestRequest body = new Dto.IngestRequest();
        body.operation = "insert";
        body.data = row;
        body.requestMode = Dto.RequestMode.Sync; // wait for lsn
        String json = mapper.writeValueAsString(body);
        var req = HttpRequest.newBuilder(URI.create(baseUrl + "/ingest/" + table))
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        return mapper.readValue(res.body(), Dto.IngestResponse.class);
    }

    public Dto.IngestResponse insertRowProtobuf(String srcTableName, byte[] serializedRowProto) throws Exception {
        Dto.IngestProtobufRequest body = new Dto.IngestProtobufRequest();
        body.operation = "insert";
        body.data = serializedRowProto; // custom serializer writes as JSON array
        body.requestMode = Dto.RequestMode.Sync; // wait for lsn
        String json = mapper.writeValueAsString(body);
        var req = HttpRequest.newBuilder(URI.create(baseUrl + "/ingestpb/" + srcTableName))
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();
                
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        return mapper.readValue(res.body(), Dto.IngestResponse.class);
    }

    // Raw Avro ingestion for Kafka payloads (Confluent wire format). Server assumes insert + async.
    public Dto.IngestResponse insertRowAvroRaw(String srcTableName, byte[] avroBytes) throws Exception {
        var req = HttpRequest.newBuilder(URI.create(baseUrl + "/kafka/" + srcTableName + "/ingest"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(avroBytes))
                .header("Content-Type", "application/octet-stream")
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        return mapper.readValue(res.body(), Dto.IngestResponse.class);
    }

    // Raw Protobuf ingestion without JSON wrapper (legacy utility; server path may differ)
    public Dto.IngestResponse insertRowProtobufRaw(String srcTableName, byte[] serializedRowProto) throws Exception {
        var req = HttpRequest.newBuilder(URI.create(baseUrl + "/ingestpb_raw/" + srcTableName))
                .POST(HttpRequest.BodyPublishers.ofByteArray(serializedRowProto))
                .header("Content-Type", "application/octet-stream")
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        return mapper.readValue(res.body(), Dto.IngestResponse.class);
    }

    public org.apache.arrow.vector.types.pojo.Schema fetchArrowSchema(String database, String table) throws Exception {
        String uri = baseUrl + "/schema/" + database + "/" + table;
        var req = HttpRequest.newBuilder(URI.create(uri))
                .GET()
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        JsonNode root = mapper.readTree(res.body());
        JsonNode schemaNode = root.get("schema");
        if (schemaNode == null || schemaNode.isNull()) {
            throw new ConnectException("Moonlink fetch schema returned no 'schema' field");
        }

        System.out.println("Schema: " + schemaNode.toString());
        return org.apache.arrow.vector.types.pojo.Schema.fromJSON(schemaNode.toString());
    }

    public org.apache.arrow.vector.types.pojo.Schema fetchArrowSchemaIpc(String database, String table) throws Exception {
        String uri = baseUrl + "/schema/" + database + "/" + table;
        var req = HttpRequest.newBuilder(URI.create(uri))
                .GET()
                .header("Accept", "application/json")
                .build();
        var res = http.send(req, HttpResponse.BodyHandlers.ofString());
        ensure2xx(res);
        JsonNode root = mapper.readTree(res.body());
        JsonNode schemaNode = root.get("serialized_schema");
        if (schemaNode == null || !schemaNode.isArray()) {
            throw new ConnectException("Moonlink fetch schema returned no 'serialized_schema' array");
        }
        byte[] bytes = new byte[schemaNode.size()];
        for (int i = 0; i < schemaNode.size(); i++) {
            bytes[i] = (byte) (schemaNode.get(i).asInt() & 0xFF);
        }
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), new RootAllocator())) {
            VectorSchemaRoot rootSchema = reader.getVectorSchemaRoot();
            return rootSchema.getSchema();
        } catch (Exception e) {
            throw new ConnectException("Failed to parse Arrow IPC schema: " + e.getMessage(), e);
        }
    }

    private static void ensure2xx(HttpResponse<?> res) {
        int s = res.statusCode();
        if (s < 200 || s >= 300) {
            throw new ConnectException("Moonlink HTTP " + s + ": " + res.body());
        }
    }
}
