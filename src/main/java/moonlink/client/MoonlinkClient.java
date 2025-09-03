package moonlink.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.errors.ConnectException;

public class MoonlinkClient {
  private final HttpClient http;
  private final ObjectMapper mapper;
  private final String baseUrl;

  public MoonlinkClient(String baseUrl) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.http =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    this.mapper = new ObjectMapper();
  }

  public Dto.ListTablesResponse listTables() throws Exception {
    var req =
        HttpRequest.newBuilder(URI.create(baseUrl + "/tables"))
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

  // Raw Avro ingestion for Kafka payloads (Confluent wire format). Server assumes insert + async.
  public Dto.IngestResponse insertRowAvroRaw(String srcTableName, byte[] avroBytes)
      throws Exception {
    var req =
        HttpRequest.newBuilder(URI.create(baseUrl + "/kafka/" + srcTableName + "/ingest"))
            .POST(HttpRequest.BodyPublishers.ofByteArray(avroBytes))
            .header("Content-Type", "application/octet-stream")
            .header("Accept", "application/json")
            .build();
    var res = http.send(req, HttpResponse.BodyHandlers.ofString());
    ensure2xx(res);
    return mapper.readValue(res.body(), Dto.IngestResponse.class);
  }

  public Dto.SetAvroSchemaResponse setAvroSchema(
      String database, String table, String srcTableName, String avroSchemaJson, long schemaId)
      throws Exception {
    Dto.SetAvroSchemaRequest body = new Dto.SetAvroSchemaRequest();
    body.database = database;
    body.table = table;
    body.kafkaSchema = avroSchemaJson;
    body.schemaId = schemaId;
    String json = mapper.writeValueAsString(body);
    var req =
        HttpRequest.newBuilder(URI.create(baseUrl + "/kafka/" + srcTableName + "/schema"))
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .build();
    var res = http.send(req, HttpResponse.BodyHandlers.ofString());
    ensure2xx(res);
    return mapper.readValue(res.body(), Dto.SetAvroSchemaResponse.class);
  }

  public Dto.SyncFlushResponse waitForFlush(String database, String table, long lsn)
      throws Exception {
    Dto.SyncFlushRequest body = new Dto.SyncFlushRequest();
    body.database = database;
    body.table = table;
    body.lsn = lsn;
    String json = mapper.writeValueAsString(body);
    var req =
        HttpRequest.newBuilder(URI.create(baseUrl + "/tables/" + database + "." + table + "/flush"))
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(30))
            .build();
    var res = http.send(req, HttpResponse.BodyHandlers.ofString());
    ensure2xx(res);
    return mapper.readValue(res.body(), Dto.SyncFlushResponse.class);
  }

  private static void ensure2xx(HttpResponse<?> res) {
    int s = res.statusCode();
    if (s < 200 || s >= 300) {
      throw new ConnectException("Moonlink HTTP " + s + ": " + res.body());
    }
  }
}
