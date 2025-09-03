package example.loadgen;

import static example.loadgen.LoadGeneratorConnectorConfig.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadGeneratorTask extends SourceTask {

  // No default schema; must be provided via config

  private final Random random = new Random(System.currentTimeMillis());
  private final Logger log = LoggerFactory.getLogger(LoadGeneratorTask.class);

  private LoadGeneratorConnectorConfig config;
  private int messagesPerSecond;
  private int messageSizeBytes;
  private double availableTokens;
  private long lastRefillTimeMs;
  private long startTimeMs;
  private long lastLogSecond;
  private int eventsSentThisSecond;
  private long totalEventsSent;
  private int maxDurationSeconds; // 0 or negative = unlimited
  private long
      maxMessagesToSend; // 0 = unlimited; computed as messagesPerSecond * maxDurationSeconds
  private boolean stopLogged;
  private static final java.util.Map<String, String> SOURCE_PARTITION =
      java.util.Collections.singletonMap("source", "source-1");
  private String outputTopic;
  private Schema recordSchema;
  private List<FieldSpec> fieldSpecs;
  private boolean runIndefinitely;

  @Override
  public String version() {
    return "1.0";
  }

  @Override
  public void start(Map<String, String> properties) {
    config = new LoadGeneratorConnectorConfig(properties);
    messagesPerSecond = config.getInt(TASK_MESSAGES_PER_SECOND_CONFIG);
    if (messagesPerSecond <= 0) messagesPerSecond = 1;
    messageSizeBytes = config.getInt(MESSAGE_SIZE_BYTES_CONFIG);
    if (messageSizeBytes <= 0) messageSizeBytes = 1;
    availableTokens = 0.0;
    startTimeMs = System.currentTimeMillis();
    lastRefillTimeMs = startTimeMs;
    lastLogSecond = startTimeMs / 1000L;
    eventsSentThisSecond = 0;
    totalEventsSent = 0L;
    runIndefinitely = config.getBoolean(TASK_RUN_INDEFINITELY_CONFIG);
    maxDurationSeconds = config.getInt(TASK_MAX_DURATION_SECONDS_CONFIG);
    if (!runIndefinitely && maxDurationSeconds > 0 && messagesPerSecond > 0) {
      maxMessagesToSend = (long) messagesPerSecond * (long) maxDurationSeconds;
    } else {
      maxMessagesToSend = 0L; // unlimited
    }
    stopLogged = false;
    outputTopic = config.getString(OUTPUT_TOPIC_CONFIG);
    String schemaJson = config.getString(LoadGeneratorConnectorConfig.RECORD_SCHEMA_JSON_CONFIG);
    if (schemaJson == null || schemaJson.trim().isEmpty()) {
      throw new ConnectException("Missing required config 'record.schema.json'");
    }
    try {
      parseSchemaJson(schemaJson.trim());
      if (fieldSpecs == null || fieldSpecs.isEmpty()) {
        throw new ConnectException("record.schema.json must contain at least one field");
      }
      recordSchema = buildConnectSchema(fieldSpecs);
      log.info("Using dynamic schema with {} fields", fieldSpecs.size());
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception e) {
      throw new ConnectException("Failed to parse 'record.schema.json': " + e.getMessage(), e);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long nowMs = System.currentTimeMillis();
    if (maxMessagesToSend > 0 && totalEventsSent >= maxMessagesToSend) {
      if (!stopLogged) {
        long elapsedTotalSeconds = (nowMs - startTimeMs) / 1000L;
        log.info(
            "Load generator finished: elapsed={}s, target_messages={}, total_sent={}, stopped",
            elapsedTotalSeconds,
            maxMessagesToSend,
            totalEventsSent);
        stopLogged = true;
      }
      Thread.sleep(250);
      return Collections.emptyList();
    }

    double elapsedSeconds = (nowMs - lastRefillTimeMs) / 1000.0;
    if (elapsedSeconds > 0) {
      availableTokens =
          Math.min(messagesPerSecond, availableTokens + elapsedSeconds * messagesPerSecond);
      lastRefillTimeMs = nowMs;
    }

    int toSend = (int) Math.floor(availableTokens);
    if (maxMessagesToSend > 0) {
      long remaining = maxMessagesToSend - totalEventsSent;
      if (remaining <= 0) {
        toSend = 0;
      } else if (toSend > remaining) {
        toSend = (int) Math.min(remaining, Integer.MAX_VALUE);
      }
    }
    if (toSend <= 0) {
      Thread.sleep(Math.max(1, 1000 / Math.max(1, messagesPerSecond)));
      return Collections.emptyList();
    }

    List<SourceRecord> records = new ArrayList<>();
    for (int i = 0; i < toSend; i++) {
      long nextOffset = totalEventsSent + 1;
      records.add(
          new SourceRecord(
              SOURCE_PARTITION,
              java.util.Collections.singletonMap("offset", nextOffset),
              outputTopic,
              null,
              null,
              null,
              recordSchema,
              createStruct(recordSchema)));
      eventsSentThisSecond++;
      totalEventsSent++;
    }
    availableTokens -= toSend;

    long currentSecond = nowMs / 1000L;
    if (currentSecond > lastLogSecond) {
      long totalElapsed = (nowMs - startTimeMs) / 1000L;
      log.info(
          "Load generator stats: elapsed={}s, sent_last_second={}, total_sent={}, mps_config={} message_size_bytes={}",
          totalElapsed,
          eventsSentThisSecond,
          totalEventsSent,
          messagesPerSecond,
          messageSizeBytes);
      eventsSentThisSecond = 0;
      lastLogSecond = currentSecond;
    }
    return records;
  }

  private Struct createStruct(Schema schema) {
    Struct struct = new Struct(schema);
    if (fieldSpecs == null || fieldSpecs.isEmpty()) {
      throw new ConnectException("Internal error: fieldSpecs not initialized");
    }
    for (FieldSpec spec : fieldSpecs) {
      switch (spec.type) {
        case STRING:
          struct.put(spec.name, sizedString(messageSizeBytes));
          break;
        case INT:
          struct.put(spec.name, toIntSafely(totalEventsSent + 1));
          break;
        case BOOLEAN:
          struct.put(spec.name, random.nextBoolean());
          break;
        default:
          throw new ConnectException("Unsupported field type: " + spec.type);
      }
    }
    return struct;
  }

  private String sizedString(int size) {
    if (size <= 0) return "";
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 'A');
    return new String(bytes, StandardCharsets.US_ASCII);
  }

  @Override
  public void stop() {}

  private int toIntSafely(long value) {
    if (value > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    if (value < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }
    return (int) value;
  }

  private void parseSchemaJson(String json) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json);
    List<FieldSpec> specs = new ArrayList<>();
    if (root.isArray()) {
      // Legacy format: array of {name, data_type, nullable}
      for (JsonNode node : root) {
        String name = getRequiredText(node, "name");
        String dataType = getRequiredText(node, "data_type");
        boolean nullable = node.has("nullable") && node.get("nullable").asBoolean(false);
        FieldType type = parseFieldType(dataType);
        specs.add(new FieldSpec(name, type, nullable));
      }
      this.fieldSpecs = specs;
      return;
    }

    if (!root.isObject()) {
      throw new IllegalArgumentException("record.schema.json must be either an Avro schema object or legacy array of fields");
    }

    // Avro record schema
    String typeStr = getRequiredText(root, "type");
    if (!"record".equalsIgnoreCase(typeStr)) {
      throw new IllegalArgumentException("Only Avro record schemas are supported (type=record)");
    }
    JsonNode fields = root.get("fields");
    if (fields == null || !fields.isArray()) {
      throw new IllegalArgumentException("Avro schema must contain an array 'fields'");
    }
    for (JsonNode f : fields) {
      String name = getRequiredText(f, "name");
      JsonNode typeNode = f.get("type");
      if (typeNode == null) {
        throw new IllegalArgumentException("Field '" + name + "' missing 'type'");
      }
      boolean nullable = false;
      String baseType = null;
      if (typeNode.isTextual()) {
        baseType = typeNode.asText();
      } else if (typeNode.isArray()) {
        // Union: look for null and one primitive
        for (JsonNode u : typeNode) {
          if (u.isTextual() && "null".equals(u.asText())) {
            nullable = true;
          } else if (u.isTextual()) {
            baseType = u.asText();
          } else if (u.isObject() && u.has("type") && u.get("type").isTextual()) {
            baseType = u.get("type").asText();
          }
        }
      } else if (typeNode.isObject() && typeNode.has("type")) {
        JsonNode t = typeNode.get("type");
        if (t.isTextual()) baseType = t.asText();
      }
      if (baseType == null) {
        throw new IllegalArgumentException("Unsupported Avro type for field '" + name + "': " + typeNode.toString());
      }
      FieldType fieldType = parseFieldType(baseType);
      specs.add(new FieldSpec(name, fieldType, nullable));
    }
    this.fieldSpecs = specs;
  }

  private FieldType parseFieldType(String t) {
    String v = t == null ? "" : t.trim().toLowerCase();
    switch (v) {
      case "string":
        return FieldType.STRING;
      case "int":
      case "int32":
        return FieldType.INT;
      case "boolean":
        return FieldType.BOOLEAN;
      default:
        throw new IllegalArgumentException("Unsupported data_type: " + t);
    }
  }

  private String getRequiredText(JsonNode node, String field) {
    if (!node.hasNonNull(field)) {
      throw new IllegalArgumentException("Missing field '" + field + "' in schema spec");
    }
    return node.get(field).asText();
  }

  private Schema buildConnectSchema(List<FieldSpec> specs) {
    SchemaBuilder builder = SchemaBuilder.struct();
    for (FieldSpec spec : specs) {
      Schema fieldSchema;
      Schema optionalFieldSchema;
      switch (spec.type) {
        case STRING:
          fieldSchema = Schema.STRING_SCHEMA;
          optionalFieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
          break;
        case INT:
          fieldSchema = Schema.INT32_SCHEMA;
          optionalFieldSchema = Schema.OPTIONAL_INT32_SCHEMA;
          break;
        case BOOLEAN:
          fieldSchema = Schema.BOOLEAN_SCHEMA;
          optionalFieldSchema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
          break;
        default:
          continue;
      }
      if (spec.nullable) {
        builder.field(spec.name, optionalFieldSchema);
      } else {
        builder.field(spec.name, fieldSchema).required();
      }
    }
    return builder.build();
  }

  private static final class FieldSpec {
    final String name;
    final FieldType type;
    final boolean nullable;

    FieldSpec(String name, FieldType type, boolean nullable) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
    }
  }

  private enum FieldType {
    STRING,
    INT,
    BOOLEAN
  }
}
