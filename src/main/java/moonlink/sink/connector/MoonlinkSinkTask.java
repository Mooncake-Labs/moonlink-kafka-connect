package moonlink.sink.connector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import moonlink.client.MoonlinkClient;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoonlinkSinkTask extends SinkTask {

  private final Logger log = LoggerFactory.getLogger(MoonlinkSinkTask.class);

  private MoonlinkSinkConnectorConfig config;
  private MoonlinkClient client;
  private long latestLsn;
  private String taskId;
  private long totalProcessed;

  private String databaseName;
  private String tableName;

  // Per-partition index of Kafka offset -> Moonlink LSN (ordered by offset)
  private final Map<TopicPartition, NavigableMap<Long, Long>> partitionOffsetToLsn =
      new HashMap<>();

  

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
      latestLsn = 0L;
      totalProcessed = 0L;
      // Try to capture the Kafka Connect task id if present
      this.taskId = props.get("task.id");
      this.databaseName = config.getString(MoonlinkSinkConnectorConfig.DATABASE_NAME);
      this.tableName = config.getString(MoonlinkSinkConnectorConfig.TABLE_NAME);
    } catch (Exception e) {
      throw new ConnectException("Failed to initialize Moonlink client", e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("Moonlink Sink Task received {} records", records.size());
    long batchStartNs = System.nanoTime();
    int processedThisBatch = 0;
    long batchMaxLsn = latestLsn;
    for (SinkRecord record : records) {
      log.debug(
          "Processing record: topic={}, partition={}, offset={}",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());
      try {
        String srcTableName = databaseName + "." + tableName;
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
          throw new DataException(
              "Invalid Confluent Avro payload: missing magic byte or too short");
        }
        // Extract and ignore schema id (schema assumed pre-registered and stable)
        /* int schemaId = */ ByteBuffer.wrap(bytes, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt();

        // Strip Confluent wire format header (magic byte + schema id)
        byte[] avroDatum = Arrays.copyOfRange(bytes, 5, bytes.length);

        var resp = client.insertRowAvroRaw(srcTableName, avroDatum);
        Long respLsn = resp.lsn;
        if (respLsn == null) {
          throw new DataException("Insert row Avro raw response missing lsn");
        }
        if (respLsn > batchMaxLsn) batchMaxLsn = respLsn;
        if (respLsn > latestLsn) latestLsn = respLsn;

        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        NavigableMap<Long, Long> index =
            partitionOffsetToLsn.computeIfAbsent(tp, k -> new TreeMap<>());
        index.put(record.kafkaOffset(), respLsn);

        log.debug("Inserted row, lsn={}", respLsn);
        processedThisBatch++;
      } catch (Exception e) {
        throw new DataException("Failed to ingest record", e);
      }
    }
    long batchElapsedNs = System.nanoTime() - batchStartNs;
    double batchElapsedMs = batchElapsedNs / 1_000_000.0;
    double batchThroughputRps =
        processedThisBatch / Math.max(0.001, (batchElapsedNs / 1_000_000_000.0));
    totalProcessed += processedThisBatch;
    log.info(
        "Sink task batch: task_id={} processed={} total_completed={} elapsed_ms={} rps={} last_lsn={}",
        taskId,
        processedThisBatch,
        totalProcessed,
        String.format("%.3f", batchElapsedMs),
        String.format("%.2f", batchThroughputRps),
        batchMaxLsn);
  }

  @Override
  public void flush(
      Map<
              org.apache.kafka.common.TopicPartition,
              org.apache.kafka.clients.consumer.OffsetAndMetadata>
          offsets) {
    long flushStartNs = System.nanoTime();
    log.info(
        "Sink task flush start: task_id={} partitions={} offsets={}",
        taskId,
        offsets.size(),
        offsets);

    long globalMaxLsn = -1L;
    // Compute the maximum LSN across all partitions up to the provided offsets
    for (Map.Entry<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
      TopicPartition tp = new TopicPartition(e.getKey().topic(), e.getKey().partition());
      NavigableMap<Long, Long> index = partitionOffsetToLsn.get(tp);
      if (index == null || index.isEmpty()) continue;
      long upToOffset = e.getValue().offset();
      NavigableMap<Long, Long> head = index.headMap(upToOffset, true);
      if (head.isEmpty()) continue;
      long maxLsnForPartition = -1L;
      for (Long lsn : head.values()) {
        if (lsn != null && lsn > maxLsnForPartition) maxLsnForPartition = lsn;
      }
      if (maxLsnForPartition > globalMaxLsn) globalMaxLsn = maxLsnForPartition;
      if (log.isDebugEnabled()) {
        log.debug(
            "Flush calc: tp={}-{} up_to_offset={} entries_considered={} max_lsn_for_tp={}",
            tp.topic(),
            tp.partition(),
            upToOffset,
            head.size(),
            maxLsnForPartition);
      }
    }

    if (globalMaxLsn < 0) {
      log.info(
          "Sink task flush no-op: task_id={} no LSNs found for provided offsets (size={})",
          taskId,
          offsets.size());
      return; // nothing to flush
    }

    int maxRetries = config.getInt(MoonlinkSinkConnectorConfig.FLUSH_RETRIES);
    long backoffMs = config.getLong(MoonlinkSinkConnectorConfig.FLUSH_RETRY_BACKOFF_MS);
    int attempt = 0;
    while (true) {
      try {
        client.waitForFlush(databaseName, tableName, globalMaxLsn);
        break; // success
      } catch (Exception ex) {
        boolean isTimeout = isTimeoutException(ex);
        if (!isTimeout || attempt >= maxRetries) {
          throw new ConnectException(
              "Moonlink flush failed for LSN " + globalMaxLsn + " after " + attempt + " retries", ex);
        }
        attempt++;
        log.warn(
            "Moonlink flush timed out for lsn={}, attempt={}/{}; retrying after {} ms",
            globalMaxLsn,
            attempt,
            maxRetries,
            backoffMs);
        try {
          Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new ConnectException("Interrupted during flush retry backoff", ie);
        }
      }
    }

    // On success, prune entries up to the requested offsets whose LSN <= globalMaxLsn
    int pruned = 0;
    for (Map.Entry<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
      TopicPartition tp = new TopicPartition(e.getKey().topic(), e.getKey().partition());
      NavigableMap<Long, Long> index = partitionOffsetToLsn.get(tp);
      if (index == null || index.isEmpty()) continue;
      long upToOffset = e.getValue().offset();
      NavigableMap<Long, Long> head = index.headMap(upToOffset, true);
      if (head.isEmpty()) continue;
      Iterator<Map.Entry<Long, Long>> it = head.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Long, Long> entry = it.next();
        Long lsn = entry.getValue();
        if (lsn != null && lsn <= globalMaxLsn) {
          it.remove();
          pruned++;
        }
      }
      if (index.isEmpty()) {
        partitionOffsetToLsn.remove(tp);
      }
    }
    long flushElapsedNs = System.nanoTime() - flushStartNs;
    double flushElapsedMs = flushElapsedNs / 1_000_000.0;
    log.info(
        "Sink task flush done: task_id={} flushed_lsn={} pruned_entries={} elapsed_ms={} offsets={}",
        taskId,
        globalMaxLsn,
        pruned,
        String.format("%.3f", flushElapsedMs),
        offsets);
  }

  @Override
  public void stop() {
    log.info("Moonlink Sink Task stopping");
  }

  private static boolean isTimeoutException(Exception ex) {
    // Retry only on HTTP 408 or HTTP client timeouts
    if (ex instanceof java.net.http.HttpTimeoutException) return true;
    Throwable cur = ex;
    while (cur != null) {
      String msg = cur.getMessage();
      if (msg != null && msg.contains("HTTP 408")) return true;
      cur = cur.getCause();
    }
    return false;
  }

  
}
