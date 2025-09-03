package example.loadgen;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class LoadGeneratorConnectorConfig extends AbstractConfig {

  public LoadGeneratorConnectorConfig(final Map<?, ?> originalProps) {
    super(CONFIG_DEF, originalProps);
  }

  public static final String TASK_MESSAGES_PER_SECOND_CONFIG = "task.messages.per.second";
  private static final String TASK_MESSAGES_PER_SECOND_DOC =
      "Number of messages the task emits per second";
  private static final int TASK_MESSAGES_PER_SECOND_DEFAULT = 1;

  public static final String MESSAGE_SIZE_BYTES_CONFIG = "message.size.bytes";
  private static final String MESSAGE_SIZE_BYTES_DOC =
      "Approximate size of generated message payload in bytes";
  private static final int MESSAGE_SIZE_BYTES_DEFAULT = 100;

  // If true, ignore any max duration/samples and keep generating until the task is stopped
  public static final String TASK_RUN_INDEFINITELY_CONFIG = "task.run.indefinitely";
  private static final String TASK_RUN_INDEFINITELY_DOC =
      "If true, ignore max duration/samples and produce indefinitely until stopped";
  private static final boolean TASK_RUN_INDEFINITELY_DEFAULT = false;

  // Maximum duration for which a task should emit events (seconds). 0 or negative = unlimited
  public static final String TASK_MAX_DURATION_SECONDS_CONFIG = "task.max.duration.seconds";
  private static final String TASK_MAX_DURATION_SECONDS_DOC =
      "Maximum duration in seconds a task should emit events; 0 or negative means unlimited";
  private static final int TASK_MAX_DURATION_SECONDS_DEFAULT = 5;

  public static final String MONITOR_THREAD_TIMEOUT_CONFIG = "monitor.thread.timeout";
  private static final String MONITOR_THREAD_TIMEOUT_DOC = "Timeout used by the monitoring thread";
  private static final int MONITOR_THREAD_TIMEOUT_DEFAULT = 10000;

  // Output Kafka topic for this source connector (single topic)
  public static final String OUTPUT_TOPIC_CONFIG = "output.topic";
  private static final String OUTPUT_TOPIC_DOC = "Kafka topic to write produced records to";
  private static final String OUTPUT_TOPIC_DEFAULT = "source-1";

  public static final String RECORD_SCHEMA_JSON_CONFIG = "record.schema.json";
  private static final String RECORD_SCHEMA_JSON_DOC =
      "JSON array of field specs with name, data_type (string|int|boolean), and nullable";
  private static final String RECORD_SCHEMA_JSON_DEFAULT = "";

  public static final ConfigDef CONFIG_DEF = createConfigDef();

  private static ConfigDef createConfigDef() {
    ConfigDef configDef = new ConfigDef();
    addParams(configDef);
    return configDef;
  }

  private static void addParams(final ConfigDef configDef) {
    configDef
        .define(
            TASK_MESSAGES_PER_SECOND_CONFIG,
            Type.INT,
            TASK_MESSAGES_PER_SECOND_DEFAULT,
            Importance.HIGH,
            TASK_MESSAGES_PER_SECOND_DOC)
        .define(
            MESSAGE_SIZE_BYTES_CONFIG,
            Type.INT,
            MESSAGE_SIZE_BYTES_DEFAULT,
            Importance.HIGH,
            MESSAGE_SIZE_BYTES_DOC)
        .define(
            TASK_RUN_INDEFINITELY_CONFIG,
            Type.BOOLEAN,
            TASK_RUN_INDEFINITELY_DEFAULT,
            Importance.MEDIUM,
            TASK_RUN_INDEFINITELY_DOC)
        .define(
            TASK_MAX_DURATION_SECONDS_CONFIG,
            Type.INT,
            TASK_MAX_DURATION_SECONDS_DEFAULT,
            Importance.MEDIUM,
            TASK_MAX_DURATION_SECONDS_DOC)
        .define(
            MONITOR_THREAD_TIMEOUT_CONFIG,
            Type.INT,
            MONITOR_THREAD_TIMEOUT_DEFAULT,
            Importance.LOW,
            MONITOR_THREAD_TIMEOUT_DOC)
        .define(
            OUTPUT_TOPIC_CONFIG,
            Type.STRING,
            OUTPUT_TOPIC_DEFAULT,
            Importance.HIGH,
            OUTPUT_TOPIC_DOC)
        .define(
            RECORD_SCHEMA_JSON_CONFIG,
            Type.STRING,
            RECORD_SCHEMA_JSON_DEFAULT,
            Importance.HIGH,
            RECORD_SCHEMA_JSON_DOC);
  }
}
