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

    public static final String FIRST_REQUIRED_PARAM_CONFIG = "first.required.param";
    private static final String FIRST_REQUIRED_PARAM_DOC = "This is the 1st required parameter";

    public static final String SECOND_REQUIRED_PARAM_CONFIG = "second.required.param";
    private static final String SECOND_REQUIRED_PARAM_DOC = "This is the 2nd required parameter";

    public static final String FIRST_NONREQUIRED_PARAM_CONFIG = "first.nonrequired.param";
    private static final String FIRST_NONREQUIRED_PARAM_DOC = "This is the 1st non-required parameter";
    private static final String FIRST_NONREQUIRED_PARAM_DEFAULT = "foo";

    public static final String SECOND_NONREQUIRED_PARAM_CONFIG = "second.nonrequired.param";
    private static final String SECOND_NONREQUIRED_PARAM_DOC = "This is the 2nd non-required parameter";
    private static final String SECOND_NONREQUIRED_PARAM_DEFAULT = "bar";

    public static final String TASK_MESSAGES_PER_SECOND_CONFIG = "task.messages.per.second";
    private static final String TASK_MESSAGES_PER_SECOND_DOC = "Number of messages the task emits per second";
    private static final int TASK_MESSAGES_PER_SECOND_DEFAULT = 1;

    public static final String MESSAGE_SIZE_BYTES_CONFIG = "message.size.bytes";
    private static final String MESSAGE_SIZE_BYTES_DOC = "Approximate size of generated message payload in bytes";
    private static final int MESSAGE_SIZE_BYTES_DEFAULT = 100;

    // Maximum duration for which a task should emit events (seconds). 0 or negative = unlimited
    public static final String TASK_MAX_DURATION_SECONDS_CONFIG = "task.max.duration.seconds";
    private static final String TASK_MAX_DURATION_SECONDS_DOC = "Maximum duration in seconds a task should emit events; 0 or negative means unlimited";
    private static final int TASK_MAX_DURATION_SECONDS_DEFAULT = 5;

    public static final String MONITOR_THREAD_TIMEOUT_CONFIG = "monitor.thread.timeout";
    private static final String MONITOR_THREAD_TIMEOUT_DOC = "Timeout used by the monitoring thread";
    private static final int MONITOR_THREAD_TIMEOUT_DEFAULT = 10000;

    // Output Kafka topic for this source connector (single topic)
    public static final String OUTPUT_TOPIC_CONFIG = "output.topic";
    private static final String OUTPUT_TOPIC_DOC = "Kafka topic to write produced records to";
    private static final String OUTPUT_TOPIC_DEFAULT = "source-1";

    public static final ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        addParams(configDef);
        return configDef;
    }

    private static void addParams(final ConfigDef configDef) {
        configDef.define(
            FIRST_REQUIRED_PARAM_CONFIG,
            Type.STRING,
            Importance.HIGH,
            FIRST_REQUIRED_PARAM_DOC)
        .define(
            SECOND_REQUIRED_PARAM_CONFIG,
            Type.STRING,
            Importance.HIGH,
            SECOND_REQUIRED_PARAM_DOC)
        .define(
            FIRST_NONREQUIRED_PARAM_CONFIG,
            Type.STRING,
            FIRST_NONREQUIRED_PARAM_DEFAULT,
            Importance.HIGH,
            FIRST_NONREQUIRED_PARAM_DOC)
        .define(
            SECOND_NONREQUIRED_PARAM_CONFIG,
            Type.STRING,
            SECOND_NONREQUIRED_PARAM_DEFAULT,
            Importance.HIGH,
            SECOND_NONREQUIRED_PARAM_DOC)
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
            OUTPUT_TOPIC_DOC);
    }
}





