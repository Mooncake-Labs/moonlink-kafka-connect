package moonlink.sink.connector;

import static moonlink.sink.connector.MoonlinkSinkConnectorConfig.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoonlinkSinkConnector extends SinkConnector {

  private final Logger log = LoggerFactory.getLogger(MoonlinkSinkConnector.class);

  private Map<String, String> originalProps;

  @Override
  public String version() {
    return "1.0";
  }

  @Override
  public ConfigDef config() {
    // Is called to validate the connector configuration
    return CONFIG_DEF;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MoonlinkSinkTask.class;
  }

  @Override
  public void start(Map<String, String> originalProps) {
    this.originalProps = originalProps;
    log.info("Moonlink Sink Connector started with properties: {}", originalProps);
    // No schema fetch needed; values are raw Avro bytes
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    int numTasks = Math.max(1, maxTasks);
    for (int i = 0; i < numTasks; i++) {
      Map<String, String> taskConfig = new HashMap<>(originalProps);
      taskConfig.put("task.id", Integer.toString(i));
      taskConfigs.add(taskConfig);
    }
    if (taskConfigs.isEmpty()) {
      log.warn("No tasks created because there is zero to work on");
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    log.info("Moonlink Sink Connector stopping");
  }
}
