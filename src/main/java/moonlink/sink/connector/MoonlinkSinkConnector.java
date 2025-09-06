package moonlink.sink.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import static moonlink.sink.connector.MoonlinkSinkConnectorConfig.*;

import moonlink.client.MoonlinkClient;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MoonlinkSinkConnector extends SinkConnector {

    private final Logger log = LoggerFactory.getLogger(MoonlinkSinkConnector.class);

    private Map<String, String> originalProps;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
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

        try {
            var cfg = new MoonlinkSinkConnectorConfig(originalProps);
            String baseUrl = cfg.getString(MOONLINK_URI);
            String database = cfg.getString(DATABASE_NAME);
            String table = cfg.getString(TABLE_NAME);
            var client = new MoonlinkClient(baseUrl);

            // Fetch Arrow schema via IPC (primary) and also fetch JSON to pass via props
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = client.fetchArrowSchemaIpc(database, table);
            String arrowSchemaJson = new ObjectMapper().writeValueAsString(arrowSchema);
            this.originalProps = new HashMap<>(this.originalProps);
            this.originalProps.put("arrow.schema.json", arrowSchemaJson);
            log.info("Fetched Arrow schema JSON and stored for tasks");
        } catch (Exception e) {
            throw new ConnectException("Failed to fetch Moonlink table schema. Ensure the table exists in Moonlink before starting the sink connector.", e);
        }
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