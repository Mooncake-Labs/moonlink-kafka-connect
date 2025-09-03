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
import moonlink.client.MoonlinkInitializer;
import moonlink.client.Dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

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
            String schemaJson = cfg.getString(TABLE_SCHEMA_JSON);

            // Parse schema JSON -> List<FieldSchema>
            // TODO: Eventually we should get the schema from moonlink backend instead of passing it in here.
            List<Dto.FieldSchema> schema = new ObjectMapper().readValue(
                schemaJson,
                new TypeReference<List<Dto.FieldSchema>>() {}
            );

            // Table config: specify required mooncake settings
            Map<String, Object> tableConfig = new HashMap<>();
            Map<String, Object> mooncakeConfig = new HashMap<>();
            mooncakeConfig.put("append_only", true);
            mooncakeConfig.put("row_identity", "None");
            mooncakeConfig.put("skip_index_merge", false);
            mooncakeConfig.put("skip_data_compaction", false);
            tableConfig.put("mooncake", mooncakeConfig);

            var client = new MoonlinkClient(baseUrl);
            var init = new MoonlinkInitializer(client);
            // TODO: Eventually we should get the schema from moonlink backend instead of just checking for existence.
            init.ensureTableExists(database, table, schema, tableConfig);
            log.info("Ensured Moonlink table exists: {}.{}", database, table);
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize Moonlink table", e);
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