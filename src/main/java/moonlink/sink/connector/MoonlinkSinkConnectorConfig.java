package moonlink.sink.connector;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class MoonlinkSinkConnectorConfig extends AbstractConfig {

  public MoonlinkSinkConnectorConfig(final Map<?, ?> originalProps) {
    super(CONFIG_DEF, originalProps);
  }

  public static final String MOONLINK_URI = "moonlink.uri";
  private static final String MOONLINK_URI_DOC = "This is the Moonlink URI";

  public static final String TABLE_NAME = "moonlink.table.name";
  private static final String TABLE_NAME_DOC = "This is the Moonlink table name";

  public static final String DATABASE_NAME = "moonlink.database.name";
  private static final String DATABASE_NAME_DOC = "This is the Moonlink database name";

  public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  private static final String SCHEMA_REGISTRY_URL_DOC =
      "Schema Registry base URL (e.g. http://schema-registry:8081)";

  public static final String FLUSH_RETRIES = "moonlink.flush.retries";
  private static final String FLUSH_RETRIES_DOC = "Number of retries on Moonlink flush timeout";

  public static final String FLUSH_RETRY_BACKOFF_MS = "moonlink.flush.retry.backoff.ms";
  private static final String FLUSH_RETRY_BACKOFF_MS_DOC =
      "Backoff in milliseconds between Moonlink flush retries";

  public static final ConfigDef CONFIG_DEF = createConfigDef();

  private static ConfigDef createConfigDef() {
    ConfigDef configDef = new ConfigDef();
    addParams(configDef);
    return configDef;
  }

  private static void addParams(final ConfigDef configDef) {
    configDef
        .define(MOONLINK_URI, Type.STRING, Importance.HIGH, MOONLINK_URI_DOC)
        .define(TABLE_NAME, Type.STRING, Importance.HIGH, TABLE_NAME_DOC)
        .define(DATABASE_NAME, Type.STRING, Importance.HIGH, DATABASE_NAME_DOC)
        .define(SCHEMA_REGISTRY_URL, Type.STRING, Importance.HIGH, SCHEMA_REGISTRY_URL_DOC)
        .define(FLUSH_RETRIES, Type.INT, 2, Importance.MEDIUM, FLUSH_RETRIES_DOC)
        .define(
            FLUSH_RETRY_BACKOFF_MS,
            Type.LONG,
            1000L,
            Importance.MEDIUM,
            FLUSH_RETRY_BACKOFF_MS_DOC);
  }
}
