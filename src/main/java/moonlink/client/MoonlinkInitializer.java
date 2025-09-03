package moonlink.client;

import java.util.List;
import java.util.Map;

public class MoonlinkInitializer {
    private final MoonlinkClient client;

    public MoonlinkInitializer(MoonlinkClient client) {
        this.client = client;
    }

    public void ensureTableExists(String database, String table,
                                  List<Dto.FieldSchema> schema,
                                  Map<String, Object> tableConfig) throws Exception {
        if (client.tableExists(database, table)) return;

        var req = new Dto.CreateTableRequest();
        req.database = database;
        req.table = table;
        req.schema = schema;
        req.tableConfig = tableConfig;

        client.createTable(req);
    }
}
