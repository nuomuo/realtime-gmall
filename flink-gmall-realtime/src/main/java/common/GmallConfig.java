package common;

public class GmallConfig {

    public static final String HBASE_SCHEMA = "GMALL2020_REALTIME";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static final String PHOENIX_SERVER = "jdbc:phoenix:node1,node2,node3:2181";

    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://node1:8123/default";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
