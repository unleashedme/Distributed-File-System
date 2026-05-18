package dfs.common;

public class Constants {
    public static final String NAMENODE_IP = ConfigManager.getString("namenode.ip", "127.0.0.1");

    public static final int NAMENODE_PORT = ConfigManager.getInt("namenode.port", 9000);

    public static final int REPLICATION_FACTOR = ConfigManager.getInt("dfs.replication.factor", 2);

    public static final long HEARTBEAT_INTERVAL_MS = ConfigManager.getInt("dfs.heartbeat.interval.ms", 5000);

    public static final long TIMEOUT_MS = ConfigManager.getInt("dfs.timeout.ms", 15000);
}
