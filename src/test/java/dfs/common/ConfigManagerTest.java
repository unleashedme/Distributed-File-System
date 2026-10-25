package dfs.common;

import dfs.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Relies on the project's dfs.properties, which Maven picks up from the working directory. */
class ConfigManagerTest {

    private static final String INJECTED_KEY = "test.injected.key";

    private final Properties properties = TestUtils.getStatic(ConfigManager.class, "properties");

    @AfterEach
    void removeInjectedKey() {
        properties.remove(INJECTED_KEY);
    }

    @Test
    void readsStringFromPropertiesFile() {
        assertEquals("127.0.0.1", ConfigManager.getString("namenode.ip", "fallback"));
    }

    @Test
    void missingStringFallsBackToDefault() {
        assertEquals("fallback", ConfigManager.getString("no.such.key", "fallback"));
    }

    @Test
    void readsIntFromPropertiesFile() {
        assertEquals(9000, ConfigManager.getInt("namenode.port", -1));
        assertEquals(2, ConfigManager.getInt("dfs.replication.factor", -1));
    }

    @Test
    void missingIntFallsBackToDefault() {
        assertEquals(42, ConfigManager.getInt("no.such.key", 42));
    }

    @Test
    void invalidIntFallsBackToDefault() {
        properties.setProperty(INJECTED_KEY, "not-a-number");
        assertEquals(7, ConfigManager.getInt(INJECTED_KEY, 7));
    }

    @Test
    void constantsAreBackedByConfiguration() {
        assertEquals(ConfigManager.getString("namenode.ip", "127.0.0.1"), Constants.NAMENODE_IP);
        assertEquals(ConfigManager.getInt("namenode.port", 9000), Constants.NAMENODE_PORT);
        assertEquals(ConfigManager.getInt("dfs.replication.factor", 2), Constants.REPLICATION_FACTOR);
        assertEquals(ConfigManager.getInt("dfs.heartbeat.interval.ms", 5000), Constants.HEARTBEAT_INTERVAL_MS);
        assertEquals(ConfigManager.getInt("dfs.timeout.ms", 15000), Constants.TIMEOUT_MS);
    }
}
