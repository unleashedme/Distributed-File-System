package dfs.namenode;

import dfs.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** WALManager writes to a fixed file in the working directory; any existing log is backed up and restored. */
class WALManagerTest {

    private final List<WALManager> opened = new ArrayList<>();

    private WALManager newWal() {
        WALManager wal = new WALManager();
        opened.add(wal);
        return wal;
    }

    @BeforeEach
    void setUp() throws IOException {
        TestUtils.backupWal();
    }

    @AfterEach
    void tearDown() throws IOException {
        opened.forEach(TestUtils::closeWal);
        TestUtils.restoreWal();
    }

    @Test
    void replayWithoutLogFileReturnsEmptyList() throws IOException {
        WALManager wal = newWal();
        TestUtils.closeWal(wal);
        Files.delete(TestUtils.WAL_FILE);

        assertTrue(wal.replayLog().isEmpty());
    }

    @Test
    void constructorCreatesLogFile() {
        newWal();
        assertTrue(Files.exists(TestUtils.WAL_FILE));
    }

    @Test
    void loggedTransactionsAreReplayedInOrder() {
        WALManager wal = newWal();
        wal.logTransaction("CREATE_FILE|a.txt|a_chunk0.bin:h0");
        wal.logTransaction("TOMBSTONE_FILE|a.txt");

        assertEquals(List.of("CREATE_FILE|a.txt|a_chunk0.bin:h0", "TOMBSTONE_FILE|a.txt"), wal.replayLog());
    }

    @Test
    void newInstanceAppendsToExistingLog() {
        WALManager first = newWal();
        first.logTransaction("tx-1");
        TestUtils.closeWal(first);

        WALManager second = newWal();
        second.logTransaction("tx-2");

        assertEquals(List.of("tx-1", "tx-2"), second.replayLog());
    }
}
