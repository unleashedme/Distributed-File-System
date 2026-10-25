package dfs.namenode;

import dfs.TestUtils;
import dfs.TestUtils.FakeNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import static dfs.TestUtils.activeNodes;
import static dfs.TestUtils.registerNode;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises NameNode's request handler over a real loopback socket, plus its recovery
 * and self-healing routines, without starting the full server on its configured port.
 */
class NameNodeTest {

    private WALManager wal;
    private final List<Closeable> resources = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        TestUtils.backupWal();
        wal = TestUtils.resetNameNode();
    }

    @AfterEach
    void tearDown() throws IOException {
        for (Closeable c : resources) c.close();
        TestUtils.closeWal(wal);
        TestUtils.restoreWal();
    }

    /** A client connection whose server side is handled by NameNode.handleDataNode. */
    private class Connection implements Closeable {
        final Socket socket;
        final PrintWriter out;
        final BufferedReader in;

        Connection() throws IOException {
            try (ServerSocket server = new ServerSocket(0)) {
                socket = new Socket("127.0.0.1", server.getLocalPort());
                TestUtils.handleOnNameNode(server.accept());
            }
            socket.setSoTimeout(5000);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            resources.add(this);
        }

        void send(String message) {
            out.println(message);
        }

        String request(String message) throws IOException {
            send(message);
            return in.readLine();
        }

        /** Requests are handled in order, so a reply means every earlier message was processed. */
        void sync() throws IOException {
            request("GET_FILE_INFO|__sync__");
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private FakeNode fakeNode() throws IOException {
        FakeNode node = new FakeNode();
        resources.add(node);
        return node;
    }

    // ---- Heartbeats and pipeline assignment ----

    @Test
    void heartbeatRegistersNodeWithSenderAddressAndDataPort() throws IOException {
        Connection conn = new Connection();
        conn.send("Heartbeat node-1 5555");
        conn.sync();

        NameNode.NodeInfo info = activeNodes().get("node-1");
        assertNotNull(info);
        assertEquals("127.0.0.1", info.ipAddress);
        assertEquals(5555, info.dataPort);
    }

    @Test
    void malformedHeartbeatIsIgnored() throws IOException {
        Connection conn = new Connection();
        conn.send("Heartbeat node-1");
        conn.sync();

        assertTrue(activeNodes().isEmpty());
    }

    @Test
    void uploadRequestWithoutActiveNodesReturnsError() throws IOException {
        assertTrue(new Connection().request("Upload_Request 3").startsWith("Error"));
    }

    @Test
    void uploadRequestAssignsPipelineOfAtMostTwoNodes() throws IOException {
        registerNode("a", 7001);
        registerNode("b", 7002);
        registerNode("c", 7003);

        String[] pipeline = new Connection().request("Upload_Request 1").split(",");

        assertEquals(2, pipeline.length);
        Set<String> known = Set.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003");
        assertTrue(known.contains(pipeline[0]));
        assertTrue(known.contains(pipeline[1]));
        assertNotEquals(pipeline[0], pipeline[1]);
    }

    @Test
    void uploadRequestWithSingleNodeReturnsThatNode() throws IOException {
        registerNode("a", 7001);
        assertEquals("127.0.0.1:7001", new Connection().request("Upload_Request 1"));
    }

    // ---- Block reports and chunk lookup ----

    @Test
    void blockReportMakesChunkLocatable() throws IOException {
        registerNode("node-1", 7001);
        Connection conn = new Connection();

        conn.send("BLOCK_REPORT node-1 c0");
        assertEquals("127.0.0.1:7001", conn.request("LOCATE_CHUNK c0"));
        assertEquals(Set.of("node-1"), NameNode.chunkLocations.get("c0"));
    }

    @Test
    void locatingUnknownChunkReturnsError() throws IOException {
        assertEquals("ERROR: Chunk not found", new Connection().request("LOCATE_CHUNK nope"));
    }

    @Test
    void locatingChunkWhoseHostsAreAllDeadReturnsError() throws IOException {
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("gone")));

        String response = new Connection().request("LOCATE_CHUNK c0");

        assertTrue(response.startsWith("ERROR") && response.contains("dead"), response);
    }

    @Test
    void locateSkipsDeadHostsAndPicksALiveOne() throws IOException {
        registerNode("alive", 7002);
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("gone", "alive")));

        assertEquals("127.0.0.1:7002", new Connection().request("LOCATE_CHUNK c0"));
    }

    // ---- File namespace ----

    @Test
    void createFileRegistersChunksHashesAndWritesWal() throws IOException {
        String tx = "CREATE_FILE|a.txt|a_chunk0.bin:h0,a_chunk1.bin:h1";

        assertEquals("SUCCESS", new Connection().request(tx));

        assertEquals(List.of("a_chunk0.bin:h0", "a_chunk1.bin:h1"), NameNode.fileTree.get("a.txt"));
        assertEquals("h0", NameNode.expectedHashes.get("a_chunk0.bin"));
        assertEquals("h1", NameNode.expectedHashes.get("a_chunk1.bin"));
        assertEquals(List.of(tx), wal.replayLog());
    }

    @Test
    void getFileInfoReturnsRegisteredChunks() throws IOException {
        NameNode.fileTree.put("a.txt", List.of("a_chunk0.bin:h0", "a_chunk1.bin:h1"));

        assertEquals("a_chunk0.bin:h0,a_chunk1.bin:h1", new Connection().request("GET_FILE_INFO|a.txt"));
    }

    @Test
    void getFileInfoForUnknownFileReturnsError() throws IOException {
        assertTrue(new Connection().request("GET_FILE_INFO|missing.txt").startsWith("ERROR"));
    }

    @Test
    void getFileInfoForTombstonedFileReportsCatastrophicFailure() throws IOException {
        NameNode.corruptedFiles.add("lost.txt");

        String response = new Connection().request("GET_FILE_INFO|lost.txt");

        assertTrue(response.contains("CATASTROPHIC_FAILURE"), response);
    }

    @Test
    void createdFileCanBeQueriedOnSameConnection() throws IOException {
        Connection conn = new Connection();
        assertEquals("SUCCESS", conn.request("CREATE_FILE|b.txt|b_chunk0.bin:hb"));
        assertEquals("b_chunk0.bin:hb", conn.request("GET_FILE_INFO|b.txt"));
    }

    // ---- WAL recovery ----

    @Test
    void recoverStateRebuildsNamespaceFromWal() {
        wal.logTransaction("CREATE_FILE|keep.txt|k0:hk0,k1:hk1");
        wal.logTransaction("CREATE_FILE|lost.txt|l0:hl0");
        wal.logTransaction("TOMBSTONE_FILE|lost.txt");

        TestUtils.invokeStatic(NameNode.class, "recoverState", new Class<?>[0]);

        assertEquals(List.of("k0:hk0", "k1:hk1"), NameNode.fileTree.get("keep.txt"));
        assertFalse(NameNode.fileTree.containsKey("lost.txt"));
        assertTrue(NameNode.corruptedFiles.contains("lost.txt"));
        assertEquals("hk0", NameNode.expectedHashes.get("k0"));
        assertEquals("hl0", NameNode.expectedHashes.get("l0"));
    }

    // ---- Bit-rot scrubbing ----

    @Test
    void scrubWithMatchingHashTakesNoAction() throws Exception {
        FakeNode node = fakeNode();
        registerNode("n1", node.port());
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("n1")));
        NameNode.expectedHashes.put("c0", "h0");

        Connection conn = new Connection();
        conn.send("BLOCK_SCRUB|n1|c0|h0");
        conn.sync();

        assertNull(node.nextMessage(200));
        assertEquals(Set.of("n1"), NameNode.chunkLocations.get("c0"));
    }

    @Test
    void scrubForChunkWithoutKnownHashIsIgnoredAndConnectionSurvives() throws Exception {
        FakeNode node = fakeNode();
        registerNode("n1", node.port());
        NameNode.chunkLocations.put("orphan", new HashSet<>(Set.of("n1")));

        Connection conn = new Connection();
        conn.send("BLOCK_SCRUB|n1|orphan|h0");

        assertEquals("SUCCESS", conn.request("CREATE_FILE|after.txt|after_chunk0.bin:h1"));
        assertEquals(Set.of("n1"), NameNode.chunkLocations.get("orphan"));
        assertNull(node.nextMessage(200));
    }

    @Test
    void scrubMismatchPurgesBadReplicaAndRequestsHealing() throws Exception {
        FakeNode bad = fakeNode();
        FakeNode good = fakeNode();
        registerNode("bad", bad.port());
        registerNode("good", good.port());
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("bad", "good")));
        NameNode.expectedHashes.put("c0", "h0");

        Connection conn = new Connection();
        conn.send("BLOCK_SCRUB|bad|c0|rotten");
        conn.sync();

        assertEquals("DELETE_CHUNK|c0", bad.nextMessage(2000));
        assertEquals("REPLICATE_CHUNK|c0|127.0.0.1:" + bad.port(), good.nextMessage(2000));
        assertEquals(Set.of("bad", "good"), NameNode.chunkLocations.get("c0"));
    }

    @Test
    void scrubMismatchWithoutHealthyReplicaTombstonesFile() throws Exception {
        FakeNode bad = fakeNode();
        registerNode("bad", bad.port());
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("bad")));
        NameNode.expectedHashes.put("c0", "h0");
        NameNode.fileTree.put("f.txt", List.of("c0:h0"));

        Connection conn = new Connection();
        conn.send("BLOCK_SCRUB|bad|c0|rotten");

        assertTrue(conn.request("GET_FILE_INFO|f.txt").contains("CATASTROPHIC_FAILURE"));
        assertFalse(NameNode.fileTree.containsKey("f.txt"));
        assertTrue(NameNode.corruptedFiles.contains("f.txt"));
        assertTrue(wal.replayLog().contains("TOMBSTONE_FILE|f.txt"));
        assertEquals("DELETE_CHUNK|c0", bad.nextMessage(2000));
    }

    // ---- Node failure recovery ----

    @Test
    void nodeFailureReplicatesUnderReplicatedChunksToSpareNode() throws Exception {
        FakeNode survivor = fakeNode();
        FakeNode spare = fakeNode();
        registerNode("survivor", survivor.port());
        registerNode("spare", spare.port());
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("dead", "survivor")));

        TestUtils.invokeStatic(NameNode.class, "handleNodeFailure", new Class<?>[]{String.class}, "dead");

        assertEquals(Set.of("survivor"), NameNode.chunkLocations.get("c0"));
        assertEquals("REPLICATE_CHUNK|c0|127.0.0.1:" + spare.port(), survivor.nextMessage(2000));
    }

    @Test
    void nodeFailureLeavesUnaffectedChunksAlone() throws Exception {
        FakeNode a = fakeNode();
        FakeNode b = fakeNode();
        registerNode("a", a.port());
        registerNode("b", b.port());
        NameNode.chunkLocations.put("c0", new HashSet<>(Set.of("a", "b")));

        TestUtils.invokeStatic(NameNode.class, "handleNodeFailure", new Class<?>[]{String.class}, "dead");

        assertEquals(Set.of("a", "b"), NameNode.chunkLocations.get("c0"));
        assertNull(a.nextMessage(200));
        assertNull(b.nextMessage(0));
    }
}
