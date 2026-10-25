package dfs.datanode;

import dfs.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Drives DataNode.acceptChunks over loopback sockets. Two nodes are started once for the class. */
class DataNodeTest {

    private static final String ID_A = "test-A-" + UUID.randomUUID();
    private static final String ID_B = "test-B-" + UUID.randomUUID();
    private static ServerSocket serverA;
    private static ServerSocket serverB;
    private static int portA;
    private static int portB;

    @BeforeAll
    static void startNodes() throws IOException {
        serverA = new ServerSocket(0);
        serverB = new ServerSocket(0);
        startNode(serverA, ID_A);
        startNode(serverB, ID_B);
        portA = serverA.getLocalPort();
        portB = serverB.getLocalPort();
    }

    @AfterAll
    static void stopNodes() throws IOException {
        serverA.close();
        serverB.close();
        TestUtils.deleteRecursively(storage(ID_A));
        TestUtils.deleteRecursively(storage(ID_B));
    }

    static Thread startNode(ServerSocket server, String nodeId) {
        Thread t = new Thread(() -> DataNode.acceptChunks(server, nodeId));
        t.setDaemon(true);
        t.start();
        TestUtils.waitUntil(() -> Files.isDirectory(storage(nodeId)), "storage dir for " + nodeId);
        return t;
    }

    static Path storage(String nodeId) {
        return Path.of("datanode_storage_" + nodeId);
    }

    static byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        new Random(size).nextBytes(data);
        return data;
    }

    static String uniqueChunk() {
        return "chunk-" + UUID.randomUUID() + ".bin";
    }

    /** Sends a header line plus a payload, half-closes, and returns the node's reply line. */
    static String sendChunk(int port, String header, byte[] payload) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            out.write((header + "\n").getBytes(StandardCharsets.UTF_8));
            out.write(payload);
            out.flush();
            socket.shutdownOutput();
            return new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
        }
    }

    static byte[] sendCommandAndReadAll(int port, String command) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write((command + "\n").getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            return socket.getInputStream().readAllBytes();
        }
    }

    @Test
    void storesChunkWhenPipelineEndsHere() throws IOException {
        String chunk = uniqueChunk();
        byte[] data = randomBytes(150_000);

        assertEquals("SUCCESS", sendChunk(portA, chunk + "|", data));
        assertArrayEquals(data, Files.readAllBytes(storage(ID_A).resolve(chunk)));
    }

    @Test
    void payloadContainingNewlinesIsStoredVerbatim() throws IOException {
        String chunk = uniqueChunk();
        byte[] data = "line1\nline2\n\nline4".getBytes(StandardCharsets.UTF_8);

        assertEquals("SUCCESS", sendChunk(portA, chunk + "|", data));
        assertArrayEquals(data, Files.readAllBytes(storage(ID_A).resolve(chunk)));
    }

    @Test
    void forwardsChunkToNextNodeInPipeline() throws IOException {
        String chunk = uniqueChunk();
        byte[] data = randomBytes(300_000);

        assertEquals("SUCCESS", sendChunk(portA, chunk + "|127.0.0.1:" + portB, data));

        assertArrayEquals(data, Files.readAllBytes(storage(ID_A).resolve(chunk)));
        assertArrayEquals(data, Files.readAllBytes(storage(ID_B).resolve(chunk)));
    }

    @Test
    void unreachableDownstreamNodeFailsUploadAndDiscardsLocalCopy() throws IOException {
        String chunk = uniqueChunk();
        int deadPort = TestUtils.freePort();

        String ack = sendChunk(portA, chunk + "|127.0.0.1:" + deadPort, randomBytes(1000));

        assertTrue(ack.startsWith("ERROR"), ack);
        assertFalse(Files.exists(storage(ID_A).resolve(chunk)));
    }

    @Test
    void downloadServesStoredChunk() throws IOException {
        String chunk = uniqueChunk();
        byte[] data = randomBytes(200_000);
        Files.write(storage(ID_A).resolve(chunk), data);

        assertArrayEquals(data, sendCommandAndReadAll(portA, "DOWNLOAD_CHUNK|" + chunk));
    }

    @Test
    void downloadOfMissingChunkReturnsNoData() throws IOException {
        assertEquals(0, sendCommandAndReadAll(portA, "DOWNLOAD_CHUNK|" + uniqueChunk()).length);
    }

    @Test
    void deleteCommandRemovesChunk() throws IOException {
        String chunk = uniqueChunk();
        Path file = Files.write(storage(ID_A).resolve(chunk), randomBytes(10));

        sendCommandAndReadAll(portA, "DELETE_CHUNK|" + chunk);

        TestUtils.waitUntil(() -> !Files.exists(file), "chunk to be deleted");
    }

    @Test
    void deleteOfMissingChunkIsNotTreatedAsAnUpload() throws IOException {
        byte[] reply = sendCommandAndReadAll(portA, "DELETE_CHUNK|" + uniqueChunk());

        assertEquals("", new String(reply, StandardCharsets.UTF_8));
        assertFalse(Files.exists(storage(ID_A).resolve("DELETE_CHUNK")));
    }

    @Test
    void acceptLoopExitsWhenServerSocketIsClosed() throws Exception {
        String nodeId = "test-stop-" + UUID.randomUUID();
        try {
            ServerSocket server = new ServerSocket(0);
            Thread node = startNode(server, nodeId);

            server.close();
            node.join(5000);

            assertFalse(node.isAlive(), "acceptChunks should return once its socket is closed");
        } finally {
            TestUtils.deleteRecursively(storage(nodeId));
        }
    }

    @Test
    void replicateCommandPushesChunkToPeer() throws IOException {
        String chunk = uniqueChunk();
        byte[] data = randomBytes(80_000);
        Files.write(storage(ID_A).resolve(chunk), data);

        sendCommandAndReadAll(portA, "REPLICATE_CHUNK|" + chunk + "|127.0.0.1:" + portB);

        Path replica = storage(ID_B).resolve(chunk);
        TestUtils.waitUntil(() -> {
            try {
                return Files.exists(replica) && Files.size(replica) == data.length;
            } catch (IOException e) {
                return false;
            }
        }, "replica on node B");
        assertArrayEquals(data, Files.readAllBytes(replica));
    }
}
