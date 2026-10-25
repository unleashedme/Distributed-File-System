package dfs.client;

import dfs.TestUtils;
import dfs.common.Constants;
import dfs.common.FileMetadata;
import dfs.datanode.DataNode;
import dfs.namenode.NameNode;
import dfs.namenode.WALManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Uploads and downloads through the real client classes against an in-process NameNode
 * handler (bound to the configured NameNode port) and two in-process DataNodes.
 * Skipped if the NameNode port is already taken, e.g. by a running NameNode.
 */
class ClientRoundTripTest {

    private static final String ID_A = "rt-A-" + UUID.randomUUID();
    private static final String ID_B = "rt-B-" + UUID.randomUUID();
    private static ServerSocket nameNodeServer;
    private static int portA;
    private static int portB;

    private WALManager wal;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void startCluster() throws IOException {
        try {
            nameNodeServer = new ServerSocket(Constants.NAMENODE_PORT);
        } catch (IOException e) {
            Assumptions.abort("NameNode port " + Constants.NAMENODE_PORT + " is in use");
        }
        Thread acceptor = new Thread(() -> {
            while (!nameNodeServer.isClosed()) {
                try {
                    Socket s = nameNodeServer.accept();
                    TestUtils.handleOnNameNode(s);
                } catch (IOException ignored) {
                }
            }
        });
        acceptor.setDaemon(true);
        acceptor.start();

        portA = startDataNode(ID_A);
        portB = startDataNode(ID_B);
    }

    @AfterAll
    static void stopCluster() throws IOException {
        if (nameNodeServer != null) nameNodeServer.close();
        TestUtils.deleteRecursively(Path.of("datanode_storage_" + ID_A));
        TestUtils.deleteRecursively(Path.of("datanode_storage_" + ID_B));
    }

    private static int startDataNode(String nodeId) throws IOException {
        ServerSocket server = new ServerSocket(0);
        Thread t = new Thread(() -> DataNode.acceptChunks(server, nodeId));
        t.setDaemon(true);
        t.start();
        return server.getLocalPort();
    }

    @BeforeEach
    void setUp() throws IOException {
        TestUtils.backupWal();
        wal = TestUtils.resetNameNode();
        TestUtils.registerNode(ID_A, portA);
        TestUtils.registerNode(ID_B, portB);
    }

    @AfterEach
    void tearDown() throws IOException {
        TestUtils.closeWal(wal);
        TestUtils.restoreWal();
    }

    private FileMetadata uploadRandomFile(String name, int size) throws IOException {
        byte[] data = new byte[size];
        new Random(size).nextBytes(data);
        Path input = Files.write(tempDir.resolve(name), data);
        Path chunkDir = tempDir.resolve("chunks");

        FileMetadata metadata = FileSplitter.splitFile(input.toString(), chunkDir.toString());
        assertTrue(ClientUploader.uploadFile(metadata, chunkDir.toString()), "upload should succeed");

        // DataNodes send block reports after acknowledging, so wait for the NameNode to catch up.
        for (String chunk : metadata.getChunkNames()) {
            TestUtils.waitUntil(() -> NameNode.chunkLocations.getOrDefault(chunk, Set.of()).size() == 2,
                    "block reports for " + chunk);
        }
        return metadata;
    }

    @Test
    void uploadRegistersFileAndReplicatesEveryChunkToBothNodes() throws IOException {
        FileMetadata metadata = uploadRandomFile("upload.bin", FileSplitter.CHUNK_SIZE * 2 + 500);

        assertEquals(3, NameNode.fileTree.get("upload.bin").size());
        for (String chunk : metadata.getChunkNames()) {
            byte[] original = Files.readAllBytes(tempDir.resolve("chunks").resolve(chunk));
            assertArrayEquals(original, Files.readAllBytes(Path.of("datanode_storage_" + ID_A, chunk)));
            assertArrayEquals(original, Files.readAllBytes(Path.of("datanode_storage_" + ID_B, chunk)));
            assertNotNull(NameNode.expectedHashes.get(chunk));
        }
    }

    @Test
    void downloadReassemblesUploadedFile() throws IOException {
        uploadRandomFile("roundtrip.bin", FileSplitter.CHUNK_SIZE * 2 + FileSplitter.CHUNK_SIZE / 2);
        Path output = tempDir.resolve("downloaded.bin");

        ClientDownloader.downloadAndReassemble("roundtrip.bin", output.toString());

        assertArrayEquals(Files.readAllBytes(tempDir.resolve("roundtrip.bin")), Files.readAllBytes(output));
    }

    @Test
    void downloadReplacesExistingOutputFile() throws IOException {
        uploadRandomFile("overwrite.bin", 5000);
        Path output = Files.writeString(tempDir.resolve("existing.bin"), "stale contents from an earlier download");

        ClientDownloader.downloadAndReassemble("overwrite.bin", output.toString());

        assertArrayEquals(Files.readAllBytes(tempDir.resolve("overwrite.bin")), Files.readAllBytes(output));
    }

    @Test
    void uploadReportsFailureWhenDataNodeIsUnreachable() throws IOException {
        TestUtils.activeNodes().clear();
        TestUtils.registerNode("dead", TestUtils.freePort());
        Path input = Files.writeString(tempDir.resolve("unreachable.txt"), "payload");
        Path chunkDir = tempDir.resolve("chunks");
        FileMetadata metadata = FileSplitter.splitFile(input.toString(), chunkDir.toString());

        assertFalse(ClientUploader.uploadFile(metadata, chunkDir.toString()));
    }

    @Test
    void sendChunkSucceedsThroughFullPipeline() throws IOException {
        Path chunk = Files.writeString(tempDir.resolve("direct.bin"), "direct-send");
        String pipeline = "127.0.0.1:" + portA + ",127.0.0.1:" + portB;

        ClientUploader.sendChunkToDataNode(pipeline, "direct.bin", tempDir.toString());

        assertArrayEquals(Files.readAllBytes(chunk), Files.readAllBytes(Path.of("datanode_storage_" + ID_A, "direct.bin")));
        assertArrayEquals(Files.readAllBytes(chunk), Files.readAllBytes(Path.of("datanode_storage_" + ID_B, "direct.bin")));
    }

    @Test
    void sendChunkThrowsWhenDataNodeIsUnreachable() throws IOException {
        Files.writeString(tempDir.resolve("c.bin"), "data");
        String pipeline = "127.0.0.1:" + TestUtils.freePort();

        assertThrows(IOException.class, () -> ClientUploader.sendChunkToDataNode(pipeline, "c.bin", tempDir.toString()));
    }

    @Test
    void sendChunkThrowsWhenPipelineRejectsChunk() throws IOException {
        Files.writeString(tempDir.resolve("rejected.bin"), "data");
        String pipeline = "127.0.0.1:" + portA + ",127.0.0.1:" + TestUtils.freePort();

        IOException e = assertThrows(IOException.class,
                () -> ClientUploader.sendChunkToDataNode(pipeline, "rejected.bin", tempDir.toString()));
        assertTrue(e.getMessage().contains("rejected"), e.getMessage());
    }

    @Test
    void downloadOfUnknownFileWritesNothing() {
        Path output = tempDir.resolve("never.bin");

        ClientDownloader.downloadAndReassemble("not-uploaded.bin", output.toString());

        assertFalse(Files.exists(output));
    }

    @Test
    void downloadAbortsWhenChunkIsCorrupted() throws IOException {
        FileMetadata metadata = uploadRandomFile("corrupt.bin", 1000);
        String chunk = metadata.getChunkNames().get(0);
        Files.writeString(Path.of("datanode_storage_" + ID_A, chunk), "rotten");
        Files.writeString(Path.of("datanode_storage_" + ID_B, chunk), "rotten");
        Path output = tempDir.resolve("corrupt-download.bin");

        ClientDownloader.downloadAndReassemble("corrupt.bin", output.toString());

        assertEquals(0, Files.size(output));
        assertFalse(Files.exists(Path.of("QUARANTINE_" + chunk)), "quarantined chunk should be removed");
    }
}
