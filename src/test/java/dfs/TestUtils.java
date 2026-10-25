package dfs;

import dfs.namenode.NameNode;
import dfs.namenode.WALManager;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared helpers for tests. The production code keeps its state in static fields and
 * private methods, so tests reach into it with reflection rather than changing the sources.
 */
public final class TestUtils {

    public static final Path WAL_FILE = Path.of("namenode_wal.log");
    private static final Path WAL_BACKUP = Path.of("namenode_wal.log.testbak");

    private TestUtils() {}

    @SuppressWarnings("unchecked")
    public static <T> T getStatic(Class<?> type, String fieldName) {
        try {
            Field field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void setStatic(Class<?> type, String fieldName, Object value) {
        try {
            Field field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(null, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Object invokeStatic(Class<?> type, String methodName, Class<?>[] paramTypes, Object... args) {
        try {
            Method method = type.getDeclaredMethod(methodName, paramTypes);
            method.setAccessible(true);
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void waitUntil(BooleanSupplier condition, String description) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail("Timed out waiting for: " + description);
    }

    public static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (var paths = Files.walk(path)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    // ---- WAL handling: the WAL lives at a fixed path in the working directory ----

    public static void backupWal() throws IOException {
        if (Files.exists(WAL_FILE)) {
            Files.move(WAL_FILE, WAL_BACKUP, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static void closeWal(WALManager wal) {
        if (wal == null) return;
        try {
            Field field = WALManager.class.getDeclaredField("walWriter");
            field.setAccessible(true);
            ((PrintWriter) field.get(wal)).close();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void restoreWal() throws IOException {
        Files.deleteIfExists(WAL_FILE);
        if (Files.exists(WAL_BACKUP)) {
            Files.move(WAL_BACKUP, WAL_FILE);
        }
    }

    // ---- NameNode state ----

    /** Clears all static NameNode state and installs a fresh WALManager, which is returned. */
    public static WALManager resetNameNode() {
        activeNodes().clear();
        NameNode.chunkLocations.clear();
        NameNode.fileTree.clear();
        NameNode.expectedHashes.clear();
        NameNode.corruptedFiles.clear();
        WALManager wal = new WALManager();
        setStatic(NameNode.class, "walManager", wal);
        return wal;
    }

    public static Map<String, NameNode.NodeInfo> activeNodes() {
        return getStatic(NameNode.class, "activeNodes");
    }

    public static void registerNode(String nodeId, int port) {
        activeNodes().put(nodeId, new NameNode.NodeInfo("127.0.0.1", port, System.currentTimeMillis()));
    }

    /** Runs NameNode's private connection handler on the server side of the given socket. */
    public static void handleOnNameNode(Socket serverSide) {
        Thread handler = new Thread(() ->
                invokeStatic(NameNode.class, "handleDataNode", new Class<?>[]{Socket.class}, serverSide));
        handler.setDaemon(true);
        handler.start();
    }

    /**
     * A stand-in DataNode that records every line it receives, used to observe the
     * commands the NameNode sends out (DELETE_CHUNK, REPLICATE_CHUNK, ...).
     */
    public static final class FakeNode implements Closeable {
        private final ServerSocket server;
        private final BlockingQueue<String> received = new LinkedBlockingQueue<>();

        public FakeNode() throws IOException {
            server = new ServerSocket(0);
            Thread acceptor = new Thread(() -> {
                while (!server.isClosed()) {
                    try (Socket s = server.accept();
                         BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
                        String line;
                        while ((line = in.readLine()) != null) {
                            received.add(line);
                        }
                    } catch (IOException ignored) {
                    }
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();
        }

        public int port() {
            return server.getLocalPort();
        }

        public String nextMessage(long timeoutMs) throws InterruptedException {
            return received.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }
}
