package dfs.client;

import dfs.common.FileMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static dfs.client.FileSplitter.CHUNK_SIZE;
import static org.junit.jupiter.api.Assertions.*;

class FileSplitterTest {

    @TempDir
    Path tempDir;

    private Path writeRandomFile(String name, int size) throws IOException {
        byte[] data = new byte[size];
        new Random(size).nextBytes(data);
        return Files.write(tempDir.resolve(name), data);
    }

    private byte[] reassemble(Path chunkDir, List<String> chunkNames) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String chunk : chunkNames) {
            out.write(Files.readAllBytes(chunkDir.resolve(chunk)));
        }
        return out.toByteArray();
    }

    @Test
    void smallFileProducesSingleChunk() throws IOException {
        Path input = writeRandomFile("small.txt", 100);
        Path out = tempDir.resolve("chunks");

        FileMetadata metadata = FileSplitter.splitFile(input.toString(), out.toString());

        assertEquals("small.txt", metadata.getFileName());
        assertEquals(100, metadata.getFileSize());
        assertEquals(List.of("small.txt_chunk0.bin"), metadata.getChunkNames());
        assertArrayEquals(Files.readAllBytes(input), Files.readAllBytes(out.resolve("small.txt_chunk0.bin")));
    }

    @Test
    void fileOfExactMultipleOfChunkSizeHasNoTrailingChunk() throws IOException {
        Path input = writeRandomFile("exact.bin", 2 * CHUNK_SIZE);
        Path out = tempDir.resolve("chunks");

        FileMetadata metadata = FileSplitter.splitFile(input.toString(), out.toString());

        assertEquals(List.of("exact.bin_chunk0.bin", "exact.bin_chunk1.bin"), metadata.getChunkNames());
        for (String chunk : metadata.getChunkNames()) {
            assertEquals(CHUNK_SIZE, Files.size(out.resolve(chunk)));
        }
    }

    @Test
    void lastChunkHoldsTheRemainder() throws IOException {
        int size = 2 * CHUNK_SIZE + CHUNK_SIZE / 2;
        Path input = writeRandomFile("partial.bin", size);
        Path out = tempDir.resolve("chunks");

        FileMetadata metadata = FileSplitter.splitFile(input.toString(), out.toString());

        assertEquals(3, metadata.getChunkNames().size());
        assertEquals(CHUNK_SIZE / 2, Files.size(out.resolve("partial.bin_chunk2.bin")));
        assertEquals(size, metadata.getFileSize());
    }

    @Test
    void chunksReassembleToOriginalFile() throws IOException {
        Path input = writeRandomFile("roundtrip.bin", 3 * CHUNK_SIZE + 12345);
        Path out = tempDir.resolve("chunks");

        FileMetadata metadata = FileSplitter.splitFile(input.toString(), out.toString());

        assertArrayEquals(Files.readAllBytes(input), reassemble(out, metadata.getChunkNames()));
    }

    @Test
    void emptyFileProducesNoChunks() throws IOException {
        Path input = Files.createFile(tempDir.resolve("empty.txt"));

        FileMetadata metadata = FileSplitter.splitFile(input.toString(), tempDir.resolve("chunks").toString());

        assertTrue(metadata.getChunkNames().isEmpty());
        assertEquals(0, metadata.getFileSize());
    }

    @Test
    void createsMissingOutputDirectories() throws IOException {
        Path input = writeRandomFile("a.txt", 10);
        Path out = tempDir.resolve("nested").resolve("chunks");

        FileSplitter.splitFile(input.toString(), out.toString());

        assertTrue(Files.isDirectory(out));
    }

    @Test
    void missingInputFileThrows() {
        String missing = tempDir.resolve("missing.txt").toString();
        assertThrows(FileNotFoundException.class,
                () -> FileSplitter.splitFile(missing, tempDir.resolve("chunks").toString()));
    }
}
