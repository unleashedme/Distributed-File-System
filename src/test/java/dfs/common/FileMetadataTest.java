package dfs.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileMetadataTest {

    @Test
    void gettersReturnConstructorValues() {
        List<String> chunks = List.of("a_chunk0.bin", "a_chunk1.bin");
        FileMetadata metadata = new FileMetadata("a.txt", 1234L, chunks);

        assertEquals("a.txt", metadata.getFileName());
        assertEquals(1234L, metadata.getFileSize());
        assertEquals(chunks, metadata.getChunkNames());
    }

    @Test
    void toStringSummarisesNameSizeAndChunkCount() {
        FileMetadata metadata = new FileMetadata("a.txt", 1234L, List.of("c0", "c1", "c2"));
        assertEquals("File: a.txt (1234 bytes) -> chunks: 3", metadata.toString());
    }
}
