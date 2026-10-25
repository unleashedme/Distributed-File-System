package dfs.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class HashUtilTest {

    @TempDir
    Path tempDir;

    @Test
    void emptyFileHasWellKnownMd5() throws Exception {
        Path file = Files.createFile(tempDir.resolve("empty.bin"));
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", HashUtil.calculateMD5(file.toFile()));
    }

    @Test
    void knownStringHasExpectedMd5() throws Exception {
        Path file = Files.writeString(tempDir.resolve("hello.txt"), "hello", StandardCharsets.UTF_8);
        assertEquals("5d41402abc4b2a76b9719d911017c592", HashUtil.calculateMD5(file.toFile()));
    }

    @Test
    void fileLargerThanReadBufferIsHashedCompletely() throws Exception {
        byte[] data = new byte[200 * 1024 + 17];
        new Random(42).nextBytes(data);
        Path file = Files.write(tempDir.resolve("large.bin"), data);

        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(data));
        assertEquals(expected, HashUtil.calculateMD5(file.toFile()));
    }

    @Test
    void hashIsLowercaseHexOf32Characters() throws Exception {
        Path file = Files.writeString(tempDir.resolve("a.txt"), "anything");
        assertTrue(HashUtil.calculateMD5(file.toFile()).matches("[0-9a-f]{32}"));
    }

    @Test
    void differentContentProducesDifferentHash() throws Exception {
        Path a = Files.writeString(tempDir.resolve("a.txt"), "content-a");
        Path b = Files.writeString(tempDir.resolve("b.txt"), "content-b");
        assertNotEquals(HashUtil.calculateMD5(a.toFile()), HashUtil.calculateMD5(b.toFile()));
    }

    @Test
    void missingFileThrows() {
        File missing = tempDir.resolve("does-not-exist").toFile();
        assertThrows(FileNotFoundException.class, () -> HashUtil.calculateMD5(missing));
    }
}
