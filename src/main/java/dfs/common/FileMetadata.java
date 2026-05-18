package dfs.common;

import java.util.List;

public class FileMetadata {
    private String fileName;
    private long fileSize;
    private List<String> chunkNames;

    public FileMetadata(String fileName, long fileSize, List<String> chunkNames){
        this.chunkNames = chunkNames;
        this.fileName = fileName;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public List<String> getChunkNames() {
        return chunkNames;
    }

    @Override
    public String toString() {
        return "File: "+ fileName + " (" +fileSize+" bytes) -> chunks: "+ chunkNames.size();
    }
}
