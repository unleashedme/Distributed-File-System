package dfs.client;

import dfs.common.FileMetadata;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileSplitter {

    public static final int CHUNK_SIZE = 1024*1024;

    public static FileMetadata splitFile(String inputFilePath, String outputDirectory) throws IOException {
        File inputFile = new File(inputFilePath);
        if(!inputFile.exists()){
            throw new FileNotFoundException("File Not Found: "+inputFilePath);
        }

        List<String> chunkNames = new ArrayList<>();
        byte[] buffer = new byte[CHUNK_SIZE];

        new File(outputDirectory).mkdirs();

        try(FileInputStream fis = new FileInputStream(inputFile);
            BufferedInputStream bis = new BufferedInputStream(fis)){

            int bytesRead;
            int chunkIndex=0;

            while((bytesRead = bis.read(buffer))>0){

                String chunkName = inputFile.getName()+"_chunk"+chunkIndex+".bin";
                File chunkFile = new File(outputDirectory, chunkName);

                try(FileOutputStream fos = new FileOutputStream(chunkFile)){
                    fos.write(buffer, 0, bytesRead);
                }

                chunkNames.add(chunkName);
                chunkIndex++;
            }
        }
        System.out.println("Successfully split "+ inputFile.getName()+" into "+ chunkNames.size()+" chunks");
        return new FileMetadata(inputFile.getName(), inputFile.length(), chunkNames);
    }

    private static void createDummyFile(String fileName, double sizeInMB){
        try(FileOutputStream fos = new FileOutputStream(fileName)){
            byte[] data = "DistributedFileSystemDemoData.".getBytes();
            long targetBytes = (long)(sizeInMB*1024*1024);
            long written = 0;

            while(written < targetBytes){
                fos.write(data);
                written+=data.length;
            }
            System.out.println("Created dummy file: " + fileName + " (" + sizeInMB + " MB)");
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        createDummyFile("testUpload.txt", 2.5);

        try{
            FileMetadata metadata = splitFile("testUpload.txt","tempChunks");

            System.out.println(metadata.toString());
            System.out.println("Generated chunks: "+metadata.getChunkNames());
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
