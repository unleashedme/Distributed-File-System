package dfs.client;

import dfs.common.Constants;
import dfs.common.HashUtil;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

public class ClientDownloader {

    public static void downloadAndReassemble(String targetFileName, String outputFileName){

        System.out.println("Querying NameNode for file: "+targetFileName);
        List<String> chunkNames = getFileInfoFromNameNode(targetFileName);

        if(chunkNames == null || chunkNames.isEmpty()){
            System.err.println("download Aborted. file not exists on the DFS");
            return;
        }

        System.out.println("File found. consists of "+chunkNames.size()+" chunks");

        File outputFile = new File(outputFileName);

        try(FileOutputStream out = new FileOutputStream(outputFile, true)){

            for(String chunkData: chunkNames){
                String[] parts = chunkData.split(":");
                String chunkName = parts[0];
                String expectedHash = parts.length>1? parts[1]:"";

                String targetInfoStr = locateChunkFromNameNode(chunkName);
                if(targetInfoStr == null || targetInfoStr.startsWith("ERROR")){
                    System.err.println("Failed to locate "+chunkName+". Download aborted");
                    return;
                }

                String[] targetInfo = targetInfoStr.split(":");
                String ip = targetInfo[0];
                int port = Integer.parseInt(targetInfo[1]);

                File tempChunk = new File("QUARANTINE_"+chunkName);
                fetchChunkFromDataNode(ip, port, chunkName, tempChunk);

                String actualHash = HashUtil.calculateMD5(tempChunk);
                if(!actualHash.equals(expectedHash)){
                    tempChunk.delete();
                    throw new RuntimeException("CRITICAL: Data corruption detected in: "+chunkName+" Expected Hash: "+expectedHash+" Actual Hash: "+actualHash);
                }
                System.out.println("Integrity verified for "+chunkName+" Expected Hash: "+expectedHash+" Actual Hash: "+actualHash);

                try(FileInputStream tempIn = new FileInputStream(tempChunk)){
                    byte[] buffer = new byte[64 * 1024];
                    int bytesRead;
                    while((bytesRead = tempIn.read(buffer))!=-1){
                        out.write(buffer, 0, bytesRead);
                    }
                }
                tempChunk.delete();
            }

            System.out.println("Successfully downloaded and reassembled: "+outputFileName);

        }catch (Exception e){
            System.err.println("Client download error: "+e.getMessage());
        }
    }

    private static List<String> getFileInfoFromNameNode(String fileName) {

        try(Socket nnSocket = new Socket(Constants.NAMENODE_IP, Constants.NAMENODE_PORT);
        PrintWriter out = new PrintWriter(nnSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(nnSocket.getInputStream()))){

            out.println("GET_FILE_INFO|"+fileName);
            String response = in.readLine();

            if(response == null|| response.startsWith("ERROR")){
                return null;
            }

            return Arrays.asList(response.split(","));

        }catch (Exception e){
            System.err.println("Error getting file from NameNode: "+e.getMessage());
            return null;
        }
    }

    private static String locateChunkFromNameNode(String chunkName){
        try(Socket nnSocket = new Socket(Constants.NAMENODE_IP, Constants.NAMENODE_PORT);
            PrintWriter out = new PrintWriter(nnSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(nnSocket.getInputStream()))){

            out.println("LOCATE_CHUNK "+chunkName);
            return in.readLine();
        }catch (Exception e){
            System.err.println("Error communication with namenode: "+e.getMessage());
            return null;
        }
    }

    private static void fetchChunkFromDataNode(String ip, int port, String chunkName, File stagingFile){
        try(Socket dataNodeSocket = new Socket(ip, port);
        OutputStream out = dataNodeSocket.getOutputStream();
        InputStream in = dataNodeSocket.getInputStream();
        FileOutputStream fos = new FileOutputStream(stagingFile)){

            String header = "DOWNLOAD_CHUNK|" + chunkName+"\n";
            out.write(header.getBytes());
            out.flush();

            byte[] buffer = new byte[64 * 1024];
            int bytesRead;
            while((bytesRead = in.read(buffer))!=-1){
                fos.write(buffer, 0, bytesRead);
            }
        }catch (Exception e){
            System.err.println("Failed to fetch "+chunkName+" from datanode "+e.getMessage());
        }
    }

    public static void main(String[] args) {
        System.out.println("Commencing download protocol");
        downloadAndReassemble("testUpload.txt", "finalDownloadedTestUpload.txt");
    }
}
