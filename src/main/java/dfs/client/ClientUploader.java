package dfs.client;

import dfs.common.Constants;
import dfs.common.FileMetadata;
import dfs.common.HashUtil;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ClientUploader {
    public static boolean uploadFile(FileMetadata metadata, String chunkDirectory){
        try(Socket nameNodeSocket = new Socket(Constants.NAMENODE_IP, Constants.NAMENODE_PORT);
            PrintWriter nameNodeOut = new PrintWriter(nameNodeSocket.getOutputStream(), true);
            BufferedReader nameNodeIn = new BufferedReader(new InputStreamReader(nameNodeSocket.getInputStream()))){

            List<String> securedChunks = new ArrayList<>();
            for(String chunkName: metadata.getChunkNames()){
                File localChunk = new File(chunkDirectory, chunkName);
                String hashValue = HashUtil.calculateMD5(localChunk);
                securedChunks.add(chunkName+":"+hashValue);
            }

            String chunkListStr = String.join(",", securedChunks);
            nameNodeOut.println("CREATE_FILE|"+metadata.getFileName()+"|"+chunkListStr);

            String regResponse = nameNodeIn.readLine();
            if(!"SUCCESS".equals(regResponse)){
                System.err.println("NameNode refused file registration.");
                return false;
            }


            System.out.println("Requesting assignment for "+metadata.getChunkNames().size()+" chunks");
            nameNodeOut.println("Upload_Request "+metadata.getChunkNames().size());

            String response = nameNodeIn.readLine();
            if(response == null || response.startsWith("Error")){
                System.err.println("Upload failed: "+ response);
                return false;
            }


            System.out.println("NameNode assigned Pipeline: " + response);

            for(String chunkName: metadata.getChunkNames()){
                sendChunkToDataNode(response, chunkName, chunkDirectory);
            }

            System.out.println("Upload complete!");
            return true;
        }catch (Exception e){
            System.err.println("Client Error: "+e.getMessage());
            return false;
        }
    }

    public static void sendChunkToDataNode(String pipelineStr, String chunkName, String chunkDir) throws IOException {
        File chunkFile = new File(chunkDir, chunkName);

        String[] pipeline = pipelineStr.split(",",2);
        String firstTarget = pipeline[0];
        String remainingPipeline = pipeline.length>1 ? pipeline[1]:"";

        String[] targetInfo = firstTarget.split(":");
        String ip = targetInfo[0];
        int port = Integer.parseInt(targetInfo[1]);

        try(Socket dataNodeSocket = new Socket(ip, port);
            OutputStream out = dataNodeSocket.getOutputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(dataNodeSocket.getInputStream()));
            FileInputStream fis = new FileInputStream(chunkFile)){

            String header = chunkName+"|"+remainingPipeline+"\n";
            out.write(header.getBytes());

            byte[] buffer = new byte[1024 * 64];
            int bytesRead;
            while((bytesRead = fis.read(buffer))!=-1){
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
            System.out.println("Sent "+chunkName+" to pipeline with "+ip+":"+port);

            dataNodeSocket.shutdownOutput();

            String ack = in.readLine();
            if("SUCCESS".equals(ack)){
                System.out.println("Upload confirmed for "+ chunkName);
            }
            else{
                System.err.println("Upload rejected for "+chunkName+" - "+ack);
                throw new IOException("Datanode rejected "+chunkName+": "+ack);
            }
        }catch (IOException e){
            System.err.println("Failed to send "+ chunkName +":"+ e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        try{
            System.out.println("Starting Client Uploader Process...");
            FileMetadata metadata = FileSplitter.splitFile("testUpload.txt", "tempChunks");

            uploadFile(metadata, "tempChunks");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
