package dfs.datanode;

import dfs.common.Constants;
import dfs.common.HashUtil;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.UUID;

public class DataNode {
    public static void main(String[] args) {
        String nodeId;
        if(args.length>0){
            nodeId = args[0];
        }else{
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter a unique ID for this DataNode (e.g., Node-A, Node-B): ");
            nodeId = scanner.nextLine().trim();
        }
        System.out.println(nodeId+ " is starting up...");

        int dataPort = -1;
        try{
            ServerSocket dataSocket = new ServerSocket(0); // Passing 0 tells the Operating System: "Find me any available, unused port on this machine."
            dataPort = dataSocket.getLocalPort();
            System.out.println(nodeId+" is now listening for chunks on port: "+dataPort);
            new Thread(() -> acceptChunks(dataSocket, nodeId)).start();
        }catch(Exception e){
            System.err.println("Failed to start dataNode server socket:" + e.getMessage());
            return;
        }

        File storageDir = new File("datanode_storage_"+nodeId);
        storageDir.mkdirs();

        new Thread(()->{
            while(true) {
                try {
                    Thread.sleep(30000);
                    File[] storedChunks = storageDir.listFiles();
                    if (storedChunks != null) {
                        for (File chunk : storedChunks) {
                            String hash = HashUtil.calculateMD5(chunk);
                            reportScrubToNameNode(nodeId, chunk.getName(), hash);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Scrubber thread error: " + e.getMessage());
                }
            }
        }).start();

        while(true){
            try(Socket socket = new Socket(Constants.NAMENODE_IP, Constants.NAMENODE_PORT);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){

                System.out.println("Connected to NameNode, starting Heartbeat...");

                if(storageDir.exists() && storageDir.isDirectory()){
                    File[] storedChunks = storageDir.listFiles();
                    if(storedChunks!=null){
                        for (File chunk:storedChunks){
                            out.println("BLOCK_REPORT "+nodeId+" "+chunk.getName());
                            System.out.println("sent boot-up block report for: "+chunk.getName());
                        }
                    }
                }

                while(true){
                    out.println("Heartbeat "+ nodeId+ " "+dataPort);
                    if(out.checkError()){
                        System.out.println("Connection dropped by NameNode");
                        break;
                    }
                    Thread.sleep(Constants.HEARTBEAT_INTERVAL_MS);
                }

            }catch(Exception e){
                System.err.println("Failed to connect to NameNode. Retrying in 5 Seconds");
                try{
                    Thread.sleep(5000);
                }catch (InterruptedException ie){
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void reportScrubToNameNode(String nodeId, String chunkName, String hash) {
        try(Socket socket = new Socket(Constants.NAMENODE_IP, Constants.NAMENODE_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){

            out.println("BLOCK_SCRUB|"+nodeId+"|"+chunkName+"|"+hash);
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
    }

    public  static void acceptChunks(ServerSocket serverSocket, String nodeId){
        File storageDir = new File("datanode_storage_" + nodeId);
        storageDir.mkdirs();
        while(!serverSocket.isClosed()){
            try(Socket clientSocket = serverSocket.accept();
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();
                PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true)){

                System.out.println("Receiving chunk at port "+serverSocket.getLocalPort()+"...");

                StringBuilder headerBuilder = new StringBuilder();
                int c;
                while((c = in.read()) != '\n' && c!=-1){
                    headerBuilder.append((char) c);
                }
                String header = headerBuilder.toString().trim();

                if(header.startsWith("DELETE_CHUNK|")){
                    String chunkName = header.split("\\|")[1];
                    File chunkFile = new File(storageDir, chunkName);
                    if(chunkFile.delete()){
                        System.out.println("purged corrupted chunk: "+chunkName);
                    }else{
                        System.err.println("Failed to purge chunk: "+chunkName);
                    }
                    continue;
                }

                else if(header.startsWith("DOWNLOAD_CHUNK|")){
                    String chunkName = header.split("\\|")[1];
                    File chunkFile = new File(storageDir, chunkName);

                    if(chunkFile.exists()){
                        System.out.println("Serving chunk to client: "+chunkName);
                        try(FileInputStream fis = new FileInputStream(chunkFile)){
                            byte[] buffer = new byte[64 * 1024];
                            int bytesRead;
                            while((bytesRead = fis.read(buffer)) != -1){
                                out.write(buffer, 0, bytesRead);
                            }
                            out.flush();
                        }
                    }else{
                        System.err.println("Chunk not found on the disk: "+chunkName);
                    }
                    continue;
                }
                else if(header.startsWith("REPLICATE_CHUNK|")){
                    String[] parts = header.split("\\|");
                    String chunkName = parts[1];
                    String targetNode = parts[2];

                    File chunkFile = new File(storageDir, chunkName);
                    if(chunkFile.exists()){
                        System.out.println("Recieved command to replicate "+chunkName+" to "+targetNode);

                        new Thread(() -> pushChunkToTarget(chunkFile, chunkName, targetNode)).start();
                    }
                    continue;
                }

                String[] headerParts = header.split("\\|", 2);
                String chunkName = headerParts[0];
                String remainingPipeline = headerParts.length>1? headerParts[1]:"";


                File chunkFile = new File(storageDir, chunkName);

                FileOutputStream fos = new FileOutputStream(chunkFile);
                OutputStream forwardOut = null;
                Socket forwardSocket = null;
                BufferedReader forwardIn = null;

                boolean pipelineHealthy = true;

                if(!remainingPipeline.isEmpty()){
                    try{
                        String[] pipelineNodes = remainingPipeline.split(",",2);
                        String nextNode = pipelineNodes[0];
                        String nextRemaining = pipelineNodes.length>1 ? pipelineNodes[1]:"";

                        String[] nodeInfo = nextNode.split(":");
                        forwardSocket = new Socket(nodeInfo[0], Integer.parseInt(nodeInfo[1]));
                        forwardOut = forwardSocket.getOutputStream();

                        forwardIn = new BufferedReader(new InputStreamReader(forwardSocket.getInputStream()));

                        String nextHeader = chunkName+ "|" + nextRemaining +"\n";
                        forwardOut.write(nextHeader.getBytes());
                        System.out.println("Forwarding "+chunkName+" to "+nextNode);
                    }catch (Exception e){
                        System.err.println("Failed to connect to downstream node. Aborting...");
                        pipelineHealthy = false;
                    }
                }

                if(pipelineHealthy){
                    byte[] buffer = new byte[64 * 1024];
                    int bytesRead;
                    try {
                        while((bytesRead = in.read(buffer))!=-1){
                            fos.write(buffer, 0 , bytesRead);
                            if(forwardOut!=null){
                                forwardOut.write(buffer, 0, bytesRead);
                            }
                        }
                        if (forwardSocket != null) {
                            forwardSocket.shutdownOutput();
                        }
                    } catch (Exception e) {
                        pipelineHealthy = false;
                    }
                }
                fos.close();

                if (pipelineHealthy && forwardSocket != null) {
                    try {
                        String ack = forwardIn.readLine();
                        if (ack == null || !ack.equals("SUCCESS")) {
                            pipelineHealthy = false;
                        }
                    } catch (Exception e) {
                        pipelineHealthy = false;
                    }
                }

                if (pipelineHealthy){
                    clientOut.println("SUCCESS");
                    System.out.println("Commited chunk: "+chunkName);

                    reportBlockToNameNode(nodeId, chunkName);
                }
                else{
                    chunkFile.delete();
                    clientOut.println("ERROR: pipeline failure");
                    System.err.println("Pipeline failed. deleted local copy of " + chunkName);
                }

                if(forwardOut!=null){
                    forwardOut.flush();
                    forwardSocket.close();
                }
                clientSocket.close();
            }catch (Exception e){
                System.err.println("Error accepting chunk: "+e.getMessage());
            }
        }
    }

    private static void pushChunkToTarget(File chunkFile, String chunkName, String targetStr) {
        String[] targetInfo = targetStr.split(":");
        String ip = targetInfo[0];
        int port = Integer.parseInt(targetInfo[1]);

        try(Socket peerSocket = new Socket(ip, port);
        OutputStream out = peerSocket.getOutputStream();
        BufferedReader peerIn = new BufferedReader(new InputStreamReader(peerSocket.getInputStream()));
        FileInputStream fis = new FileInputStream(chunkFile)){

            String header = chunkName+"|\n";
            out.write(header.getBytes());

            byte[] buffer = new byte[64 * 1024];
            int bytesRead;
            while((bytesRead = fis.read(buffer))!=-1){
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
            peerSocket.shutdownOutput();

            String ack = peerIn.readLine();
            if ("SUCCESS".equals(ack)) {
                System.out.println("Successfully replicated " + chunkName + " to peer.");
            } else {
                System.err.println("Peer rejected replication of " + chunkName);
            }

        }catch (Exception e){
            System.err.println("Failed to push replication chunk: "+e.getMessage());
        }
    }

    private static void reportBlockToNameNode(String nodeId, String chunkName) {
        try(Socket nnSocket = new Socket(Constants.NAMENODE_IP, Constants.NAMENODE_PORT);
            PrintWriter nnOut = new PrintWriter(nnSocket.getOutputStream(), true)){

            nnOut.println("BLOCK_REPORT "+nodeId+" "+chunkName);
        }catch (Exception e){
            System.err.println("Failed to send block report for "+chunkName);
        }
    }
}
