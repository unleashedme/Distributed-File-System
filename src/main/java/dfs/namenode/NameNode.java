package dfs.namenode;

import dfs.common.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NameNode {

    public static class NodeInfo{
        public String ipAddress;
        public int dataPort;
        public long lastHeartBeat;

        public NodeInfo(String ipAddress, int dataPort, long lastHeartBeat) {
            this.ipAddress = ipAddress;
            this.dataPort = dataPort;
            this.lastHeartBeat = lastHeartBeat;
        }
    }

    private static final ConcurrentHashMap<String, NodeInfo> activeNodes = new ConcurrentHashMap<>();
    private static final long TIMEOUT_MS = Constants.TIMEOUT_MS;

    public static final ConcurrentHashMap<String, Set<String>> chunkLocations = new ConcurrentHashMap<>();
    private static final int REPLICATION_FACTOR = Constants.REPLICATION_FACTOR;

    public static final ConcurrentHashMap<String, List<String>> fileTree = new ConcurrentHashMap<>();
    private static WALManager walManager;

    public static final ConcurrentHashMap<String, String> expectedHashes = new ConcurrentHashMap<>();

    public static final Set<String> corruptedFiles = ConcurrentHashMap.newKeySet();


    public static void main(String[] args) {

        System.out.println("Starting NameNode on port "+ Constants.NAMENODE_PORT+"...");

        walManager = new WALManager();
        recoverState();

        startHealthMonitor();

        try(ServerSocket serverSocket = new ServerSocket(Constants.NAMENODE_PORT)){

            System.out.println("NameNode is active and listening for DataNodes");
            while(true){
                Socket clientSocket = serverSocket.accept();

                new Thread(() -> {
                    try {
                        handleDataNode(clientSocket);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }

        }catch (Exception e){
            System.err.println("NameNode server error: "+e.getMessage());
        }
    }

    private static void recoverState(){
        List<String> transactions = walManager.replayLog();
        for(String tx : transactions){
            if(tx.startsWith("CREATE_FILE|")){
                String[] parts = tx.split("\\|");
                String fileName = parts[1];
                List<String> chunks = Arrays.asList(parts[2].split(","));
                fileTree.put(fileName, chunks);

                for(String chunkData: chunks){
                    String[] chunkParts = chunkData.split(":");
                    if(chunkParts.length>1){
                        expectedHashes.put(chunkParts[0], chunkParts[1]);
                    }
                }
            }
            else if(tx.startsWith("TOMBSTONE_FILE|")){
                String fileName = tx.split("\\|")[1];
                fileTree.remove(fileName);
                corruptedFiles.add(fileName);
            }
        }
    }

    private static void handleDataNode(Socket socket) throws IOException {
        try(BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))){
            String message;
            String ipAddress = socket.getInetAddress().getHostAddress();

            while((message = in.readLine())!=null){

                if(message.startsWith("Heartbeat ")){
                    String[] parts = message.split(" ");
                    if(parts.length == 3){
                        String nodeId = parts[1];
                        int dataPort = Integer.parseInt(parts[2]);
                        activeNodes.put(nodeId, new NodeInfo(ipAddress, dataPort, System.currentTimeMillis()));
                        System.out.println("Registry updated: " + nodeId + " available at " + ipAddress + ":" + dataPort);
                    }
                }
                else if(message.startsWith("Upload_Request ")){
                    int numChunks = Integer.parseInt(message.split(" ")[1]);
                    if(activeNodes.isEmpty()){
                        socket.getOutputStream().write("Error: No active dataNodes available.\n".getBytes());
                        continue;
                    }

                    StringBuilder pipeline = new StringBuilder();
                    int count=0;
                    for(NodeInfo info: activeNodes.values()){
                        if(count>0) pipeline.append(',');
                        pipeline.append(info.ipAddress).append(':').append(info.dataPort);
                        count++;
                        if(count>=2) break;
                    }
                    String response = pipeline.toString()+"\n";
                    socket.getOutputStream().write(response.getBytes());
                    System.out.println("Assigned pipeline: "+response.trim());
                }
                else if(message.startsWith("BLOCK_REPORT")){
                    String[] parts = message.split(" ");
                    if(parts.length == 3){
                        String nodeId = parts[1];
                        String chunkName = parts[2];

                        chunkLocations.computeIfAbsent(chunkName, k -> new HashSet<>()).add(nodeId);
                        System.out.println("Block report: "+chunkName+" is now hosted on "+nodeId);
                    }
                }
                else if(message.startsWith("LOCATE_CHUNK")){
                    String chunkName = message.split(" ")[1];
                    Set<String> hostNodes = chunkLocations.get(chunkName);

                    if (hostNodes == null || hostNodes.isEmpty()) {
                        socket.getOutputStream().write("ERROR: Chunk not found\n".getBytes());
                    }
                    else {
                        NodeInfo targetHost = null;
                        for(String hostId: hostNodes){
                            if(activeNodes.containsKey(hostId)){
                                targetHost = activeNodes.get(hostId);
                                break;
                            }
                        }

                        if(targetHost != null){
                            String response = targetHost.ipAddress+":"+ targetHost.dataPort+"\n";
                            socket.getOutputStream().write(response.getBytes());
                            System.out.println("Directed client to "+targetHost.ipAddress+ " for "+chunkName);
                        }else {
                            socket.getOutputStream().write("ERROR: All nodes hosting this chunk are dead.\n".getBytes());
                        }
                    }
                }
                else if(message.startsWith("CREATE_FILE|")){
                    walManager.logTransaction(message);

                    String[] parts = message.split("\\|");
                    String fileName = parts[1];
                    List<String> chunks = Arrays.asList(parts[2].split(","));
                    fileTree.put(fileName, chunks);

                    for(String chunkData : chunks) {
                        String[] chunkParts = chunkData.split(":");
                        if (chunkParts.length > 1) {
                            expectedHashes.put(chunkParts[0], chunkParts[1]);
                        }
                    }

                    System.out.println("Persisted new file to tree: "+fileName);
                    socket.getOutputStream().write("SUCCESS\n".getBytes());
                }
                else if(message.startsWith("GET_FILE_INFO|")){
                    String fileName = message.split("\\|")[1];

                    if(corruptedFiles.contains(fileName)){
                        socket.getOutputStream().write("ERROR: CATASTROPHIC_FAILURE - File suffered total data loss. All replicas rotted. Please re-upload\n".getBytes());
                        return;
                    }

                    List<String> chunks = fileTree.get(fileName);

                    if(chunks!=null){
                        String response = String.join(",", chunks)+"\n";
                        socket.getOutputStream().write(response.getBytes());
                        System.out.println("Served file info for: "+fileName);
                    }else{
                        socket.getOutputStream().write("ERROR: file not found in registry.\n".getBytes());
                    }
                }
                else if(message.startsWith("BLOCK_SCRUB|")){
                    String[] parts = message.split("\\|");
                    String nodeId = parts[1];
                    String chunkName = parts[2];
                    String actualHash = parts[3];

                    String expectedHash = expectedHashes.get(chunkName);

                    if(expectedHash == null){
                        System.err.println("Scrub report for unknown chunk "+chunkName+" from "+nodeId+". Ignoring");
                    }
                    else if(!expectedHash.equals(actualHash)){
                        System.err.println("CRITICAL: Bit rot detected on "+nodeId+" for "+chunkName);
                        triggerActiveHealing(nodeId, chunkName);
                    }
                }
            }
        }catch (Exception e){
            System.out.println("Connection lost with a datanode.");
        }
    }

    private static void triggerActiveHealing(String badNodeId, String chunkName) {
        Set<String> locations = chunkLocations.get(chunkName);

        if(locations!=null && locations.contains(badNodeId)){

            locations.remove(badNodeId);
            NodeInfo badNodeInfo = activeNodes.get(badNodeId);

            if(badNodeInfo != null){
                try(Socket socket = new Socket(badNodeInfo.ipAddress, badNodeInfo.dataPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){

                    out.println("DELETE_CHUNK|"+chunkName);
                }catch (Exception e){
                    System.err.println("failed to send delete command to bad node");
                }
            }

            if(!locations.isEmpty()){
                String goodNodeId = locations.iterator().next();
                NodeInfo goodNodeInfo = activeNodes.get(goodNodeId);

                if(goodNodeInfo!=null && badNodeInfo!=null){
                    System.out.println("Commanding "+goodNodeId+" to push healthy chunk to "+badNodeId);
                    commandReplication(goodNodeInfo, badNodeInfo, chunkName);

                    locations.add(badNodeId);
                }
            }else{
                System.err.println("DATA LOSS: No healthy replicas available for "+chunkName);

                String parentFileName = null;
                for(var entry: fileTree.entrySet()){
                    for(String chunkData: entry.getValue()){
                        if(chunkData.startsWith(chunkName+":")){
                            parentFileName = entry.getKey();
                            break;
                        }
                    }
                }

                if(parentFileName!=null){
                    fileTree.remove(parentFileName);
                    corruptedFiles.add(parentFileName);
                    walManager.logTransaction("TOMBSTONE_FILE|" + parentFileName);
                    System.err.println("File [" + parentFileName + "] has been tombstoned. It requires client re-upload.");
                }
            }
        }
    }

    private static void startHealthMonitor(){
        new Thread(() -> {
            while (true) {
                try{
                    Thread.sleep(5000);

                    long now = System.currentTimeMillis();
                    List<String> deadNodes = new ArrayList<>();

                    for(var entry: activeNodes.entrySet()){
                        if((now-entry.getValue().lastHeartBeat) > TIMEOUT_MS){
                            deadNodes.add(entry.getKey());
                        }
                    }

                    for(String deadNodeId: deadNodes){
                        System.err.println("ALERT: "+deadNodeId+" is dead. removing from registry.");
                        activeNodes.remove(deadNodeId);

                        handleNodeFailure(deadNodeId);
                    }
                }catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    System.err.println("Health Monitor Interrupted");
                    break;
                }
            }
        }).start();
    }

    private static void handleNodeFailure(String deadNodeId){
        System.out.println("Initiating recovery for chunks lost on "+deadNodeId);

        for(String chunkName: chunkLocations.keySet()){
            Set<String> locations = chunkLocations.get(chunkName);

            if(locations.contains(deadNodeId)){
                locations.remove(deadNodeId);

                if(locations.size()<REPLICATION_FACTOR && locations.size()>0){
                    System.out.println("chunk "+chunkName+ " is under replicated. Healing now...");

                    String sourceNodeId = locations.iterator().next();
                    NodeInfo sourceInfo = activeNodes.get(sourceNodeId);

                    NodeInfo targetInfo = null;
                    for(var entry: activeNodes.entrySet()){
                        if(!locations.contains(entry.getKey())){
                            targetInfo = entry.getValue();
                            break;
                        }
                    }

                    if(sourceInfo!=null && targetInfo!=null){
                        commandReplication(sourceInfo, targetInfo, chunkName);
                    }else{
                        System.err.println("Not enough active nodes to replicate "+chunkName);
                    }
                }
            }else if(locations.isEmpty()){
                System.err.println("CRITICAL DATA LOSS: all copies of "+chunkName+"are gone");
            }
        }
    }

    private static void commandReplication(NodeInfo source, NodeInfo target, String chunkName) {
        try(Socket socket = new Socket(source.ipAddress, source.dataPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){

            String command = "REPLICATE_CHUNK|"+chunkName+"|"+target.ipAddress+":"+target.dataPort+"\n";
            out.write(command);
            out.flush();
            System.out.println("Commanded " + source.ipAddress + " to replicate " + chunkName + " to " + target.ipAddress);
        }catch (Exception e){
            System.err.println("Failed to send replication command: "+e.getMessage());
        }
    }
}
