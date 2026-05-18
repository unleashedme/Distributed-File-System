package dfs.namenode;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class WALManager {
    private static final String WAL_FILE = "namenode_wal.log";
    private PrintWriter walWriter;

    public WALManager(){
        try{

            walWriter = new PrintWriter(new FileWriter("namenode_wal.log", true), true);
        }catch(Exception e){
            System.err.println("Failed to initiate write-ahead log: "+e.getMessage());
            System.exit(1);
        }
    }

    public synchronized void logTransaction(String transaction){
        walWriter.println(transaction);
    }
    public List<String> replayLog(){
        List<String> transactions = new ArrayList<>();
        File logFile = new File(WAL_FILE);

        if(!logFile.exists()){
            System.out.println("No existing log file. starting fresh");
            return transactions;
        }

        System.out.println("Replaying write-ahead log");
        try(BufferedReader reader = new BufferedReader(new FileReader(logFile))){
            String line;
            while((line = reader.readLine())!=null){
                transactions.add(line);
            }
            System.out.println("Replayed "+transactions.size()+" transactions");
        }catch (Exception e){
            System.err.println("Failed to read WAL: "+e.getMessage());
        }
        return transactions;
    }
}
