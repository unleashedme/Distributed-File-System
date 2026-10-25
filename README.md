# Distributed File System (DFS)

A highly available, fault-tolerant, and self-healing Distributed File System built entirely from scratch in standard Java. 

This project simulates the core architectural patterns of enterprise storage systems like Hadoop HDFS and Google File System (GFS), specifically focusing on decentralized storage, pipeline replication, cryptographic data integrity, and automated cluster recovery.

## System Architecture

The system is divided into three primary components operating over raw TCP sockets:

* **NameNode (The Master):** A stateless metadata router. It maintains the hierarchical file tree and orchestrates network traffic, but never touches the physical data bytes.
* **DataNode (The Worker):** The stateful storage engines. They hold the physical 64KB file chunks, perform continuous cryptographic self-audits, and handle peer-to-peer data replication.
* **Client:** The interface that shreds local files into chunks, negotiates routing with the NameNode, streams bytes to the DataNodes, and dynamically verifies integrity during reassembly.

## Core Features

### 1. Persistent State via Write-Ahead Log (WAL)
To prevent master-node amnesia upon reboot, the NameNode relies on a strictly append-only Write-Ahead Log (`namenode_wal.log`). Every file registration or deletion is flushed to disk before the memory map is updated. Upon a crash, the NameNode replays this log to reconstruct the logical file tree in milliseconds.

### 2. Stateless Physical Routing (The Ghost Directory Fix)
Physical routing maps are highly volatile. Therefore, the NameNode does not log where chunks are physically stored. Instead, the system relies on a **Boot-Up Block Report** architecture. When a DataNode connects to the cluster, it scans its permanent local storage (`datanode_storage_{ID}`) and actively reports its inventory to the NameNode, dynamically rebuilding the routing table in real-time.

### 3. Protection Against Silent Data Corruption (Bit Rot)
TCP guarantees packet delivery, but not long-term physical disk integrity. DFS implements a "Trust but Verify" cryptographic model:

* **Active Verification (Data Scrubbing):** DataNodes run a background thread that calculates the MD5 hash of every chunk every 30 seconds and reports it to the NameNode.
* **Destination Verification:** During peer-to-peer pipeline replication, the receiving DataNode recalculates the incoming MD5 hash in memory before saving it to disk, instantly terminating the transfer if a peer attempts to replicate corrupted data.

### 4. Autonomous Self-Healing & Tombstoning
If a DataNode goes offline or reports a corrupted block via the Scrubber, the NameNode instantly excises that node from the routing table and commands a surviving replica to clone the data to a new healthy node.

In the event of a simultaneous multi-node failure resulting in zero valid replicas, the NameNode executes the **Tombstone Protocol**: it quarantines the file, permanently flags it as dead in the WAL, and intercepts any future download attempts with a catastrophic data loss alert, preventing the Client from ever assembling poisoned data.

## Getting Started

### Prerequisites
* Java 24 or higher (the build targets Java 24)
* Apache Maven

### Installation & Build
This project uses Maven to manage builds and package executable JAR files. Clone the repository and run the build command from the root directory:
```bash
git clone https://github.com/yourusername/java-distributed-file-system.git
cd java-distributed-file-system
mvn clean package
```
This will generate `NameNode.jar` and `DataNode.jar` inside the `/target` directory.

### Configuration
Cluster variables are extracted from the source code and managed via the `dfs.properties` file located in the root directory.

```properties
namenode.ip=127.0.0.1
namenode.port=9000
dfs.replication.factor=2
dfs.heartbeat.interval.ms=5000
```
## Running a Local Cluster

**1. Start the NameNode:**

```bash
java -jar target/NameNode.jar
```

**2. Start the DataNodes (Requires a unique static ID):**
Open new terminal windows and spin up as many DataNodes as your replication factor requires.

```bash
java -jar target/DataNode.jar Node-Alpha
java -jar target/DataNode.jar Node-Beta
java -jar target/DataNode.jar Node-Gamma
```

**3. Run the Client:**
(Currently executed via IDE or by compiling the Client classes directly, pointing to your local file paths).

## Running Tests

The project has a JUnit 5 test suite covering the common utilities, the file splitter, the WAL, NameNode request handling and self-healing, DataNode storage and replication, and a full client upload/download round trip.

Run it from the root directory with a JDK 24 or newer:

```bash
mvn test
```

If your default `java` is older than 24, point Maven at a newer JDK for that command:

```bash
JAVA_HOME=/path/to/jdk-24 mvn test
```

Notes:
* The tests run against real sockets on `127.0.0.1`, so no external services are needed.
* `ClientRoundTripTest` binds the NameNode port from `dfs.properties` (9000 by default) and is skipped if that port is already in use, for example by a running NameNode. Stop any local cluster first to run the whole suite.
* The tests back up and restore any existing `namenode_wal.log` and clean up the DataNode storage directories they create.

## Tech Stack
- **Language:** Java (Core)

- **Networking:** java.net.Socket, ServerSocket

- **Concurrency:** java.util.concurrent, Thread, synchronized

- **Build System:** Apache Maven

- **Cryptography:** java.security.MessageDigest(MD5)

## Project Structure

```bash

Distributed-File-System/
├── pom.xml                     
├── dfs.properties              
└── src/
    ├── main/
    │   └── java/
    │       └── dfs/
    │           ├── common/
    │           │   ├── Constants.java
    │           │   ├── ConfigManager.java
    │           │   ├── FileMetadata.java
    │           │   └── HashUtil.java
    │           ├── namenode/
    │           │   ├── NameNode.java
    │           │   └── WALManager.java
    │           ├── datanode/
    │           │   └── DataNode.java
    │           └── client/
    │               ├── ClientUploader.java
    │               ├── ClientDownloader.java
    │               └── FileSplitter.java
    └── test/
        └── java/
            └── dfs/
                ├── TestUtils.java
                ├── common/
                │   ├── ConfigManagerTest.java
                │   ├── FileMetadataTest.java
                │   └── HashUtilTest.java
                ├── namenode/
                │   ├── NameNodeTest.java
                │   └── WALManagerTest.java
                ├── datanode/
                │   └── DataNodeTest.java
                └── client/
                    ├── ClientRoundTripTest.java
                    └── FileSplitterTest.java
```                    
