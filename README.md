# LightRedis: Asynchronous In-Memory Storage Engine

**LightRedis** is a single-threaded, high-performance C++17 key-value storage engine built from scratch using raw Linux system calls, an asynchronous `epoll` network architecture, and live multi-server data streaming.

Instead of hiding behind heavy web frameworks, LightRedis communicates directly with the Linux operating system kernel to handle thousands of users simultaneously over network sockets.

---

## 📋 Overview

Modern web applications rely on ultra-fast caching systems to read and write data in milliseconds. While production software is massive, LightRedis is a custom-engineered version built to explore exactly how memory management, network rulebooks, hard drive file logging, and server-to-server replication function at the lowest level.

---

## 🔑 Key Features

* ✅ **Asynchronous Networking:** Built using the Linux `epoll` API to handle thousands of open client connections at the same time over a single CPU thread.
* ✅ **Official Protocol Parsing:** Strict support for the official Redis Serialization Protocol (RESP) format using symbols like `*` and `$`.
* ✅ **Crash-Resilient Backups:** Automatically logs every change into an Append-Only File (AOF) on the hard drive to survive power failures or crashes.
* ✅ **Automatic Memory Cleaning:** Tracks active system time using Time-To-Live (TTL) expiration mechanics to automatically purge stale data.
* ✅ **Advanced Data Types:** Upgraded storage arrays supporting sequential list operations (`LPUSH` and `LRANGE`).
* ✅ **Live Distributed Syncing:** Supports Master-Slave replication to automatically stream data changes across separate server ports instantly.
* ✅ **Robust Input Validation:**Implemented advanced error handling and input sanitization to prevent server crashes on malformed protocol commands.

---

## 🏗️ System Architecture

```text
Client App  ──[TCP Sockets]──>  epoll Multiplexer  ──>  RESP Translator  ──>  In-Memory Storage (RAM)
                                                                                  │
                                            ┌─────────────────────────────────────┴─────────────────────────────────────┐
                                            ▼                                                                           ▼
                           Append-Only File (AOF Diary on Disk)                                        Master-Slave Replication Stream
```

### 1. The Asynchronous Core (`epoll`)
* Operates on a single thread to eliminate the heavy CPU lag caused by constantly spinning up new threads.
* Monitors thousands of active network pipes simultaneously and wakes up only when data arrives.

### 2. The Storage & Protocol Layer
* Reads the incoming byte stream and breaks down raw network sentences into individual valid execution commands.
* Stores and manages elements in RAM using fast C++ hash maps and vector blocks.

### 3. The Sync & Persistence Layer
* Continuously appends change statements onto the hard drive for boot-up reconstruction using an Append-Only File (AOF).
* Maintains real-time replication streams to instantly distribute information state updates across active slave ports.

---

## 🗺️ Project Roadmap & Development Milestones

*Note: While this table outlines the step-by-step development journey, the final, 
fully-integrated production version of LightRedis is contained entirely 
within the `08_replication` module.*

| Milestone | Module Folder | Core Production Mechanics Built |
| :--- | :--- | :--- |
| **01** | `01_terminal_engine` | Designed the terminal command loop and core `SET`/`GET` dictionary storage in memory. |
| **02** | `02_network_portal` | Opened up network ports using POSIX socket descriptors to listen for remote messages. |
| **03** | `03_resp_protocol` | Built a dynamic string translator to decode professional Redis commands. |
| **04** | `04_multi_client` | Upgraded the server into a non-blocking multi-tasker using Linux kernel event monitors (`epoll`). |
| **05** | `05_persistence` | Added an automated data logging diary to keep database values safe on the hard drive via AOF. |
| **06** | `06_ttl` | Created active/passive memory cleaners that automatically drop keys when their timer expires. |
| **07** | `07_data_structures` | Expanded storage models to hold full dynamic arrays instead of single words. |
| **08** | `08_replication` |Consolidates all previous features (Persistence, TTL, Lists) with replication and robust error handling. |

---
### Project Directory Structure
```text
LightRedis/
├── 01_terminal_engine/
├── 02_network_portal/
├── 03_resp_protocol/
├── 04_multi_client/
├── 05_persistence/
├── 06_ttl/
├── 07_data_structures/
└── 08_replication/  <-- Final Integrated Engine


 ---

## 🚀 Build and Run From Source

### Requirements
* A Linux operating system (Ubuntu / Debian / WSL)
* A C++ compiler supporting C++17 or higher (`g++`)
* Standard network testing tools (`netcat` or `redis-cli`)

### 1. Compilation
The final, integrated engine containing all features (Persistence, TTL, Lists, and Replication) is located in the 08_replication module.
```bash
cd 08_replication
g++ main.cpp -o server
```

### 2. Launch the Master Instance
Start the core primary database instance on the default port:
```bash
./server 6379
```

### 3. Launch a Slave Replica
In a separate terminal tab, spin up a secondary server instance and chain it directly to your master node:
```bash
./server 6380 --replicaof 127.0.0.1 6379
```

### 4. Test Live Replication Stream
Open a third terminal window and broadcast data to the cluster using netcat:
```bash
nc localhost 6379
SET city Noida
```

Connect to your replica on port 6380 to verify data synced across the network automatically:
```bash
nc localhost 6380
GET city
```

---

## 🛠️ Key Architectural Decisions & Limitations

* **Single-Threaded Event Loop:** Opted for a single-threaded architecture managed by `epoll` to bypass the massive CPU context-switching overhead common in multi-threaded connection-per-client models.
* **Zero External Dependencies:** Built completely on raw Linux system calls and standard C++ libraries to gain full exposure to system memory allocation and socket level event loops.
* **Linux Specific:** Relies entirely on the native Linux `epoll` kernel system; requires translation wrappers (like `kqueue`) to execute natively on macOS.

---

## 💡 Why LightRedis Is Different

Most developer projects rely on massive frameworks where everything under the hood is hidden behind a single black box.

LightRedis takes the complete opposite path. By stripping away external libraries and writing raw, direct Linux systems code from scratch, it shows you exactly how memory maps, communication channels, hard drive storage loops, and multi-node clusters interact in real-world, high-performance environments.

---

## 📄 License

MIT License