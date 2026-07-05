# QueueCTL - Background Job Queue System

[cite_start]QueueCTL is a production-grade, CLI-based asynchronous background job execution queue engine built natively using Python[cite: 7, 8]. [cite_start]The system manages full background task lifecycles, safely coordinates tasks with concurrent workers using transactional locking, executes shell scripts isolated from the main process, and handles transient failures via exponential backoff delays and a Dead Letter Queue (DLQ)[cite: 8, 10, 11, 12, 13, 14, 66].

---

## 🚀 Architecture Overview

### 1. Job Lifecycle
[cite_start]Jobs transition through predictable, atomic states to protect system predictability[cite: 30]:
* [cite_start]**`pending`**: Task registered in storage, waiting for worker retrieval[cite: 33, 34].
* [cite_start]**`processing`**: Currently picked up and actively running inside an isolated process[cite: 35, 36].
* [cite_start]**`completed`**: Finished running with an exit code of `0`[cite: 37, 38].
* [cite_start]**`failed`**: Halted with a non-zero exit code but eligible for backoff retries[cite: 39, 40].
* [cite_start]**`dead`**: Failed repeatedly and moved permanently to the Dead Letter Queue (DLQ)[cite: 41, 42].

### 2. Data Persistence
[cite_start]All job specifications, metrics, and state metrics persist inside an embedded **SQLite database (`queue.db`)**[cite: 15, 61]. [cite_start]Using SQLite ensures your background tasks survive application crashes and manual machine restarts[cite: 15, 60]. 

### 3. Worker Concurrency & Locking Logic
[cite_start]To enforce rigorous thread/process safety, workers leverage SQLite's **`BEGIN IMMEDIATE`** transaction isolation barriers[cite: 66]. [cite_start]When multiple workers check for tasks simultaneously, only one worker can successfully lock the table row[cite: 66]. [cite_start]This architectural boundary guarantees zero race conditions or duplicate task executions[cite: 66, 117].

---

## ⚙️ Setup Instructions

### Prerequisites
* Python 3.8+ installed locally.

### Installation
Clone the repository and verify your setup environment:
```bash
git clone [https://github.com/NAND-369/QueueCTL-Job-Queue.git](https://github.com/NAND-369/QueueCTL-Job-Queue.git)
cd QueueCTL-Job-Queue