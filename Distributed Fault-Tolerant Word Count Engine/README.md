# Distributed Fault-Tolerant Word Count Engine

A robust, distributed word count application designed to simulate a multi-node system using Python processes and Redis. This project implements a Map-Reduce style computation model where stateless workers coordinate through Redis to process large-scale CSV datasets in parallel, ensuring data integrity even in the event of worker or database crashes.

## Overview

This system leverages Redis as a central "Master" to manage task distribution (via Redis Streams) and global state (via Sorted Sets). The architecture focuses on three core distributed systems principles:
1.  **Parallel Execution:** Multi-process workers consuming tasks from a shared stream.
2.  **Worker Fault Tolerance:** Handling worker crashes using `xautoclaim` and idempotent operations.
3.  **Database Fault Tolerance:** Ensuring system recovery from Redis failures using periodic checkpointing (`BGSAVE`) and RDB persistence.

## Dataset Description

The system is designed to process large Twitter customer support datasets. The dataset is available at [link](https://www.kaggle.com/thoughtvector/customer-support-on-twitter) [1].

Each CSV file in the input directory contains the following attributes:

* **tweet_id**: A unique, anonymized ID for the Tweet.
* **author_id**: A unique, anonymized user ID.
* **inbound**: Boolean indicating if the tweet is "inbound" to a company.
* **created_at**: Timestamp of the tweet.
* **text**: The actual content of the tweet (Target attribute for word count).
* **response_tweet_id**: IDs of tweets that are responses to this tweet.
* **in_response_to_tweet_id**: ID of the tweet this tweet is in response to.

## Architecture & Implementation

### Part 1: Parallel Stream Processing
* **Client**: Iterates through the data directory and populates a Redis Stream (`files`) with file paths.
* **Workers**: Utilize `xreadgroup` to ensure each file is processed by exactly one worker at a time.
* **Word Counting**: Workers parse the `text` column of the CSVs using `pandas`, tokenizing content and calculating local frequencies.

### Part 2: Reliability & Atomicity
To handle real-world failure scenarios (crashing or slow workers), the following were implemented:
* **Atomic Updates (Lua Scripting)**: To prevent partial updates if a worker crashes mid-increment, I developed a Lua function `atomic_increment_and_acknowledge`. This ensures that the `XACK` (acknowledgment) and the `ZINCRBY` (global count update) occur as a single atomic unit.
* **Task Stealing**: If a worker crashes after claiming a file, other workers use `xautoclaim` to "steal" and re-process messages that have been idle past a specific timeout.
* **Termination Logic**: Workers use `xpending` to ensure all files in the stream are fully processed and acknowledged before exiting.

### Part 3: Redis Persistence & Recovery
To survive database failures:
* **Checkpointing**: A dedicated thread triggers periodic `BGSAVE` commands to create snapshots (`dump.rdb`) of the Redis state.
* **Recovery**: Upon Redis restart, the system automatically restores from the RDB file. Because the workers are stateless and updates are atomic, any task lost during a crash is simply re-processed, maintaining 100% accuracy.

## Technical Stack

* **Language**: Python 3.10
* **Data Coordination**: Redis 7.4 (Streams, Sorted Sets, Lua Functions)
* **Data Processing**: Pandas
* **Infrastructure**: Docker (for Redis deployment)

## Configuration

The system behavior is controlled via `config.json`:
* `N_NORMAL_WORKERS`: Number of standard processing units.
* `N_CRASHING_WORKERS`: Simulated faulty workers to test fault tolerance.
* `CHECKPOINT_INTERVAL`: Frequency of Redis state snapshots.

## How to Run

1.  **Start Redis**:
    ```bash
    docker run -d -p 6379:6379 -v /your/local/path:/data --name redis --rm redis:7.4
    ```
2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run the Engine**:
    ```bash
    python3 client.py
    ```

# References

\[1\]: <https://www.kaggle.com/thoughtvector/customer-support-on-twitter>