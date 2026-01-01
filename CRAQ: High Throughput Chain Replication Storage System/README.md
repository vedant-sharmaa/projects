# CRAQ: High Throughput Chain Replication Storage System


This project implements a **strongly-consistent distributed key-value store** based on  
**CRAQ (Chain Replication with Apportioned Queries)**, inspired by the USENIX paper: [`CRAQ: A Distibuted Object-Storage System`](https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf)

The system improves read throughput over basic Chain Replication by enabling **distributed reads** (not just tail reads) while preserving **linearizability** and **strong consistency**.


## Features

| Capability | Status |
|------------|---------|
| SET / GET Key-Value Data Store | Implemented |
| Linearizable Consistency Guarantees | Verified with Porcupine |
| Distributed Reads (CRAQ) | Achieves >2× read throughput |
| Multi-Client Support | Concurrent Workers |
| Server Topology Management | Custom Cluster + Replication Logic |
| Network Pooling + Message Passing | Uses custom connection pool |


## Background

The `core` library has the following functionalities:
- `core/message.py` : To define the message format for communication between servers.
    The same format is used by the client to communicate with the server.
    All the messages should be of type `JsonMessage` and can contain any number of fields.
- `core/server.py` : Each server process will be an instance of the `Server` class.
    It listens for incoming connections and handles the messages in a separate
    thread.
- `core/network.py` : To establish connections between servers.
    The class `TcpClient` is used to establish a connection with a server.
    It maintains a pool of connections to the server and reuses them.
    The `send` method sends a message to a server and returns the response message from the server. \
    The class `ConnectionStub` maintains all the connections to the servers in the cluster.
    It takes a set of servers it can connect to and establishes connections to all of them.
    An instance of this class is made available to all the servers in the cluster.
    You are supposed to use this class to send messages to other servers in the cluster.
    It provides an interface to send messages to any server by name in the cluster without 
    worrying about the connection details. It provides the following methods:
    - `send` : To send a message to a server by name and get the response.
    - `initiate_connections` : To establish connections to all the servers in the cluster. 
        This method should be called during the cluster formation before sending any messages.
- `core/cluster.py` : The class `ClusterManager` is used to manage the cluster of servers.
    It starts the servers and establishes connections between them according to a specified topology.
    This class expects the following parameters:
    - topology: The cluster topology information of the form `dict[ServerInfo, set[ServerInfo]]`
    representing a set of servers each server can send messages to. This information is used to create `ConnectionStub`
    which maintains the connections between the servers as explained above.
    - master_name: Let's assume that the tail is the master. This currently does not have much significance.
    - sock_pool_size: The number of sockets to be maintained in the pool for each server. This directly affects the number of concurrent connections that can be maintained by the server.
- `core/logger.py` : To log the messages and events in the system.
    The client logs will be used to check the linearizability of the system.

## System Design of Chain Replication

### CrClient

The `CrClient` object can be used to send `SET` and `GET` requests to the cluster. The client picks the appropriate server to send the request based on the request type. If the request is a `SET` request, it sends the request to the `head` server of the chain. If the request is a `GET` request, it sends the request to the `tail` server of the chain.

The message format for the requests is of type `JsonMessage` and as follows:
- `SET` request: `{"type": "SET", "key": <key>, "value": <value>}`
- `GET` request: `{"type": "GET", "key": <key>}`

### CrCluster
We will manage a cluster of servers (in this assignment, we will have 4 servers) namely a, b, c and d. 
The interconnection among the servers will be mananged by the ClusterManager. 

![Image](docs/cr-chain.png)

- Topology: For each server we will store the set of servers that, this server can 
send messages to.
    ```python3
        topology={self.a: {self.b}, # a can send message to b
            self.b: {self.c},       # b can send message to c
            self.c: {self.d},       # c can send message to d
            self.d: set()}          # d does not send message to any server
    ```
- Each server will also store its previous and next server in the chain.
- The first server of the chain is called `head` and the last one is called `tail`.
- The `connect` method of `CrCluster` returns a `CrClient` object which can be used to send requests to the cluster.

### CrServer

This extends the `Server` class from the `core` library. It stores the key-value pairs in its local dictionary. It additionally stores the previous and next server in the chain. `_process_req` is called whenever a message is received by the server. The server processes the request and sends the response back to the client. 

#### Handling SET Request
Whenever the `head` server receives a `SET` request, it updates its local
dictionary and sends this request to its adjacent server. The `tail` upon receving
this request sends the acknowledgement as `"status" : "OK"` message to the
penultimate node of the chain. This way, the acknowlegment is sent back.

When this acknowledgement reaches back the `head` node, we say that the `SET`
request is completed and the head node sends the acknowledgement to the client.

#### Handling GET Request
In Chain replication, the `GET` request is always sent to the `tail` node. Hence
when it recieves such a request, it sends the response from its local dictionary.

## Checking Linearizability

**PRE-REQUISITE**: You need to have `go` version >= 1.21 installed on your system. You can find the installation instructions [here](https://golang.org/doc/install).

To test that the read-write history of the system is linearizable, we have used
[Porcupine](https://github.com/anishathalye/porcupine).

We use the logs from the client to check if the history is linearizable or not. 
The `lcheck` library is used for testing. 
This assumes that the initial value of any key is `"0"` if no explicit SET call is made.

Run this command to test linearizability in lcheck directory:
```bash!
go run main.go <client-log-file-path>
``` 

The testcases can consist of multiple worker threads (clients) sending requests concurrently.
A request can be a `SET` or a `GET` request and the client logs the request and the response.
The following is the expected format of the client logs:
```
12:15:50 INFO worker_0 Setting key = 0
12:15:50 INFO worker_1 Getting key
12:15:50 INFO worker_1 Get key = 0
12:15:50 INFO worker_0 Set key = 0
```
The linearizability checker looks for 4 types of events in the logs:
- `Setting <key> = <value>` : A `SET` request is made by the client.
- `Set <key> = <value>` : The `SET` request is completed.
- `Getting <key>` : A `GET` request is made by the client.
- `Get <key> = <value>` : The `GET` request is completed.

NOTE: Here, worker and client are used interchangeably.

It also fetches the worker id from the log to track the order of events.
The worker name must follow the format `worker_<id>` and be unique.
A worker should only send requests one after the other, not concurrently.


## CRAQ
Taking the implementation of Chain Replication as reference, we implemented CRAQ, using the `core/` library to setup the server framework,
handle connections, and transfer messages.

In CRAQ, the `GET` requests could be served by any server (not just the tail, as
opposed to the case of Chain Replication - Refer Section2.3 of the paper). Hence, the read throughput should be
comparatively high for CRAQ (refer Figure 4 of the paper).


## Measuring Throughput

The throughput of the system can be measured by the number of requests processed by the system in a given time. You can trigger requests from multiple clients concurrently to measure the throughput of the system. Each client should send requests sequentially for a specified duration. The throughput can be calculated as the total number of requests processed by the system in that duration.

In order to measure the throughput, you should limit the bandwidth of the network. You can use the `tc` command to limit the bandwidth of the network. Steps to limit the bandwidth are in the Makefile. You can use the following command to limit the bandwidth to 10kbps:
```bash
make limit_ports
```

NOTE: The `tc` command requires `sudo` permissions. You can run the `make limit_ports` command with `sudo` permissions. This command works only on the linux machines.
