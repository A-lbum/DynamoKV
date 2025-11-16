# Midterm Report: Distributed Key-Value Storage

##### Team Members

> Tingxuan Liu 123090363@link.cuhk.edu.cn
>
> Yuxuan Liu 123090377@link.cuhk.edu.cn

## 1. Project Roadmap

### Phase 1: Target planning and System Design

In this phase, we define the project goals, performance metrics, and overall architecture of the system. The focus is on designing a scalable, available, and eventually consistent distributed key-value store inspired by Amazon Dynamo.

- **Background Knowledge Attainment**: Review the designs of Dynamo and Etcd, identifying their trade-offs under the CAP theorem. 

- **System Architecture Design**: First design a standalone Key-value storage node, and then expand it to a distributed framework, i.e., a storage layer and a distribution layer.

### Phase 2: Standalone KV Implementation

In this phase, we implement a single-node key-value storage service that uses a gRPC service to expose basic raw key-value operations. The storage engine is implemented as a wrapper around the Badger key/value API. 

- **gRPC Communication Protocol Design**: Design the client-server gRPC communication protocol by utilizing Protocol Buffers to define the service interfaces and message formats.
- **Standalone Storage Engine Implementation**: Define the storage interface and wrap the underlying key-value store.
- **Raw Key/Value Service Handlers Implementation**: Implement the logic for the raw key/value service handlers.
- **StandaloneKV Test**: Conduct initial tests for correctness and reliability.

### Phase 3: Distributed Framework Implementation

In this phase, we extend the standalone KV store into a fully distributed, Dynamo-style system prototype, implementing scalability, fault tolerance, and eventual consistency.

- **Node Communication and Membership Management**: Implement the **Gossip Protocol** to allow all nodes to be aware of which nodes are currently active in the cluster and to maintain shared state information.
- **Data Partitioning and Load Balancing**: Implement **Consistent Hashing** to distribute keys evenly across the cluster and minimize data movement when nodes join or leave.
- **Fault Tolerance and High Availability**: Implement **Data Replication** to store multiple redundant copies of data for reliability.
- **Elastic Scaling and Data Consistency**: Implement **Dynamic Scaling** to prevent data loss when a node is temporarily unavailable.

### Phase 4: Deployment and Evaluation

In this phase, we deploy the system on Kubernetes and evaluate its scalability, availability, and fault tolerance under realistic workloads.

- **Deployment**: Deploy the system on Kubernetes.
- **Evaluation**: Write test scripts to evaluate the system's scalability, fault tolerance, and eventual consistency.



## 2. Preliminary result

### 2.1 **Standalone KVStore Prototype**

Up tp now,  we have successfully completed a functional **standalone KVstore prototype**, which serves as the foundation for the later distributed version. This preliminary system implements basic key-value operations, gRPC service interface, BadgerDB-Based Storage and  raw Key/Value service handler, which forms callable APIs and provides a clean interface for further extension into a multi-node architecture.

##### **1. Basic Key–Value Operations** 

The standalone system supports:

- **PUT(key, value)** — inserts or updates a key-value pair

- **GET(key)** — retrieves the value associated with a key

- **DELETE(key)** — removes the key-value entry

```go
func (s *StandAloneStorage) Write(batch []storage.Modify) error 
```

Each of these operations will return **standardized response messages**.

##### 2. gRPC Service Interface

To make the KVStore accessible to external clients and prepare for distributed communication, we implemented:

- The gRPC service definitions **(.proto files)**

```
service RawKV {
    rpc RawGet (RawGetRequest) returns (RawGetResponse);
    rpc RawPut (RawPutRequest) returns (RawPutResponse);
    ......
}
```

This will **automatic generate code  for server and client stubs**.

- A lightweight request and response schema

The above implementation provides high-performance communication, built-in serialization, and strong typing, and easily extensible for adding future distributed features such as replication RPCs.

##### **3.BadgerDB-Based Storage**

The prototype construct an accessible interface to **BadgerDB**, which is a high-performance LSM-tree–based embedded key-value store. This provides persistent key-value storage with efficient read and write performance thanks to Badger’s log-structured merge-tree design. It also ensures crash resilience and durability guarantees without requiring external services. As a production-grade storage engine, BadgerDB lays a solid foundation for scaling each node into a fully distributed KVStore with replication, partitioning, and quorum-based operations in later stages.

```go
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	......
}
```

##### 4.**Raw Key/Value Service Handler Implementation**

We designed and implemented the server-side handlers that bind gRPC requests to the storage backend, where each handler will perform:

- **Request Validation** — Ensure all incoming RPC requests are well-formed by checking conditions such as non-empty keys, valid value formats, and correct argument types.

- **Execution of Storage Operation** — Delegate the actual data manipulation to the storage backend, maintaining a clean separation between RPC logic and storage logic.

- **Structured Response Formatting** — Construct consistent, well-defined response messages using the gRPC schema.

- **Error Handling** — Detect issues such as missing keys, malformed requests, invalid arguments... then converting them into standardized gRPC error codes and understandable messages.

  #### Sample function

```go
func (server *Server) RawGet(ctx context.Context, req *rawkv.RawGetRequest) (*rawkv.RawGetResponse, error) 
```

### 2.2 **Preliminary Performance Evaluation**

We conducted a basic performance measurement of our standalone KVStore by performing PUT, GET, and DELETE operations on varying numbers of key-value pairs (n = 100, 1,000, 10,000, 100,000, 1,000,000). The results are  showing below:

=== RUN   TestBasicPerformanceScaling1
| n       | PUT (ms) | GET (ms) | DELETE (ms) |
| ------- | -------- | -------- | ----------- |
| 100     | 5        | 0        | 4           |
| 1000    | 35       | 0        | 27          |
| 10000   | 266      | 4        | 264         |
| 100000  | 2652     | 50       | 2639        |
| 1000000 | 26563    | 910      | 26517       |
--- PASS: TestBasicPerformanceScaling1 (60.36s)

The preliminary performance results demonstrate that both PUT and DELETE operations exhibit roughly linear growth with respect to the number of key-value pairs, indicating that the underlying BadgerDB storage engine maintains predictable performance even under extremely high loads. In contrast, GET operations remain significantly faster than writes and deletions, reflecting efficient read paths and in-memory indexing. Furthermore, despite increasing the dataset size from hundreds to one million entries, the system consistently executes all operations without errors or failures, demonstrating strong stability and robustness at scale.

### 2.3 **Challenges**

#### **RPC Layer Isolation**

We need to ensure that the RPC layer is decoupled from the storage system. This separation is necessary before we continue doing integrate replication, quorum operations, and gossip protocols without modifying core storage logic.

#### **Error Handling**

Handle invalid requests, non-existent keys, or storage-layer exceptions is critical to maintain system correctness. By isolating errors between the RPC and storage layers, the system can return meaningful responses to clients while preventing backend failures from propagating.

#### **Concurrency Handling**

In some occasional cases, the gRPC server may receive multiple Put, Get, or Delete requests concurrently. We need a thread-safe access to the Badger storage  to avoid race conditions, data corruption, or inconsistent state under concurrent load.

### 2.4 **Feasibility**

We conducted two types of tests on our standalone KVStore prototype to demonstrate the correctness and robustness of the basic key-value operations.

**1.Correctness**

This test performs a series of operations—PUT, GET, and DELETE—on multiple keys. For each operation, the test verifies the expected behavior: PUT correctly writes values, GET retrieves the latest value, and DELETE removes the key. This demonstrates that the KVStore API functions correctly and that the server-side handlers properly connect RPC requests to the storage backend

```bash
=== RUN   TestRawPutGetDelete1
    server_test.go:371: PUT operation succeeded for key=testkey
    server_test.go:381: GET operation returned expected value for key=testkey
    server_test.go:390: DELETE operation succeeded for key=testkey
    server_test.go:396: GET after DELETE confirmed key=testkey is not found
    PASS: TestRawPutGetDelete1
```

##### 2. Robustness(**Concurrent Operation**) 

This test simulates multiple clients performing PUT, GET, and DELETE operations concurrently. Using goroutines and a wait group, the test verifies that all operations execute correctly without data races or inconsistencies. Successful completion confirms that the KVStore and BadgerDB storage layer handle concurrent access safely, ensuring thread safety and robustness in a high-concurrency scenario.

```
=== RUN   TestConcurrentPutGetDelete1
    server_test.go:423: PUT key-29 = val-29
    server_test.go:423: PUT key-8 = val-8
    ......(50 operations)
    server_test.go:438: GET key-15 = val-15
    server_test.go:438: GET key-7 = val-7
    ......(50 operations)
    server_test.go:452: DELETE key-28
    server_test.go:452: DELETE key-19
--- PASS: TestConcurrentPutGetDelete1 
```

Together, the two preliminary tests validate the **functional correctness, gRPC integration, and concurrency safety** of the standalone KVStore .

### 2.5 Summary

In conclusion, the above indicates that we have successfully done the standalone KV implementation, which construct solid foundations to expand it to distributes systems. We also did some tests to ensure the correctness and robutness of our current single node system.

## 3. References

1. DeCandia, G., Hastorun, D., Jampani, M., Kakulapati, G., Lakshman, A., Pilchin, A., ... & Vogels, W. (2007). Dynamo: Amazon's highly available key-value store. *ACM SIGOPS operating systems review*, *41*(6), 205-220.

