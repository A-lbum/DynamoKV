# Midterm Report: Distributed Key-Value Storage

##### Team Members

> Tingxuan Liu 123090363@link.cuhk.edu.cn
>
> Yuxuan Liu 123090377@link.cuhk.edu.cn

## Project Roadmap

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





