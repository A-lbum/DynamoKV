# CSC4160 Final Report : A Gossip-based Distributed Key-Value Storage System

Tingxuan Liu 123090363@link.cuhk.edu.cn

Yuxuan Liu 123090377@link.cuhk.edu.cn

### Demo video link:

------

## 1. Motivation

Distributed key-value stores play a central role in modern cloud and web-scale systems. Among existing designs, **Amazon’s Dynamo** demonstrates exceptional performance in achieving high availability, partition tolerance, and elastic scalability in real production environments (DeCandia et al., 2007). Its success highlights the importance of decentralized architectures that avoid single points of failure and continue operating effectively under node outages or network partitions.

**Motivated by these insights**, our project aims to build a simplified system that captures the essential characteristics of Dynamo. It provides horizontal scalability through partitioned data storage and maintains high availability by storing multiple copies of data. The system also offers eventual consistency and adapts to workload changes with dynamic node join and leave. Moreover, it also manages cluster membership and preserves write availability even when nodes temporarily fail. Together, these capabilities capture the qualities that make modern cloud storage systems robust and highly scalable in those dynamic, failure-prone environments.

## 2. Overrall approach and System architecture

For an arbitrary sample request ,the system work as follows:

<img src="/Users/album/Desktop/未命名绘图.jpg" style="zoom: 33%;" />

### Decentralized structure

We ensure all nodes in the cluster are **functionally equivalent** and all have storage capacity, coordination capacity. Thus, any node that receives the request from the client can serve as a temporary "coordinator". If any node goes down, other nodes can immediately take over its coordinating role, this ensures a high availability and fault tolerance.

### Partitioner 

The partitioner will determine the which  N nodes to send replications.

### Quorum(R/W) and Hinted Handoff

Read operations (R) and write operations (W) must be confirmed by at least R and W nodes among the N replicas to be successful. This make the system can continue to operate even when some nodes go down.  If a replica node is unreachable, the coordinator **stores the write locally as a “hint”**, together with metadata indicating which node should eventually receive it. Later, when the failed replica recovers and becomes reachable again, the node holding the hint forwards the stored write to the recovered replica, completing the handoff.  These mechanisms supports eventual consistency and avoid the system blocking on a failures.

### The Gossip Moudle

The gossip module maintains a decentralized and continuously updated view of cluster membership. Each node periodically exchanges membership information with a small subset of other nodes, allowing changes such as  dynamical node joins, leaves, and failures to propagate gradually throughout the system.

## 3. Design and Inplementaion details

In our inplementation, we first build and validate a standalone single-node key-value store with a well-defined storage interface and gRPC service. Then we extend this foundation into a fully distributed, Dynamo-style system by adding cluster membership, data partitioning, replication, and fault-tolerance mechanisms.

### 3.1 Standalone implementation

The following functions form the critical execution path from gRPC requests to the underlying storage engine and define the fundamental semantics of key-value operations(Get, Put, Delete). Take Put as an example:

```go
func (server *Server) RawPut(ctx context.Context, req *rawkv.RawPutRequest) (*rawkv.RawPuttResponse, error) {
```

These funtions interact with the the following storage interface to perform operations:

```go
func (s *StandAloneStorage) Write(batch []storage.Modify) error {
func (s *StandAloneStorage) Reader() (storage.StorageReader, error) {
```

This separation cleanly decouples request handling logic from the underlying storage engine, enabling modularitycand extensibility.

### 3.2 Distributed system extension

#### a. Partitioner

When extending to a distributed system, we first need a partitioner to distribute the keys across the cluster and balance  the workload. Here, we use **consistent hashing**, since its ring structure can minimize data movement (only a few nodes need to change state) when nodes join or leave.

```go
type Partitioner struct {
	mu     sync.RWMutex
	ring   []vnode // sorted by Hash
	nodes  map[NodeID]struct{}
	vnodes int 
}
```

#### b. Communiaction protocol of metadata

To maintain the decentralized system, all nodes must rely on a communication protocol to exchange metadata without a central controller.  We choose a **Gossip-based communication protocol** since it provides a scalable and fault-tolerant mechanism for disseminating membership information, allowing nodes to gradually converge to a consistent view of the cluster through peer-to-peer exchanges. 

```go
func (s *NodeServer) PushGossip(ctx context.Context, st *pb.GossipState) (*pb.GossipResponse, error) {
```

The following is the **Metadata Sync** we defined for Gossip:

```protobuf
message NodeState {
    string node = 1;
    bool alive = 2;
    int64 heartbeat = 3;
}
message GossipState {
    repeated NodeState nodes = 1;
}
message GossipResponse {
    bool ok = 1;
}
```

- The heartbeat and alive fields allow nodes to detect whether a peer has joined, left, or failed.
- The repeated NodeState field enables batch synchronization of the states of all nodes in the cluster.

#### c. Additional fault tolerance mechanism

As mentioned in part2, we use **Quorum(R/W) and Hinted Handoff** for fault tolerance and eventual consistency. For example, if there are less than R replicas responded successfully:

```Go
if successes < c.R {
    return collected, fmt.Errorf("read quorum not reached: %d/%d", successes, c.R)
}
```

If some node is currently not reachable, it will be  stored as a hint in the buffer of other nodes:

```Go
c.handoff.StoreHint(node, &pb.Hint{
    Key:        key,
    Data:       vv,
    TargetNode: node,
})
```

The above two mechanism may lead to **multiple version** of same key on different replicas, so we need a **vector clock** to judge the relationship between different versions:

```Go
func CompareVC(a, b *pb.VectorClock) int {
func MergeVC(clocks ...*pb.VectorClock) *pb.VectorClock {
```

#### d. gRPC communication framework

Finally, with gRPC framework handling network transmission, serialization, and invocation mechanisms, we can  combine all the things above together to get a distributed key-value storage system. 

```protobuf
service DynamoRPC {
    rpc InternalPut(InternalPutRequest) returns (InternalPutResponse);
    rpc InternalGet(InternalGetRequest) returns (InternalGetResponse);
    rpc SendHints(HandoffBatch) returns (HandoffAck);
    rpc PushGossip(GossipState) returns (GossipResponse);
    rpc FetchHints(FetchHintsRequest) returns (HandoffBatch);
}
```

### 3.3 Deployment on the cloud

## 4. Evaluatoin through testing experiments



------

## References

1. DeCandia, G., Hastorun, D., Jampani, M., Kakulapati, G., Lakshman, A., Pilchin, A., ... & Vogels, W. 
(2007). Dynamo: Amazon's highly available key-value store. ACM SIGOPS operating systems review, 
41(6), 205-220.