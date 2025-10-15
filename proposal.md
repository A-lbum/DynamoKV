# CSC4160 Course Project: A Gossip-based Distributed KVStore System

##### Team Members:

> Tingxuan Liu (123090363) 123090363@link.cuhk.edu.cn 
>
> Yuxuan Liu (123090377) 123090377@link.cuhk.edu.cn

### Background Information

Distributed key-value stores are a fundamental component of modern cloud and data-intensive systems. They provide scalable, fault-tolerant, and low-latency data access for large-scale applications such as web services, machine learning platforms, and cloud control planes. Classical systems such as Amazon’s Dynamo (DeCandia et al., SOSP 2007)**, **Cassandra, and Etcd demonstrate different trade-offs between consistency, availability, and partition tolerance (CAP theorem).

Among them, Dynamo stands out as a pioneering design that prioritizes high availability and eventual consistency through techniques such as consistent hashing**, **replication**, **vector clocks for versioning**, **quorum-based read/write operations, and gossip-based membership management. Dynamo’s decentralized and leaderless architecture has profoundly influenced subsequent distributed storage systems, forming the conceptual foundation for many modern NoSQL databases and cloud-native infrastructures.

### Topic Description

In this project, we aim to design and implement a Dynamo-inspired distributed key-value store that emphasizes scalability, availability, and elasticity. Our KV store will feature partitioned data storage (consistent hashing), replication for fault tolerance, and quorum-based read/write operations to achieve eventual consistency without relying on a central leader. To further enhance adaptability, the system will support dynamic node join and leave operations (elastic scaling) while maintaining consistent metadata through gossip and hinted handoff mechanisms. 

The key features of our system and the corresponding design objectives are summarized as follows:

- Partitioned data storage (consistent hashing) → implements data partitioning and horizontal scalability
- Replication → implements fault tolerance and high availability
- Quorum-based read/write operations → implements eventual consistency
- Dynamic node join and leave operations (elastic scaling) → implements system elasticity and scaling
- Gossip and hinted handoff mechanisms → implements consistent metadata maintenance

The final prototype will demonstrate the system’s behavior under node failure, recovery, and workload scaling scenarios.
