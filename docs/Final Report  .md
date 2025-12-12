# CSC4160 Final Report : A Gossip-based Distributed Key-Value Storage System

### Team members

Tingxuan Liu 123090363@link.cuhk.edu.cn

Yuxuan Liu 123090377@link.cuhk.edu.cn

### Demo video link:

------

## 1. Motivation

Distributed key-value stores play a central role in modern cloud and web-scale systems, where applications must handle massive request volumes, tolerate failures, and scale seamlessly with workload growth. Among existing designs, **Amazon’s Dynamo** demonstrates exceptional performance in achieving high availability, partition tolerance, and elastic scalability in real production environments (DeCandia et al., 2007). Its success highlights the importance of decentralized architectures that avoid single points of failure and continue operating effectively under node outages or network partitions.

**Motivated by these insights**, our project aims to build a simplified system that captures the essential characteristics of Dynamo. It provides horizontal scalability through partitioned data storage and maintains high availability by storing multiple copies of data. The system also offers eventual consistency and adapts to workload changes with dynamic node join and leave. Moreover, it also manages cluster membership and preserves write availability even when nodes temporarily fail. Together, these capabilities capture the qualities that make modern cloud storage systems robust and highly scalable in those dynamic, failure-prone environments.

## 2. Overrall approach and System architecture



## 3. Design and Inplementaion details



## 4. Evaluatoin through testing experiments



------

## References

1. DeCandia, G., Hastorun, D., Jampani, M., Kakulapati, G., Lakshman, A., Pilchin, A., ... & Vogels, W. 
(2007). Dynamo: Amazon's highly available key-value store. ACM SIGOPS operating systems review, 
41(6), 205-220.