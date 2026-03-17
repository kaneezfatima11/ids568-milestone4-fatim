# Milestone 4 Report

## Author
Fatima Kaneez

---

## Overview
This project implements a distributed feature engineering pipeline using PySpark and compares it with a local Python implementation.

---

## Experimental Setup
- Data generated using synthetic generator
- Seeded randomness for reproducibility
- Tested on increasing dataset sizes

---

## Performance Comparison

| Dataset Size | Local Runtime (sec) | Spark Runtime (sec) |
|-------------|--------------------|-------------------|
| 100K        | 0.3                | 12                |
| 1M          | 2.8                | 15                |
| 5M          | 13.7               | 23.8              |

---

## Key Observations

- Local execution is faster for small datasets due to lower overhead
- Spark introduces initialization cost (JVM + scheduling)
- Distributed processing becomes beneficial only at larger scales
- Single-node Spark limits true parallel advantage

---

## Reliability & Cost Analysis

### Bottlenecks Identified
- Spark initialization overhead (JVM startup)
- Disk I/O during CSV read/write operations
- Limited parallelism due to single-node setup

### Cost Implications
- Distributed systems require more computational resources (CPU, memory)
- Running Spark clusters in production environments incurs infrastructure costs
- Local processing is more cost-efficient for small datasets

### Reliability Considerations
- Spark provides fault tolerance through distributed processing
- Local pipelines are simpler but lack scalability and fault tolerance
- Distributed systems are more suitable for large-scale, production-grade pipelines

### Costs and Limitations
- Higher startup latency
- Increased resource usage
- Requires cluster infrastructure for full benefit

---

## Architecture Analysis

### Reliability Trade-offs
- Spill-to-disk: Spark automatically spills intermediate data to disk if memory is insufficient, increasing reliability but adding I/O overhead
- Speculative Execution: Straggler tasks can be re-run on other executors, improving reliability at the cost of extra resources

### When Distributed Processing Provides Benefits vs Overhead
- Useful for large datasets where parallelism outweighs Spark initialization overhead
- Not advantageous for small datasets; local execution is faster

### Cost Implications of Scaling
- Compute: More executors → higher CPU cost
- Memory: Insufficient memory triggers spill-to-disk
- Storage & Network: Distributed jobs may shuffle data, increasing I/O and network usage

### Recommendations for Production Deployment
- Use Spark only for sufficiently large datasets
- Ensure cluster has enough CPU and RAM to avoid spilling
- Enable speculative execution for reliability
- Local pipelines remain more cost-effective for small workloads

---

## Conclusion

Distributed processing is not always faster. It is most effective when:
- Data size is very large
- Multiple workers are available
- Parallelism outweighs overhead
