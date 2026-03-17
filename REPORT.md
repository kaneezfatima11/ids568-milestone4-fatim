# Milestone 4 Report

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
|-------------|--------------------|----------------------|
| 100K        | 0.3                | 12                   |
| 1M          | 2.8                | 15                   |
| 5M          | 13.7               | 23.8                 |

---

## Key Observations

- Local execution is faster for small datasets due to lower overhead
- Spark introduces initialization and scheduling overhead
- Distributed processing becomes beneficial only at larger scales
- Current setup uses a single-node environment, limiting Spark advantages

---

## Trade-offs

### Benefits of Distributed Processing
- Scales to large datasets
- Handles parallel processing efficiently
- Suitable for production-scale pipelines

### Costs and Limitations
- Higher startup latency
- Increased resource usage
- Requires cluster infrastructure for full benefit

---

## Conclusion

Distributed processing is not always faster. It is most effective when:
- Data size is very large
- Multiple workers are available
- Parallelism outweighs overhead
