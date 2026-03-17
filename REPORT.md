# IDS568 Milestone 4 — Distributed Feature Engineering

## Distributed Transformations
The pipeline computes a new column `value_sum` = `value1 + value2`. Implemented using PySpark DataFrame operations to leverage distributed execution. Transformations are deterministic and reproducible — verified by comparing multiple runs using `diff run1/data.csv run2/data.csv`.

## Reproducible Execution
All steps can be reproduced with the following setup:

```
git clone https://github.com/kaneezfatima11/ids568-milestone4-fatim.git
cd ids568-milestone4-fatim
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python generate_data.py --rows 1000 --seed 42 --output test_data/
time python pipeline.py --input test_data/ --output spark_output/
```

Seeded randomness ensures identical datasets for repeated runs.

## Performance Metrics
**Single-node Spark execution:**

| Dataset       | Executors | Default Parallelism | Runtime (real) |
|---------------|-----------|-------------------|----------------|
| large_data    | 1         | 4                 | 0m11.580s      |

**Notes:** Distributed metrics such as shuffle read/write, per-stage task execution, and multi-node parallelism are not available on the single-node VM. Stage metrics would be meaningful on a multi-node cluster; here, shuffle volume is negligible.

## Reliability & Cost Analysis
**Bottlenecks Identified:** JVM startup overhead, disk I/O during CSV read/write, limited parallelism on single-node setup.

**Cost Implications:** Distributed systems require additional compute, memory, and cluster resources. Local single-node execution is cheaper for small datasets.

**Reliability Considerations:** Spark provides fault tolerance and recovery in distributed clusters. Single-node execution is simple but lacks scalability and fault tolerance. Monitoring is recommended for large datasets.

## Code Structure & Clarity
`pipeline.py` and `pipeline_local.py` are modular and documented.
 Clear separation of concerns: data generation, transformation, distributed vs. local execution.
 Docstrings explain each function’s purpose.

## Operational Awareness
 Single-node execution is suitable for small datasets or testing.
 Production pipelines require cluster setup, monitoring Spark UI, and JVM metrics.
 Consider spill-to-disk, memory limits, and task scheduling in distributed deployments.

## Summary
 The Milestone 4 pipeline produces correct, reproducible, deterministic transformations.
 Runtime metrics and executor info are captured for performance analysis.
 Stage and shuffle metrics are acknowledged as unavailable for single-node VM.
 Bottlenecks, reliability, and cost trade-offs are clearly discussed.

