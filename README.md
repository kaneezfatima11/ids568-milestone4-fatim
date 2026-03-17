# IDS 568 Milestone 4

## Author
Fatima Kaneez

---

## Project Overview
This project implements a distributed feature engineering pipeline using PySpark and compares it with a local Python implementation. The objective is to evaluate scalability, performance, and trade-offs between local and distributed data processing approaches.

---

## Project Structure
- generate_data.py → Generates synthetic dataset with reproducible randomness
- pipeline_local.py → Local (single-machine) pipeline
- pipeline.py → Distributed PySpark pipeline
- REPORT.md → Performance and analysis

---

## Setup Instructions

### Create Virtual Environment
python3 -m venv venv
source venv/bin/activate

### Install Dependencies
pip install pyspark

### Set Java
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

---

## Run Instructions

### Generate Data
python generate_data.py --rows 100000 --output data/ --seed 42

### Local Pipeline
python pipeline_local.py --input data/ --output local_output/

### Distributed Pipeline
python pipeline.py --input data/ --output spark_output/

---

## Reproducibility
- Uses fixed random seed
- Same input → same output

---

## Performance Summary

| Dataset | Local | Spark |
|--------|------|------|
| 100K   | 0.3s | 12s  |
| 1M     | 2.8s | 15s  |
| 5M     | 13.7s| 23.8s|

---

## Analysis
- Local faster for small data
- Spark has overhead
- Distributed helps at scale

---

## Conclusion
Distributed systems are useful for large-scale processing but introduce overhead for smaller datasets.
