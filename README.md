# IDS568 Milestone 4 — Distributed Feature Engineering

## Overview
This project implements a PySpark pipeline to compute feature transformations on CSV datasets. The main output is a new column `value_sum` = `value1 + value2`. The pipeline is designed for distributed feature engineering, but it runs on a single-node VM for this submission.

## Setup
Clone the repository, create a virtual environment, and install dependencies:

```bash
git clone https://github.com/kaneezfatima11/ids568-milestone4-fatim.git
cd ids568-milestone4-fatim
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Data Generation
Generate a reproducible dataset with seeded randomness:

```bash
python generate_data.py --rows 1000 --seed 42 --output test_data/
```

## Pipeline Execution

### Single-node Spark
Run the main PySpark pipeline:

```bash
time python pipeline.py --input test_data/ --output spark_output/
```

### Local Python pipeline
Run the local Python version for comparison:

```bash
time python pipeline_local.py --input test_data/ --output local_output/
```

## Performance Summary — Local vs Spark

| Dataset   | Local (Python) | Spark (PySpark) |
|-----------|----------------|----------------|
| 100K      | 0.3s           | 12s            |
| 1M        | 2.8s           | 15s            |
| 5M        | 13.7s          | 24s            |

## Performance Summary — Executor Info

| Dataset       | Executors | Default Parallelism | Runtime (real) |
|---------------|-----------|-------------------|----------------|
| large_data    | 1         | 4                 | 0m11.580s      |

**Notes:**

- Runtime measured using `time` command in terminal.
- Shuffle/stage metrics are **not available** on single-node VM.
- These results illustrate scaling behavior, even though Spark is limited by single-node setup.

## Notes
- Seeded randomness ensures reproducible results.
- Output CSV contains the correct `value_sum` column, verified by comparing repeated runs:

```bash
diff run1/data.csv run2/data.csv
```

## Output
Processed CSV files are saved in the output folders:

- `spark_output/` for distributed PySpark pipeline  
- `local_output/` for local Python pipeline

## Reproducibility
Anyone can follow these instructions to reproduce the results exactly, including generating the same datasets and verifying outputs.
