# Milestone 4: Distributed & Streaming Pipeline
**Course**: IDS568 - MLOps | **Author**: Fatima | **NetID**: kfati3

## Overview
This project implements a distributed feature engineering pipeline using PySpark
for batch processing of 10M+ synthetic financial transactions, with an optional
streaming pipeline using a Python queue-based producer/consumer system.

## Repository Structure
```
ids568-milestone4-fatim/
├── pipeline.py          # Distributed feature engineering (PySpark)
├── generate_data.py     # Synthetic data generator (10M+ rows)
├── producer.py          # Streaming event producer
├── consumer.py          # Streaming event consumer with windowing
├── README.md            # This file
├── REPORT.md            # Performance analysis
├── STREAMING_REPORT.md  # Streaming analysis
└── requirements.txt     # Python dependencies
```

## Requirements
- Python 3.11+
- Java 17+ (required for PySpark)
- 8GB+ RAM recommended

## Setup Instructions

### 1. Clone the repository
```bash
git clone git@github.com:kaneezfatima11/ids568-milestone4-fatim.git
cd ids568-milestone4-fatim
```

### 2. Install dependencies
```bash
pip3 install -r requirements.txt --break-system-packages
```

### 3. Verify installation
```bash
python3 -c "import pyspark; print('PySpark:', pyspark.__version__)"
java -version
```

## Running the Pipeline

### Step 1: Generate synthetic data

**Small test (1,000 rows):**
```bash
python3 generate_data.py --rows 1000 --seed 42 --output test_data/
```

**Full dataset (10M rows):**
```bash
python3 generate_data.py --rows 10000000 --seed 42 --output data/
```

### Step 2: Run feature engineering

**Local mode (1 core baseline):**
```bash
python3 pipeline.py --input data/ --output output_local/ --mode local --partitions 4
```

**Distributed mode (4 cores):**
```bash
python3 pipeline.py --input data/ --output output_dist/ --mode distributed --cores 4 --partitions 16
```

### Step 3: Verify reproducibility
```bash
python3 generate_data.py --rows 100 --seed 42 --output run1/
python3 generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ && echo "Reproducible!" || echo "Not reproducible"
```

## Running the Streaming Pipeline (Bonus)

Run producer and consumer together:
```bash
# Terminal 1 - Start consumer first
python3 -c "
import threading
from producer import event_queue
from consumer import consume
from producer import produce

t1 = threading.Thread(target=produce, args=(100, 60, 'steady'))
t2 = threading.Thread(target=consume, args=(10,))
t1.start(); t2.start()
t1.join(); t2.join()
"
```

**Test different load levels:**
```bash
# Low load - 100 msg/s steady
python3 -c "
import threading
from producer import event_queue, produce
from consumer import consume
threading.Thread(target=produce, args=(100, 30, 'steady')).start()
consume(10)
"

# Medium load - 1000 msg/s steady
python3 -c "
import threading
from producer import event_queue, produce
from consumer import consume
threading.Thread(target=produce, args=(1000, 30, 'steady')).start()
consume(10)
"

# Burst pattern
python3 -c "
import threading
from producer import event_queue, produce
from consumer import consume
threading.Thread(target=produce, args=(500, 60, 'burst')).start()
consume(10)
"
```

## Reproducibility
All results use fixed random seed `42`. To reproduce:
```bash
python3 generate_data.py --rows 10000000 --seed 42 --output data/
python3 pipeline.py --input data/ --output output_dist/ --mode distributed --cores 4 --partitions 16
```

## Key Design Decisions
- **Partitioning**: 16 partitions for distributed mode (4x core count) balances parallelism and shuffle overhead
- **Chunk size**: 500K rows per parquet file for efficient I/O
- **Window size**: 10s tumbling windows for streaming aggregations
- **Seed**: Fixed seed `42` throughout for full reproducibility