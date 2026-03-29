"""
pipeline.py - Distributed Feature Engineering Pipeline
Implements PySpark-based distributed feature engineering on financial
transaction data with local vs distributed performance comparison.
"""

import argparse
import logging
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def create_spark_session(mode: str, cores: int = 4) -> SparkSession:
    """Create a SparkSession configured for local or distributed mode."""
    if mode == "local":
        master = "local[1]"
        log.info("Starting Spark in LOCAL mode (1 core)")
    else:
        master = f"local[{cores}]"
        log.info(f"Starting Spark in DISTRIBUTED mode ({cores} cores)")
    return (
        SparkSession.builder
        .master(master)
        .appName("FinancialFeatureEngineering")
        .config("spark.sql.shuffle.partitions", str(cores * 4))
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def run_feature_engineering(spark: SparkSession, input_dir: str, output_dir: str, partitions: int):
    """Run all feature engineering transformations."""
    log.info(f"Reading data from {input_dir}")
    df = spark.read.parquet(input_dir).repartition(partitions)
    total_rows = df.count()
    log.info(f"Loaded {total_rows:,} rows across {partitions} partitions")

    log.info("Computing amount-based features...")
    df = df.withColumn("amount_log", F.log1p(F.col("amount"))) \
           .withColumn("amount_bin", F.when(F.col("amount") < 10, "micro")
                                      .when(F.col("amount") < 100, "small")
                                      .when(F.col("amount") < 1000, "medium")
                                      .otherwise("large"))

    log.info("Computing user-level window features...")
    user_window = Window.partitionBy("user_id")
    df = df.withColumn("user_total_spend", F.sum("amount").over(user_window)) \
           .withColumn("user_avg_spend", F.avg("amount").over(user_window)) \
           .withColumn("user_txn_count", F.count("transaction_id").over(user_window)) \
           .withColumn("user_max_spend", F.max("amount").over(user_window)) \
           .withColumn("spend_vs_avg", F.col("amount") / (F.col("user_avg_spend") + 0.001))

    log.info("Computing merchant-level features...")
    merchant_window = Window.partitionBy("merchant_id")
    df = df.withColumn("merchant_txn_count", F.count("transaction_id").over(merchant_window)) \
           .withColumn("merchant_avg_amount", F.avg("amount").over(merchant_window)) \
           .withColumn("merchant_fraud_rate",
                       F.avg(F.col("is_fraud").cast("double")).over(merchant_window))

    log.info("Computing time-based features...")
    df = df.withColumn("is_weekend", (F.col("day_of_week") >= 5).cast("int")) \
           .withColumn("is_night", ((F.col("hour_of_day") >= 22) |
                                    (F.col("hour_of_day") <= 6)).cast("int")) \
           .withColumn("time_risk_score",
                       F.col("is_night") * 0.4 + F.col("is_weekend") * 0.2)

    log.info("Computing fraud risk score...")
    df = df.withColumn("fraud_risk_score",
                       (F.col("spend_vs_avg") * 0.3 +
                        F.col("merchant_fraud_rate") * 0.4 +
                        F.col("time_risk_score") * 0.3))

    log.info(f"Writing features to {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    df.write.mode("overwrite").parquet(output_dir)
    log.info("Feature engineering complete!")
    return total_rows


def main():
    parser = argparse.ArgumentParser(description="Financial transaction feature engineering")
    parser.add_argument("--input", type=str, default="data/")
    parser.add_argument("--output", type=str, default="output/")
    parser.add_argument("--mode", type=str, choices=["local", "distributed"], default="distributed")
    parser.add_argument("--cores", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=16)
    args = parser.parse_args()

    spark = create_spark_session(args.mode, args.cores)
    spark.sparkContext.setLogLevel("WARN")

    start = time.time()
    rows = run_feature_engineering(spark, args.input, args.output, args.partitions)
    elapsed = time.time() - start

    log.info("=" * 60)
    log.info(f"MODE      : {args.mode}")
    log.info(f"ROWS      : {rows:,}")
    log.info(f"CORES     : {args.cores if args.mode == 'distributed' else 1}")
    log.info(f"PARTITIONS: {args.partitions}")
    log.info(f"RUNTIME   : {elapsed:.2f} seconds")
    log.info("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()