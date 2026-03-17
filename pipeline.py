from pyspark.sql import SparkSession
from pyspark.sql.functions import col



def process_data(input_dir, output_dir):
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()

    input_file = input_dir + "/data.csv"

    df = spark.read.csv(input_file, header=True, inferSchema=True)

    df = df.withColumn("value_sum", col("value1") + col("value2"))

    df.write.mode("overwrite").csv(output_dir, header=True)

    spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)

    args = parser.parse_args()

    process_data(args.input, args.output)
