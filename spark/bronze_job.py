import os
import sys
import json
from pyspark.sql import functions as F

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, write_delta

# Paths
BRONZE_PATH = "/opt/airflow/data/delta/bronze"

def run(file_name="titanic.csv"):
    spark = get_spark_session("BronzeJob")

    input_path = f"/opt/airflow/data/input/{file_name}"
    print(f"Reading raw data from: {input_path}")

    if file_name.lower().endswith(".json"):
        print("Detected JSON format")
        df = spark.read \
            .option("multiline", "true") \
            .option("inferSchema", "true") \
            .json(input_path)
    else:  # Default to CSV
        print("Detected CSV format")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)

    raw_count = df.count()
    print(f"Raw row count: {raw_count}")
    df.printSchema()

    # Add ingest timestamp — only thing we add in Bronze
    df = df.withColumn("_ingest_timestamp", F.current_timestamp())

    print("Writing to Bronze Delta table...")
    write_delta(df, BRONZE_PATH)

    # Save metrics for Dashboard
    metrics = {"total_records": raw_count}
    os.makedirs("data/delta/metrics", exist_ok=True)
    with open("data/delta/metrics/bronze_metrics.json", "w") as f:
        json.dump(metrics, f)

    print("Bronze job complete!")
    spark.stop()

if __name__ == "__main__":
    # The filename is passed as a command-line argument from the Airflow BashOperator
    if len(sys.argv) > 1:
        file_to_process = sys.argv[1]
        run(file_to_process)
    else:
        print("No file name provided, using default 'titanic.csv'")
        run()