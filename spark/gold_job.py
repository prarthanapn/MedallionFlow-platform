import os
import sys
import json
from pyspark.sql import functions as F

# Add project root to path for delta_utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from spark.delta_utils import get_spark_session, read_delta, write_delta

# Absolute paths for Docker
SILVER_PATH = "/opt/airflow/data/delta/silver"
GOLD_PATH   = "/opt/airflow/data/delta/gold"
METRICS_PATH = "/opt/airflow/data/delta/metrics"

def run():
    spark = get_spark_session("GoldJob")

    # Read Silver table
    print("Reading Silver Delta table...")
    if not os.path.exists(SILVER_PATH):
        print(f"Silver path {SILVER_PATH} does not exist. Exiting Gold job.")
        spark.stop()
        return

    df = read_delta(spark, SILVER_PATH)
    silver_count = df.count()
    print(f"Silver row count: {silver_count}")

    if silver_count == 0:
        print("Silver table is empty. Skipping Gold job.")
        spark.stop()
        return

    # Optional quality checks (warnings instead of assert)
    if "PassengerId" in df.columns:
        null_passenger_ids = df.filter(F.col("PassengerId").isNull()).count()
        if null_passenger_ids > 0:
            print(f"WARNING: {null_passenger_ids} null PassengerIds, proceeding anyway.")

    # Add Gold timestamp
    df = df.withColumn("gold_loaded_at", F.current_timestamp())

    # Add SurvivalStatus if Survived column exists
    if "Survived" in df.columns:
        df = df.withColumn(
            "SurvivalStatus",
            F.when(F.col("Survived") == 1, "Survived").otherwise("Did not survive")
        )

    # Determine partition column (if exists)
    partition_col = "Pclass" if "Pclass" in df.columns else None

    print(f"Writing Gold Delta table at {GOLD_PATH} (partitioned by {partition_col})...")
    if partition_col:
        write_delta(df, GOLD_PATH, partition_by=partition_col, merge_schema=True)
    else:
        write_delta(df, GOLD_PATH, merge_schema=True)

    # Save metrics
    os.makedirs(METRICS_PATH, exist_ok=True)
    metrics = {"total_records": df.count()}
    with open(os.path.join(METRICS_PATH, "gold_metrics.json"), "w") as f:
        json.dump(metrics, f)

    print("Gold job complete!")
    spark.stop()


if __name__ == "__main__":
    run()