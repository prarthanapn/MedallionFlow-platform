import os
import sys
import json
import re
from pyspark.sql import functions as F

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, write_delta

# Paths
BRONZE_PATH = "/opt/airflow/data/delta/bronze"


def sanitize_column_name(name: str) -> str:
    """Convert arbitrary headers to Delta-safe snake_case names."""
    cleaned = re.sub(r"[^0-9A-Za-z_]+", "_", name.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        cleaned = "col"
    if cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    return cleaned


def normalize_columns(df):
    """Sanitize column names and keep them unique after normalization."""
    used = {}
    new_cols = []

    for original in df.columns:
        base = sanitize_column_name(original)
        count = used.get(base, 0)
        if count == 0:
            final = base
        else:
            final = f"{base}_{count}"
        used[base] = count + 1
        new_cols.append(final)

    for old, new in zip(df.columns, new_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)

    return df

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

    df = normalize_columns(df)
    print("Schema after column normalization:")
    df.printSchema()

    # Add ingest timestamp — only thing we add in Bronze
    df = df.withColumn("_ingest_timestamp", F.current_timestamp())

    print("Writing to Bronze Delta table...")
    write_delta(df, BRONZE_PATH)

    # Save metrics for Dashboard
    metrics = {"total_records": raw_count}
    os.makedirs("/opt/airflow/data/delta/metrics", exist_ok=True)
    with open("/opt/airflow/data/delta/metrics/bronze_metrics.json", "w") as f:
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