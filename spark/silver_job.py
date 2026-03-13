import os
import sys
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, read_delta, write_delta

# Paths
BRONZE_PATH = "data/delta/bronze"
SILVER_PATH = "data/delta/silver"

def run():
    spark = get_spark_session("SilverJob")

    print("Reading from Bronze Delta table...")
    df = read_delta(spark, BRONZE_PATH)
    print(f"Bronze row count: {df.count()}")

    # Step 1 — Remove duplicates
    df = df.dropDuplicates()
    print(f"After dedup row count: {df.count()}")

    # Step 2 — Fill nulls
    # Numbers get 0, strings get "unknown"
    df = df.fillna({
        "Age":      0.0,
        "Fare":     0.0,
        "Cabin":    "unknown",
        "Embarked": "unknown"
    })

    # Step 3 — Add processed timestamp
    df = df.withColumn("_processed_at", F.current_timestamp())

    print("Schema after cleaning:")
    df.printSchema()

    print("Writing to Silver Delta table...")
    write_delta(df, SILVER_PATH)

    print("Silver job complete!")
    spark.stop()

if __name__ == "__main__":
    run()