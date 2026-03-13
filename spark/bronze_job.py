import os
import sys
from pyspark.sql import functions as F

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, write_delta

# Paths
INPUT_PATH  = "data/input/titanic.csv"
BRONZE_PATH = "data/delta/bronze"

def run():
    spark = get_spark_session("BronzeJob")

    print("Reading raw CSV...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(INPUT_PATH)

    print(f"Raw row count: {df.count()}")
    df.printSchema()

    # Add ingest timestamp — only thing we add in Bronze
    df = df.withColumn("_ingest_timestamp", F.current_timestamp())

    print("Writing to Bronze Delta table...")
    write_delta(df, BRONZE_PATH)

    print("Bronze job complete!")
    spark.stop()

if __name__ == "__main__":
    run()