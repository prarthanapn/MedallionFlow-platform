import os
import sys
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, read_delta, write_delta

# Paths
SILVER_PATH = "data/delta/silver"
GOLD_PATH   = "data/delta/gold"

def run():
    spark = get_spark_session("GoldJob")

    print("Reading from Silver Delta table...")
    df = read_delta(spark, SILVER_PATH)
    row_count = df.count()
    print(f"Silver row count: {row_count}")

    # ── Quality checks ──────────────────────────────
    # Fail loudly if data looks wrong — don't silently write bad data
    assert row_count > 0, "QUALITY CHECK FAILED: No rows in Silver!"

    null_passenger_ids = df.filter(F.col("PassengerId").isNull()).count()
    assert null_passenger_ids == 0, f"QUALITY CHECK FAILED: {null_passenger_ids} null PassengerIds!"

    print("Quality checks passed ✓")

    # ── Add gold timestamp ───────────────────────────
    df = df.withColumn("_gold_loaded_at", F.current_timestamp())

    # ── Add survival label (bonus — makes Gold more useful) ─
    df = df.withColumn("SurvivalStatus",
        F.when(F.col("Survived") == 1, "Survived")
         .otherwise("Did not survive")
    )

    print("Writing to Gold Delta table (partitioned by Pclass)...")
    write_delta(df, GOLD_PATH, partition_by="Pclass", merge_schema=True)

    # ── Show Delta history ───────────────────────────
    print("\n=== DELTA HISTORY (audit trail) ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{os.path.abspath(GOLD_PATH)}`").show(5, truncate=False)

    # ── Preview the Gold table ───────────────────────
    print("\n=== GOLD TABLE PREVIEW ===")
    df_gold = read_delta(spark, GOLD_PATH)
    df_gold.select("PassengerId", "Pclass", "Sex", "Age",
                   "SurvivalStatus", "_gold_loaded_at").show(10)

    print(f"\nGold row count: {df_gold.count()}")
    print("Gold job complete!")
    demo_time_travel(spark)
    spark.stop()

def demo_time_travel(spark):
    print("\n=== TIME TRAVEL DEMO ===")

    # Version 0 — first ever write
    df_v0 = spark.read.format("delta") \
        .option("versionAsOf", 0) \
        .load(GOLD_PATH)
    print(f"Version 0 row count: {df_v0.count()}")

    # Latest version — current state
    df_latest = read_delta(spark, GOLD_PATH)
    print(f"Latest version row count: {df_latest.count()}")

    # Show full history
    print("\nFull Delta History:")
    spark.sql(f"DESCRIBE HISTORY delta.`{os.path.abspath(GOLD_PATH)}`") \
        .select("version", "timestamp", "operation", "operationParameters") \
        .show(10, truncate=False)

    print("Time travel works! We can query ANY past version of this table.")

if __name__ == "__main__":
    run()