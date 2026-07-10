import os
import sys
import json
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, read_delta, write_delta

# Paths
BRONZE_PATH = "/opt/airflow/data/delta/bronze"
SILVER_PATH = "/opt/airflow/data/delta/silver"

def run(run_id="unknown_run"):
    import time
    from datetime import datetime
    import traceback
    
    start_time = time.time()
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    spark = get_spark_session("SilverJob")
    status = "Success"
    error_msg = None
    
    bronze_count = 0
    dedup_count = 0
    duplicates_removed = 0
    total_nulls_filled = 0
    transformations = ["Remove index columns", "Deduplicate rows", "Fill null values", "Add _processed_at timestamp"]
    
    try:
        print("Reading from Bronze Delta table...")
        df = read_delta(spark, BRONZE_PATH)
        bronze_count = df.count()
        print(f"Bronze row count: {bronze_count}")
    
        # Gracefully exit if there is no new data to process
        if df.isEmpty():
            print("Bronze table is empty. No new data to process. Exiting.")
            spark.stop()
            return
    
        # Remove artifact index columns if they exist (allows deduplication to work)
        # These are common names for index columns coming from Pandas/CSV
        for col_name in ["_c0", "Unnamed: 0", "index"]:
            if col_name in df.columns:
                df = df.drop(col_name)
    
        # Step 1 — Remove duplicates
        df = df.dropDuplicates()
        dedup_count = df.count()
        duplicates_removed = bronze_count - dedup_count
        print(f"After dedup row count: {dedup_count}")
    
        # Calculate nulls before filling
        # Calculate nulls for ALL columns dynamically
        cols_check = df.columns
        
        if cols_check:
            null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols_check]).collect()[0]
            total_nulls_filled = sum(null_counts[c] for c in cols_check)
    
        # Step 2 — Fill nulls
        # Step 2 — Fill nulls dynamically based on column type
        fill_map = {}
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                fill_map[col_name] = "unknown"
            elif dtype in ("int", "double", "float", "long", "bigint"):
                fill_map[col_name] = 0
        
        if fill_map:
            df = df.fillna(fill_map)
    
        # Step 3 — Add processed timestamp
        df = df.withColumn("_processed_at", F.current_timestamp())
    
        print("Schema after cleaning:")
        df.printSchema()
    
        print("Writing to Silver Delta table...")
        write_delta(df, SILVER_PATH)
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        print(f"Error in Silver Job: {error_msg}")
        raise e
    finally:
        end_time = time.time()
        duration_sec = end_time - start_time
        
        # Save metrics for Dashboard
        metrics = {
            "layer": "Silver",
            "rows_before": bronze_count,
            "rows_after": dedup_count if status == "Success" else 0,
            "duplicates_removed": duplicates_removed,
            "nulls_filled": total_nulls_filled,
            "transformations": transformations,
            "start_time": start_dt,
            "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration": f"{duration_sec:.2f} sec",
            "status": status,
            "job": "silver_job",
            "output": "delta/silver",
            "app_name": "SilverJob"
        }
        if error_msg:
            metrics["exception"] = error_msg
        
        metadata_dir = "/opt/airflow/metadata"
        run_dir = os.path.join(metadata_dir, f"run_{run_id}")
        os.makedirs(metadata_dir, exist_ok=True)
        os.makedirs(run_dir, exist_ok=True)
        
        with open(os.path.join(run_dir, "silver.json"), "w") as f:
            json.dump(metrics, f, indent=4)
            
        print("Silver job complete!")
        spark.stop()

if __name__ == "__main__":
    run_id = "unknown_run"
    if len(sys.argv) > 1:
        run_id = sys.argv[1]
    run(run_id)