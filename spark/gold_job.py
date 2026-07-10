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

def run(run_id="unknown_run"):
    import time
    from datetime import datetime
    import traceback
    
    start_time = time.time()
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    spark = get_spark_session("GoldJob")
    status = "Success"
    error_msg = None
    output_datasets = []
    aggregation_count = 0
    kpis_generated = ["Survival Status Added", "Partitioned by Pclass (if exists)"]
    
    try:
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
            aggregation_count += 1
    
        # Determine partition column (if exists)
        partition_col = "Pclass" if "Pclass" in df.columns else None
    
        print(f"Writing Gold Delta table at {GOLD_PATH} (partitioned by {partition_col})...")
        if partition_col:
            write_delta(df, GOLD_PATH, partition_by=partition_col, merge_schema=True)
            output_datasets.append(f"gold_partitioned_by_{partition_col}")
        else:
            write_delta(df, GOLD_PATH, merge_schema=True)
            output_datasets.append("gold_unpartitioned")
            
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        print(f"Error in Gold Job: {error_msg}")
        raise e
    finally:
        end_time = time.time()
        duration_sec = end_time - start_time
        
        # Save metrics
        metrics = {
            "layer": "Gold",
            "kpis_generated": kpis_generated,
            "aggregation_count": aggregation_count,
            "output_datasets": output_datasets,
            "start_time": start_dt,
            "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration": f"{duration_sec:.2f} sec",
            "status": status,
            "job": "gold_job",
            "output": "delta/gold",
            "app_name": "GoldJob"
        }
        if error_msg:
            metrics["exception"] = error_msg
        
        metadata_dir = "/opt/airflow/metadata"
        run_dir = os.path.join(metadata_dir, f"run_{run_id}")
        os.makedirs(metadata_dir, exist_ok=True)
        os.makedirs(run_dir, exist_ok=True)
        
        with open(os.path.join(run_dir, "gold.json"), "w") as f:
            json.dump(metrics, f, indent=4)
            
        print("Gold job complete!")
        spark.stop()


if __name__ == "__main__":
    run_id = "unknown_run"
    if len(sys.argv) > 1:
        run_id = sys.argv[1]
    run(run_id)