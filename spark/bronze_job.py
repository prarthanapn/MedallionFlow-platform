import os
import sys
import json
from pyspark.sql import functions as F

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.delta_utils import get_spark_session, write_delta

# Paths
BRONZE_PATH = "/opt/airflow/data/delta/bronze"

def run(file_name="titanic.csv", run_id="unknown_run"):
    import time
    from datetime import datetime
    import traceback
    
    start_time = time.time()
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    spark = get_spark_session("BronzeJob")
    status = "Success"
    error_msg = None
    raw_count = 0
    schema_json = []
    
    try:
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
        
        schema_json = [{"name": field.name, "type": str(field.dataType)} for field in df.schema.fields]
    
        # Add ingest timestamp — only thing we add in Bronze
        df = df.withColumn("_ingest_timestamp", F.current_timestamp())
    
        print("Writing to Bronze Delta table...")
        write_delta(df, BRONZE_PATH)
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        print(f"Error in Bronze Job: {error_msg}")
        raise e
    finally:
        end_time = time.time()
        duration_sec = end_time - start_time
        
        # Save metrics for Dashboard
        metrics = {
            "layer": "Bronze",
            "source": file_name,
            "input_rows": raw_count if status == "Success" else 0,
            "output_rows": raw_count if status == "Success" else 0,
            "schema": schema_json if status == "Success" else [],
            "start_time": start_dt,
            "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration": f"{duration_sec:.2f} sec",
            "status": status,
            "job": "bronze_job",
            "output": "delta/bronze",
            "app_name": "BronzeJob"
        }
        if error_msg:
            metrics["exception"] = error_msg
        
        metadata_dir = "/opt/airflow/metadata"
        run_dir = os.path.join(metadata_dir, f"run_{run_id}")
        os.makedirs(metadata_dir, exist_ok=True)
        os.makedirs(run_dir, exist_ok=True)
        
        # Write history per run
        with open(os.path.join(run_dir, "bronze.json"), "w") as f:
            json.dump(metrics, f, indent=4)
            
        print("Bronze job complete!")
        spark.stop()

if __name__ == "__main__":
    file_to_process = "titanic.csv"
    run_id = "unknown_run"
    
    if len(sys.argv) > 1:
        file_to_process = sys.argv[1]
    if len(sys.argv) > 2:
        run_id = sys.argv[2]
        
    run(file_to_process, run_id)