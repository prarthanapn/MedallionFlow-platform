import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


QUEUE_PATH = "/opt/airflow/data/input/upload_queue.json"
SCHEDULED_DAG_ID = "scheduled_data_pipeline"


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_queue() -> list:
    if not os.path.exists(QUEUE_PATH):
        return []

    try:
        with open(QUEUE_PATH, "r", encoding="utf-8") as file_handle:
            data = json.load(file_handle)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_queue(entries: list) -> None:
    os.makedirs(os.path.dirname(QUEUE_PATH), exist_ok=True)
    with open(QUEUE_PATH, "w", encoding="utf-8") as file_handle:
        json.dump(entries, file_handle, indent=2)


def reserve_next_file(**context) -> str:
    entries = load_queue()
    run_id = context["run_id"]
    timestamp = datetime.utcnow().isoformat()

    for entry in entries:
        if entry.get("target_dag") != SCHEDULED_DAG_ID:
            continue

        if entry.get("reserved_run_id") == run_id and entry.get("status") in {"queued", "reserved"}:
            context["ti"].xcom_push(key="queued_file_name", value=entry["file_name"])
            return "ingest_to_bronze"

    for entry in entries:
        if entry.get("target_dag") == SCHEDULED_DAG_ID and entry.get("status") == "queued":
            entry["status"] = "reserved"
            entry["reserved_run_id"] = run_id
            entry["reserved_at"] = timestamp
            save_queue(entries)
            context["ti"].xcom_push(key="queued_file_name", value=entry["file_name"])
            return "ingest_to_bronze"

    context["ti"].xcom_push(key="queued_file_name", value=None)
    return "no_queued_file"


def mark_processed(**context) -> None:
    entries = load_queue()
    run_id = context["run_id"]
    timestamp = datetime.utcnow().isoformat()

    for entry in entries:
        if entry.get("reserved_run_id") == run_id:
            entry["status"] = "processed"
            entry["processed_run_id"] = run_id
            entry["processed_at"] = timestamp
            break

    save_queue(entries)


dag = DAG(
    dag_id=SCHEDULED_DAG_ID,
    default_args=default_args,
    description="Scheduled pipeline - processes the next queued upload",
    schedule_interval="0 5 * * *",
    catchup=False,
)


select_queued_file = BranchPythonOperator(
    task_id="select_queued_file",
    python_callable=reserve_next_file,
    dag=dag,
)


no_queued_file = EmptyOperator(
    task_id="no_queued_file",
    dag=dag,
)


ingest_bronze = BashOperator(
    task_id="ingest_to_bronze",
    bash_command=(
        "spark-submit --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 "
        "--conf spark.driver.host=airflow-scheduler --master spark://spark-master:7077 "
        "/opt/airflow/spark/bronze_job.py "
        "'{{ ti.xcom_pull(task_ids=\"select_queued_file\", key=\"queued_file_name\") }}'"
    ),
    dag=dag,
)


bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=(
        "spark-submit --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 "
        "--conf spark.driver.host=airflow-scheduler --master spark://spark-master:7077 "
        "/opt/airflow/spark/silver_job.py"
    ),
    dag=dag,
)


silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command=(
        "spark-submit --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 "
        "--conf spark.driver.host=airflow-scheduler --master spark://spark-master:7077 "
        "/opt/airflow/spark/gold_job.py"
    ),
    dag=dag,
)


mark_queue_processed = PythonOperator(
    task_id="mark_queue_processed",
    python_callable=mark_processed,
    dag=dag,
)


select_queued_file >> no_queued_file
select_queued_file >> ingest_bronze >> bronze_to_silver >> silver_to_gold >> mark_queue_processed
