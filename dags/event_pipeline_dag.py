from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'event_driven_data_pipeline',
    default_args=default_args,
    description='Event-driven pipeline - triggers when new CSV appears',
    schedule_interval=None,  # only triggered by FileSensor
    catchup=False,
)

# Watches data/input/ — triggers the moment a CSV appears
file_sensor = FileSensor(
    task_id='wait_for_new_file',
    filepath='/opt/airflow/data/input/titanic.csv',
    fs_conn_id='fs_default',
    poke_interval=30,       # checks every 30 seconds
    timeout=60 * 60 * 24,  # gives up after 24 hours
    mode='poke',
    dag=dag,
)

ingest_bronze = BashOperator(
    task_id='ingest_to_bronze',
    bash_command='cd /opt/airflow && python spark/bronze_job.py',
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='cd /opt/airflow && python spark/silver_job.py',
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command='cd /opt/airflow && python spark/gold_job.py',
    dag=dag,
)

# Chain — FileSensor must pass before anything runs
file_sensor >> ingest_bronze >> bronze_to_silver >> silver_to_gold