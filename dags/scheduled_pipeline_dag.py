from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    'scheduled_data_pipeline',
    default_args=default_args,
    description='Scheduled pipeline - runs daily at 5AM',
    schedule_interval='0 5 * * *',   # every day at 5 AM
    catchup=False,
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

# Chain
ingest_bronze >> bronze_to_silver >> silver_to_gold