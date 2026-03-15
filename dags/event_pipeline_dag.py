from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
    dag_id="event_driven_data_pipeline",
    default_args=default_args,
    description='Data pipeline triggered by dashboard upload',
    schedule_interval=None,  # This DAG is only triggered manually or via API
    catchup=False,
)

# ----------------------------------------------------------------------
# Python Function to Generate Report & Email
# ----------------------------------------------------------------------
def notify_stakeholders(**context):
    """
    Reads metrics from all layers, generates a text report, 
    and simulates sending an email to stakeholders.
    """
    metrics_dir = "/opt/airflow/data/delta/metrics"
    report_path = "/opt/airflow/data/delta/pipeline_report.txt"
    
    # 1. Consolidate Metrics
    report_content = f"PIPELINE EXECUTION REPORT\n"
    report_content += f"=========================\n"
    report_content += f"Run ID: {context['run_id']}\n"
    report_content += f"Date:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    layers = ["bronze", "silver", "gold"]
    for layer in layers:
        file_path = os.path.join(metrics_dir, f"{layer}_metrics.json")
        report_content += f"[{layer.upper()} LAYER]\n"
        
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                metrics = json.load(f)
                for key, value in metrics.items():
                    report_content += f" - {key}: {value}\n"
        else:
            report_content += " - Status: No metrics found (Job may have failed or skipped)\n"
        report_content += "\n"

    # 2. Save Report to File (Log format)
    with open(report_path, "w") as f:
        f.write(report_content)
    print(f"Report saved to: {report_path}")

    # 3. Send Email (Real Implementation)
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    recipient_email = smtp_user  # Sending to self for testing

    if smtp_user and smtp_password:
        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = recipient_email
            msg['Subject'] = f"✅ Pipeline Success: Run {context['run_id']}"

            msg.attach(MIMEText(report_content, 'plain'))

            # Connect to Gmail SMTP Server
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(smtp_user, smtp_password)
            text = msg.as_string()
            server.sendmail(smtp_user, recipient_email, text)
            server.quit()
            print(f"✅ Email sent successfully to {recipient_email}")
        except Exception as e:
            print(f"❌ Failed to send email: {str(e)}")
    else:
        print("⚠️ SMTP credentials not found in environment variables. Skipping email.")
        # Fallback to printing for logs
        print("\n" + "="*50)
        print(f"Subject: Pipeline Update - {context['ds']}")
        print(f"Body:\n{report_content}")
        print("="*50 + "\n")

ingest_bronze = BashOperator(
    task_id='ingest_to_bronze',
    # Get file_name from DAG run config, default to 'titanic.csv' for manual runs
    # Submit the job to the Spark cluster (Client Mode is default and required for Standalone Python)
    bash_command="spark-submit --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 --conf spark.driver.host=airflow-scheduler --master spark://spark-master:7077 /opt/airflow/spark/bronze_job.py '{{ (dag_run.conf or {}).get(\"file_name\", \"titanic.csv\") }}'",
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='spark-submit --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 --conf spark.driver.host=airflow-scheduler --master spark://spark-master:7077 /opt/airflow/spark/silver_job.py',
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command='spark-submit --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 --conf spark.driver.host=airflow-scheduler --master spark://spark-master:7077 /opt/airflow/spark/gold_job.py',
    dag=dag,
)

notify_task = PythonOperator(
    task_id='notify_stakeholders',
    python_callable=notify_stakeholders,
    dag=dag,
)

# Define the task sequence
ingest_bronze >> bronze_to_silver >> silver_to_gold >> notify_task
