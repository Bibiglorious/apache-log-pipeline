from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add project directory to Python path
sys.path.append('/path/to/LOGWATCHERS_NA')

# Import pipeline functions
from extractor import fetch_logs_with_retry
from parser import parse_logs
from classifier import classify_logs
from database import create_database, save_classified_logs_to_database
from summarizer import generate_all_reports

# Default arguments for the DAG
default_args = {
    'owner': 'logwatchers',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'apache_log_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Apache log processing',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['etl', 'logs', 'apache']
)

# Task 1: Extract logs from API
def extract_task():
    logs = fetch_logs_with_retry()
    if not logs:
        raise Exception("No logs extracted from API")
    return logs

extract_logs = PythonOperator(
    task_id='extract_logs',
    python_callable=extract_task,
    dag=dag
)

# Task 2: Parse and deduplicate logs
def parse_task(**context):
    logs = context['task_instance'].xcom_pull(task_ids='extract_logs')
    parsed_logs = parse_logs(logs)
    if not parsed_logs:
        raise Exception("No logs parsed successfully")
    return parsed_logs

parse_logs_task = PythonOperator(
    task_id='parse_logs',
    python_callable=parse_task,
    dag=dag
)

# Task 3: Classify logs
def classify_task(**context):
    parsed_logs = context['task_instance'].xcom_pull(task_ids='parse_logs')
    classified_logs = classify_logs(parsed_logs)
    if not classified_logs:
        raise Exception("No logs classified")
    return classified_logs

classify_logs_task = PythonOperator(
    task_id='classify_logs',
    python_callable=classify_task,
    dag=dag
)

# Task 4: Create database
create_db_task = PythonOperator(
    task_id='create_database',
    python_callable=create_database,
    dag=dag
)

# Task 5: Save logs to database
def save_task(**context):
    classified_logs = context['task_instance'].xcom_pull(task_ids='classify_logs')
    saved_count = save_classified_logs_to_database(classified_logs)
    return saved_count

save_logs_task = PythonOperator(
    task_id='save_logs',
    python_callable=save_task,
    dag=dag
)

# Task 6: Generate reports
generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_all_reports,
    dag=dag
)

# Task 7: Cleanup (optional)
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command='echo "Pipeline completed successfully"',
    dag=dag
)

# Define task dependencies
extract_logs >> parse_logs_task >> classify_logs_task
create_db_task >> save_logs_task
classify_logs_task >> save_logs_task >> generate_reports_task >> cleanup_task