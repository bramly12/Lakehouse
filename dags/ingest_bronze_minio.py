import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/scripts')
from ingestion_bronze import run_ingestion

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_bronze_minio",
    default_args=default_args,
    start_date=datetime(2026, 1, 14),
    schedule="*/10 * * * *",  
    catchup=False,
    tags=["bronze", "minio", "postgres"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_minio_to_bronze",
        python_callable=run_ingestion
    )
