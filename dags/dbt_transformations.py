from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_transformations",
    start_date=datetime(2026, 1, 14),
    schedule="0 * * * *",  # toutes les heures
    catchup=False,
    tags=["dbt", "silver", "gold"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_transformations",
        bash_command="cd /opt/airflow/dbt && dbt build --profiles-dir . --target prod"
    )
