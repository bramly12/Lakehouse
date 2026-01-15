# Partir de l'image officielle Airflow
FROM apache/airflow:2.9.3

USER airflow

# Installer dbt + driver Postgres
RUN pip install --no-cache-dir dbt-core dbt-postgres

# Installer git pour dbt
USER root
RUN apt-get update && apt-get install -y git
USER airflow
