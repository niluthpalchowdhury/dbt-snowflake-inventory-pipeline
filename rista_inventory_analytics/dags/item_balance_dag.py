"""Daily orchestration: extract/load -> dbt run -> dbt test.

Strictly sequential. dbt must not run against a half-written landing zone, and
tests must not be skipped just because the models built.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Deployment paths on the Airflow worker.
PROJECT_DIR = "/opt/airflow/projects/rista_item_balance"
DBT_DIR = f"{PROJECT_DIR}/rista_inventory_analytics"

# Set to e.g. "--profiles-dir /opt/airflow/.dbt" if the profile does not live
# in the worker's default ~/.dbt location.
DBT_PROFILES_FLAG = ""

default_args = {
    "owner": "niluthpal",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="item_balance_pipeline",
    description="Rista item balance: extract/load -> dbt run -> dbt test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 12 * * *",
    catchup=False,
    # The extract replaces whole date partitions, so overlapping runs could
    # race on the same blobs.
    max_active_runs=1,
    tags=["rista", "item_balance", "dbt"],
) as dag:

    # 1. Rista API -> Parquet partitions in Blob Storage (-> Snowflake RAW).
    extract_load = BashOperator(
        task_id="extract_load",
        bash_command=f"cd {PROJECT_DIR} && python main.py",
    )

    # 2. Build bronze -> silver -> gold.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run {DBT_PROFILES_FLAG}".strip(),
    )

    # 3. Grain uniqueness, not-null, and source freshness.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test {DBT_PROFILES_FLAG}".strip(),
    )

    extract_load >> dbt_run >> dbt_test
