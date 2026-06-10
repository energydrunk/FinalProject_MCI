from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "finance_analyst",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "finance_payment_pipeline",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    max_active_runs=1,
    description="CSV DustiniaDelixia → Spark Transform → ClickHouse → Metabase",
) as dag:

    extract_step = BashOperator(
        task_id="extract_and_validate",
        bash_command="python /opt/airflow/dags/scripts/extract_and_validate.py",
    )

    transform_step = BashOperator(
        task_id="transform_payments",
        bash_command="python /opt/airflow/dags/scripts/transform_payments.py",
    )

    load_step = BashOperator(
        task_id="load_to_clickhouse",
        bash_command="python /opt/airflow/dags/scripts/load_to_clickhouse.py",
    )

    extract_step >> transform_step >> load_step
