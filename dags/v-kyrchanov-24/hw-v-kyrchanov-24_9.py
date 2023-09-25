from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def push_to_xcom(ti) -> None:
    ti.xcom_push(key="sample_xcom_key", value="xcom test")


def pull_from_xcom(ti) -> None:
    value = ti.xcom_pull(key="sample_xcom_key", task_ids="push_task")
    print(f"Value from XCom: {value}")


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(year=2023, month=9, day=24),
}

with DAG(
    dag_id="my_xcom_dag",
    default_args=default_args,
    description="A new DAG with two PythonOperators using XCom",
    schedule_interval="@daily",
) as dag:
    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_to_xcom,
        provide_context=True,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_from_xcom,
        provide_context=True,
    )

    push_task >> pull_task
