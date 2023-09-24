from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def push_data(**kwargs):
    ti = kwargs["ti"]
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test",
    )


def pull_data(**kwargs):
    ti = kwargs["ti"]
    result = ti.xcom_pull(key="sample_xcom_key")
    return result


with DAG(
    "hw_a-ulyanov_9",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="hw_9 DAG",
    start_date=datetime(2023, 9, 23),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="push_data",
        python_callable=push_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="pull_data",
        python_callable=pull_data,
        provide_context=True,
    )

    t1 >> t2
