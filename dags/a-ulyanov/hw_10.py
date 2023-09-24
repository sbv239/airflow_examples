from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def push_data():
    return "Airflow tracks everything"


def pull_data(ti):
    result = ti.xcom_pull(key="return_value", task_ids="push_data")
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
