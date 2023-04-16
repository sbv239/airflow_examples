from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator


def push_xcom(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def pull_xcom(ti):
    xcom_value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="push_xm"
    )
    print(xcom_value)


with DAG(
    'aakulzhanov_task_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
   },
    description='A simple Task 9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    task_1 = PythonOperator(
        task_id="push_xm",
        python_callable=push_xcom
    )
    task_2 = PythonOperator(
        task_id="pull_xm",
        python_callable=pull_xcom
    )

    task_1 >> task_2