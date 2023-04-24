from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


def push_string(ti):
    return "Airflow tracks everything"

def pull_string(ti):
    getting = ti.xcom_pull(
        key="return_value",
        task_ids="return_string")
    return getting


with DAG(
        'les_11_task_10_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),

) as dag:
    pushing_data = PythonOperator(
        task_id="return_string",
        python_callable=push_string,
    )

    pulling_data = PythonOperator(
        task_id="pull_string_id",
        python_callable=pull_string,
    )

    pushing_data >> pulling_data
