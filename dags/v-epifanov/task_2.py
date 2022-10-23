from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_date(ds, **kwargs):
    print(f"Дата: {ds}")

with DAG (
    'task_2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd',
    )

    t2 = PythonOperator(
        task_id = 'print_date',
        python_callable=print_date,
    )

    t1 >> t2
