from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "task_2_k-zhuravlev",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:
    t1 = BashOperator(task_id="print_pwd", bash_command="pwd")

    def print_ds(ds):
        print(ds)
        return "Epshtein didn't kill himself"

    t2 = PythonOperator(ds, python_callable=print_context)

    t1 >> t2