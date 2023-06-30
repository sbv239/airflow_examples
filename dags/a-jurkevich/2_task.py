"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "a-jurkevich_task_2",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="hw_2_a-jurkevich",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["hw_2_a-jurkevich"],
) as dag:
    t1 = BashOperator(task_id="BashOperatorUsage", bash_command="pwd")

    def print_ds(ds):
        print(ds)
        print("print_ds prints whats followed")

    t2 = PythonOperator(task_id="PythonOperatorUsage", python_callable=print_ds)

    t1 >> t2
