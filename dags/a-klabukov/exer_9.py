from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def push_for_operator(ti):
    ti.xcom_push(
        key = 'sample_xcom_key',
        value = 'xcom test'
    )

def pull_for_operator(ti):
    res = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids = 'push'
    )
    print(res)

with DAG(
    'a-klabukov_hw_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task_push = PythonOperator(
        task_id = 'push',
        python_callable = push_for_operator,
    )

    task_pull = PythonOperator(
        task_id = 'pull',
        python_callable = pull_for_operator,
    )


    task_push >> task_pull