import os
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tarasova_task5',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG for task  Tarasova E',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 19),
    catchup=False,
    tags=['task5']

,
) as dag:

    for i in range(10):
        os.environ['NUMBER'] = str(i)
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command="echo '{}'".format(os.environ['NUMBER']),
        )

        t1