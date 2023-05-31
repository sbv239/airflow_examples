"""
Создайте новый DAG, содержащий два PythonOperator.
Первый оператор должен вызвать функцию, возвращающую строку "Airflow tracks everything".

Второй оператор должен получить эту строку через XCom.
Вспомните по лекции, какой должен быть ключ.
Настройте правильно последовательность операторов.

"""

from datetime import datetime, timedelta
from textwrap import dedent

import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get(ti):
    return "Airflow tracks everything"

def pull(ti):
    pulled_value = ti.xcom_pull(
        key = 'return_value',
        task_ids = 'xcom_push' # id первого PythonOperator

    )
    print(pulled_value)

with DAG(
    'hw_9_ko-popov',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_9_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_9_ko-popov'],
) as dag:
    task_get = PythonOperator(
        task_id = 'xcom_push',
        python_callable = get
    )

    task_pull = PythonOperator(
        task_id = 'xcom_pull',
        python_callable = pull
    )
    task_get >> task_pull