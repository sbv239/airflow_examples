""""
Сделайте новый DAG, содержащий два Python оператора.
Первый PythonOperator должен класть в XCom значение "xcom test" по ключу "sample_xcom_key".

Второй PythonOperator должен доставать это значение и печатать его.
Настройте правильно последовательность операторов.
"""

from datetime import datetime, timedelta
from textwrap import dedent

import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get(ti):
    ti.xcom_push(
        key = 'sample_xcom_key',
        value = "xcom test"
    )

def pull(ti):
    pulled_value = ti.xcom_pull(
        key = 'sample_xcom_key',
        task_ids = 'xcom_push' # id первого PythonOperator

    )
    print(pulled_value)

with DAG(
    'hw_8_ko-popov',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_8_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_8_ko-popov'],
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