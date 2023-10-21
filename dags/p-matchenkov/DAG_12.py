import datetime

import airflow.hooks.base
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from textwrap import dedent

from datetime import timedelta


def get_airflow_var():
    var = Variable.get('is_startml')
    print(var)
    return var



with DAG(
    'hw_p-matchenkov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task_12',
    tags=['matchenkov'],
    start_date=datetime.datetime(2023, 10, 19),
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='get_var',
        python_callable=get_airflow_var
    )

    task1