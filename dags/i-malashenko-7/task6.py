"""
Test documentation
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

import requests
import json

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
with DAG(
    'hw_6_i-malashenko-7',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 20),
    catchup=False,
    tags=['example'],
) as dag:

    def print_task_number(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print('task number is', str(kwargs['task_number']))

    for i_ in range(30):
        if i_ < 10:

            t1 = BashOperator(
                task_id=f'echo_{i_}',
                bash_command="echo $NUMBER",
                env={"NUMBER": str(i_)}
            )

        else:

            t2 = PythonOperator(
                task_id='print_task_' + str(i_),  # в id можно делать все, что разрешают строки в python
                python_callable=print_task_number,
                op_kwargs={'task_number': i_}
            )