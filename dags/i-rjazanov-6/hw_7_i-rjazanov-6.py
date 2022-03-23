import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        # Название таск-дага
        'hw_7_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_7']

) as dag:

    def print_run_id_ts(ts, run_id, task_number):
        print(f"Run_id -> {run_id}, ts -> {ts}")
        print(f"Task_number -> {task_number}")

    for i in range(20):

        t1 = PythonOperator(
            task_id='task_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_run_id_ts,
            op_kwargs={'task_number': i},
        )

        t1
