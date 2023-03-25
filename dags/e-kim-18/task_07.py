"""
Task 02 documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'e-kim-18_task_07',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A DAG for task 02',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 20),
        catchup=False,
        tags=['e-kim-18-tag'],
) as dag:

    def print_ds(ts,run_id,**kwargs):
        print(f'task number is: {kwargs}')
        print(ts)
        print(run_id)


    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_ds_{i}',  # нужен task_id, как и всем операторам
            python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': i,},
        )

    t2