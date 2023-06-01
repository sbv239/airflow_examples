from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_dmi-rusin_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Seven task',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2023, 6, 1),
        catchup=False,
        tags=['example'],
) as dag:
    def print_task(ts, run_id, task_number):
        print("task number is:", task_number)
        print(ts)
        print(run_id)

    for i in range(20):
        t1 = PythonOperator(
            task_id='dmi-rusin_7_tasks' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i, 'ts': '{{ts}}', 'run_id': '{{run_id}}'},
        )
