"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_5_a-haletina-7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_5_a-haletina-7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_5_a-haletina-7'],
) as dag:

    def task_number(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)
        return None

    for i in range(11, 31):
        t1 = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )

    t1