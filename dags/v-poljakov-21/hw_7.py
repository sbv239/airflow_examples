from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number, ts, run_id):
    print(f'task number is: {task_number}')
    print(ts)
    print(run_id)

with DAG(
    'hw_v-poljakov-21_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Exercise 7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 29),
    catchup=False,
    tags=['hw7'],
) as dag:
    for i in range(10, 30):
        task = PythonOperator(
            task_id="echo" + str(i),
            python_callable=print_task_number,
            op_kwargs={"task_number": i}
        )
        task

