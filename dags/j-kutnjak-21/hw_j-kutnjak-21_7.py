from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }

with DAG(
    'hw_j-kutnjak-21_3',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 30),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id='j-kutnjak-21_task3-1_' + str(i),
                bash_command=f"echo {i}",
            )
        else:
            def print_number(i, ts, run_id, **kwargs):
                task_number = kwargs['task_number']
                print(f"task number: {task_number}")
                print(f"ts: {ts}")
                print(f"run_id: {run_id}")
                return (f'task number: {i}')

            t1 = PythonOperator(
                task_id='print_number_' + str(i),
                python_callable=print_number,
                op_kwargs={'task_number': i}
            )
