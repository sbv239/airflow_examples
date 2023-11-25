from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_s-majkova_7',
    default_args = {
    'depens_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    },
    description = 'first_dynamic_DAG',
    schedule_interval=timedelta(days=1),
    start_date = datetime(2023, 11, 25),
    catchup = False,
    tags = ['hw_7'],
) as dag:
    def task_number(task_number, ts,  run_id, **kwargs):
        print(f'task number is: {task_number}')
        print (ts)
        print(run_id)

    for i in range(10):
        t1 = BashOperator(
            task_id ='task'+ str(i),
            bash_command = f'echo {i}',
        )
    for i in range(20):
        t2 = PythonOperator(
            task_id ='task_number' + str(i),
            python_callable = task_number,
            op_kwargs = {'task number': int(i)},
        )

t1>>t2