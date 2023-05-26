from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(task_number, **kwargs):
    return 'task number is: {task_number}'

with DAG(
    'hw_3_i-loskutov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='task03',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 27),
    catchup=False

) as dag:
    for i in range(10):
        t1 = BashOperator(
        task_id = 'task03_BashOperator',
        bash_command = 'f"echo {i}" ',
        )
    for i in range(20):
        t2 = PythonOperator(
        task_id = 'task03_PythonOperator',
        python_callable = print_context(i),
        op_kwargs={'task_number': i}
        )
    t1 >> t2




