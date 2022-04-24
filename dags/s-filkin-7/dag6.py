from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def python_task(task_number, ts, run_id):
    print(f"task number is: {task_number}")
    print(f"ts: {ts}")
    print(f"run_id: {run_id}")
    return 'ok'

with DAG(
    's-filkin-7-dag6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 16),
) as dag:

    for i in range(0, 10):
        t1 = BashOperator(
            task_id='bash_' + str(i), 
            bash_command=f"echo $NUMBER", 
            env={'NUMBER': str(i)}
        )

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='python_' + str(i), 
            python_callable=python_task,
            op_kwargs={'task_number': i},
        )

    t1 >> t2