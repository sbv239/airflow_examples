from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'a-petuhova_step7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 23),
        catchup=False,
        tags=['a-petuhova'],
) as dag:
    
    for i in range(10):
        task = BashOperator(
            task_id=f'task_bash_{i}',
            bash_command=dedent(f"echo {i}")
        )


    def task_number_is(task_number, ts, run_id, **kwargs):
        print(task_number)
        print(ts)
        print(run_id)
        return f"task number"


    for task_number in range(20):
        task = PythonOperator(
            task_id=f'python_oper_{task_number}',
            python_callable=task_number_is,
            op_kwargs={'task_number': task_number}
        )
