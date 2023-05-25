from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'a-petuhova_step3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Tasks for step 3 a-petuhova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-petuhova'],
) as dag:

    for i in range(10):
        task_Bash = BashOperator(
            task_id=f'task_bash_{i}',
            bash_command=dedent(f"echo {i}")
        )

    def task_number_is(task_number):
        return f"task number is: {task_number}"
    
    for task_number in range(20):
        task_Python = PythonOperator(
            task_id=f'task_python_{task_number}',
            python_callable=task_number_is,
            op_kwargs={'task_number':task_number}
        )
    task_Bash >> task_Python