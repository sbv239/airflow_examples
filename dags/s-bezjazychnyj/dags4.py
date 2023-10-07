from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_4_s-bezjazychnyj',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='new DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 6),
    catchup=True,
    tags=['bezsmen'],
) as dag:
    for i in range(10):
        t1=BashOperator(
            task_id=f'echo_{i}',
            bash_command=f'echo {i}',
        )
        t1.doc_md=dedent(
            """
        # New abstract
        `print task`
        **bold line**
        *italic line"
        """
        )
    for i in range(10,30):
        def print_task_number(task_number):
            print(f'task number is {task_number}')
        t2=PythonOperator(
            task_id=f'prind_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number':i}
        )