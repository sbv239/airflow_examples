from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

ID = "hw_4_n-chistjakov_"

def print_info(task_number):
    print(f'task number is: {task_number}')

with DAG(
    'hw_4_n-chistjakov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description="Second task",
    start_date=datetime(2023, 6, 30),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["task_04"],
) as dag:
    for i in range(10):
        bash = BashOperator(
            task_id=ID + f"1_{i}",
            bash_command=f"echo {i}"    
        )

    for i in range(10, 30):
        pyth = PythonOperator(
            task_id=ID + f"2_{i}",
            python_callable=print_info,
            op_kwargs={'task_number': i}
        )

        bash.doc_md = dedent(
            """
            #Title
            `This is code`
            **Bold**
            __Italic__
            """
        )

        pyth.doc_md = dedent(
            """
            #Title
            `This is code`
            **Bold**
            __Italic__
            """
        )   

    bash >> pyth