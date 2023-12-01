from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_3_m-statsenko',
        default_args=default_args,
        description='HW 3',
        start_date=datetime(2023, 11, 30),
        catchup=False,
        tags=['HW 3']
) as dag:
    for i in range(10):
        task_b = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f"echo {i}",
        )


    def print_number(task_number):
        print(f"task number is: {task_number}")


    for i in range(20):
        task_p = PythonOperator(
            task_id='print_number_task_' + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )
    task_b >> task_p
