"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'hw_7_a_loskutov',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hm_7'],
) as dag:
    


    def print_task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(30):
        if i<10:
            t1_Bash=BashOperator(
                task_id=f'print_number_of_loop_{i}',
                depends_on_past=False,
                bash_command="echo $NUMBER",
                env={"NUMBER": i}

            )
        else:
            t2_python=PythonOperator(
                task_id=f'print_task_number_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}

            )
