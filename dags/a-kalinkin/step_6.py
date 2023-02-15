
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    'hw_6_a-kalinkin',#МЕНЯЙ ИМЯ ДАГА
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='DAG wiht NUMBERS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_a-kalinkin'],
) as dag:


    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i<10:
            t=BashOperator(
                task_id=f'print_number_of_loop_{i}',
                depends_on_past=False,
                bash_command="echo $NUMBER",
                env={"NUMBER": i}


            )
        else:
            t1=PythonOperator(
                task_id=f'print_task_number_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}

            )

    t >> t1