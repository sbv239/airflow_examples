from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Task 7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 18),
        catchup=False,

        tags=['Task_7'],
) as dag:
    def print_context(ts, run_id, task_number):
        print(ts, run_id)
        return f"task number is: {task_number}"


    for i in range(20):
        t1 = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i}
        )

    t1