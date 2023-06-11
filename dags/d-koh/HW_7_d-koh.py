from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
        'HW_7_d-koh',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG from step 3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 11),
        catchup=False,
        tags=['kokh'],
) as dag:
    def print_task(task_number, ts, run_id, **kwargs):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)


    for i in range(20):
        python_op = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )

