from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_3_k_shirochenkov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 19),
    catchup=False,
    tags=['k-shirochenkov'],
) as dag:
    first_task = BashOperator(
        task_id='bash_task_1',
        bash_command='pwd',
    )

    def print_context(op_kwargs, **kwargs):
        print(f"task number is: {op_kwargs}")
        return 'Ok'

    for i in range(2, 31):
        if i < 11:
            temp_task = BashOperator(
                task_id = "bash_task_" + str(i),
                bash_command = f'echo {i}',
            )
        else:
            temp_task = PythonOperator(
                task_id=f'python_task_{i}',
                python_callable=print_context,
                op_kwargs=i
            )

        first_task >> temp_task

