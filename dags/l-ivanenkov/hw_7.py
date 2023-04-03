from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_7_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_7_l-ivanenkov'],
) as dag:
    def task_num(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)


    for i in range(20):
        if i < 10:
            t1 = BashOperator(
                task_id=f'bash_operator_{i}',
                bash_command=f'echo {i}',
            )
        t2 = PythonOperator(
            task_id=f'py_operator_{i}',
            python_callable=task_num,
            op_kwargs={'task_number': i}
        )
