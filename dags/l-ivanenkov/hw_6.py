from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_6_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_6_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_6_l-ivanenkov'],
) as dag:
    def task_num(task_number, **kwargs):
        print(f'task number is: {task_number}')


    for i in range(10):
        NUMBER = i
        t_bash = BashOperator(
            task_id=f'BashOperator_{i}',
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )