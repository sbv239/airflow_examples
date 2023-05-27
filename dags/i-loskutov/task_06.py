from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(task_number, **kwargs):
    return 'task number is: {task_number}'

with DAG(
    'hw_6_i-loskutov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='task06',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 27),
    catchup=False

) as dag:
    for i in range(10):
        t1 = BashOperator(
        task_id = f'task06_BashOperator_{i}',      
        bash_command=f"echo $NUMBER", 
        env=({'NUMBER': i}),         
        )
    t1




