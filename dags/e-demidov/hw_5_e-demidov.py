from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_2_e-demidov',
    default_args={
        'depends_on_past': False,
        'email': ['yevgeniy.demidov@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_2_e-demidov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 17),
    catchup=False,
    tags=['e-demidov'],
) as dag:
    
    for i in range(10):
        cyclic_task_1 = BashOperator(
        task_id = 'echo_' + str(i),
        env={"NUMBER": i},
        bash_command = 'echo $NUMBER')
    
cyclic_task_1