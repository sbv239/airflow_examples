from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_ta-korobitsyna_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        },
    description='hw_6_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['hw_ta-korobitsyna_6'],
) as dag:

    for i in range(10):

        t1 = BashOperator(
            
            task_id = f"echo_{i}",  # id, будет отображаться в интерфейсе
            bash_command = "echo $NUMBER",  # какую bash команду выполнить в этом таске
            env = {'NUMBER': str(i)} 
        )
