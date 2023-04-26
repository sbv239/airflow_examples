from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_6_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['hw_6_al-savelev']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'hw_6_al-savelev_bash_{i}',
            bash_command='echo $NUMBER',
            env = {'NUMBER': str(i)})
    
   
    t1