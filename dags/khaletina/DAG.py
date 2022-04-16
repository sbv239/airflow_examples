"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'tutorial',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_1_khaletina',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_1_khaletina'],
) as dag:

    t1 = BashOperator(
        task_id='print_current_folder',  
        bash_command='pwd',  
    )
    
    # def print_context(ds, **kwargs):
    # print(kwargs)
    # print(ds)
    # return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2
