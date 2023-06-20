from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from textwrap import dedent
    
with DAG(
    'hw_maks-novikov_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='HW7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_maks-novikov_7'],
) as dag:
    dag.doc_md = """
        This is a documentation placed anywhere
    """ 


    def push_xcom(ti):
            ti.xcom_push(
                key='sample_xcom_key',
                value='xcom test'
            )
    def pull_xcom(ti):
            res = ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='The_first_task'
            )
            print(res)

    t1 = PythonOperator(
            task_id='The_first_task',  
            python_callable=push_xcom)
    t2 = PythonOperator(
            task_id='The_second_task',  
            python_callable=pull_xcom,
        )
    t1 >> t2
