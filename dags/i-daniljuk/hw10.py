"""
PythonOperator and xcom
"""
from datetime import datetime, timedelta
# from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'hw_10_i-daniljuk',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    description='A cycle tasks DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['i-daniljuk'],
) as dag:
    
    def print_string():
        return'Airflow tracks everything'
    
    def func_pull(ti):
        pull_func = ti.xcom_pull(
            key='return_value',
            task_ids='print_string'
        )
        print(pull_func)
        
        
    t1 = PythonOperator(
        task_id='print_string',
        python_callable=print_string,
    )
    
    t2 = PythonOperator(
        task_id='retval',
        python_callable=func_pull,
    )
    t1 >> t2