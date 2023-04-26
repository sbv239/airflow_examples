"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'hw_10_a_loskutov',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hw'],
) as dag:
    

        def print_str():
                return 'Airflow tracks everything'


        def get_str(ti):
                result=ti.xcom_pull(
                        key="return_value",
                        task_ids='return_str'
                )
                return result

        first = PythonOperator(
                task_id='return_str',
                python_callable=print_str,
        )

        second =  PythonOperator(
                task_id='get_str',
                python_callable=get_str,
        )

        first >> second

