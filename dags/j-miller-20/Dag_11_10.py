from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# import requests
# import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_j-miller_10',
        description='A 10 simple tutorial DAG wtih Xcom',  # Описание DAG (не тасок, а самого DAG)
        schedule_interval=timedelta(days=1),  # Как часто запускать DAG
        start_date=datetime(2023, 5, 31),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        tags=['SimJul_10'],  # теги, способ помечать даги
) as dag:
    def return_data_xcom(ti):
        return "Airflow tracks everything"


    def print_data_xcom(ti):
        value = ti.xcom_pull(
            key='return_value',
            task_ids='task_return'
        )
        print(value)


    t1 = PythonOperator(
        task_id='task_return',
        python_callable=return_data_xcom,
        # op_kwargs={'xcom_data': 'xcom test'},
        provide_context=True
    )
    t2 = PythonOperator(
        task_id='task_print',
        python_callable=print_data_xcom,
        provide_context=True
    )

    t1 >> t2
