from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
import json


with DAG(
    'dag_task10',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2023, 3, 23)
) as dag:
    

    def funny_func(ti):
        return "Airflow tracks everything"
        

    def get_value(ti):
        ti.xcom_pull(
            key='return_value',
            task_ids='call_the_string'
        )
    
    t1 = PythonOperator(
        task_id = 'call_the_string',
        python_callable= funny_func ,
    )
    t2 = PythonOperator(
        task_id = 'get_string',
        python_callable = get_value,
    )

    t1 >> t2
