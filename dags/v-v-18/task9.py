from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
import json


with DAG(
    'dag_task9',
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
    

    def put_value(ti):
        my_value = 'xcom test'
        print(my_value)
        ti.xcom_push(
            key='sample_xcom_key',
            value=my_value
        )

    def get_value(ti):
        ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='putting'
        )
    
    my_put_task = PythonOperator(
        task_id = 'putting',
        python_callable= put_value ,
    )
    my_get_task = PythonOperator(
        task_id = 'getting',
        python_callable = get_value,
    )

    my_put_task >> my_get_task
