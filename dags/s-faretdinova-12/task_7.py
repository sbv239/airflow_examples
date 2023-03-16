import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def push_test(ti):
    ti.xcom_push(
        key="sample_xcom_key", 
        value="xcom test"
    )    
        
def pull_test(ti):
    testing_smth = ti.xcom_pull(key="sample_xcom_key", task_ids="get_value")
    print("If you understand what I am trying to do, print value for key sample_xcom_key", testing_smth)
                 
with DAG(
    'task_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='An attempt to create DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['attempt'],
) as dag:
    
    get_smth = PythonOperator(
        task_id = "get_value",
        python_callable=push_test,
    )
    
    give_smth = PythonOperator(
        task_id = "give_value",
        python_callable=pull_test,
    )
                 
    get_smth >> give_smth