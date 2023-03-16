import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_string(ti):
    return "Airflow tracks everything"
        
def pull_string(ti):
    testing_pull = ti.xcom_pull(key="return_value", task_ids="simple_task")
    print(testing_pull)
                 
with DAG(
    'task_8',
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
    
    task_without_xcom = PythonOperator(
        task_id = "simple_task",
        python_callable=print_string,
    )
    
    task_with_xcom = PythonOperator(
        task_id = "give_value",
        python_callable=pull_string,
    )
                 
    task_without_xcom >> task_with_xcom