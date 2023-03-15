import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def push_test(ti):
    url = "___"
    res = requests.get(url)
    ti.xcom_push(key="sample_xcom_key", value=json.loads(res.text

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
    
    def print_smth(ts, run_id, task_number, **kwargs):
        print(ts)
        print(run_id)
        print(task_number)
        
    push_task = PythonOperator(
        task_id = 'python_part' + str(i),
        python_callable=print_smth,
        op_kwargs={'task_number': i},
    )
            
    bash_task >> python_task