from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def insert_data(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

def get_data(ti):
    pulled_data = ti.xcom_pull(
        task_ids="insert_data",
        key="sample_xcom_key"
    )
    print(pulled_data)
    

with DAG(
    'hw_kamilahmadov_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_9"]
) as dag:
    
    insert_data_operator = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    get_data_operator = PythonOperator(
        task_id="get_data",
        python_callable=get_data
    )

    insert_data_operator >> get_data_operator
