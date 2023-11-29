from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def message():
    return "Airflow tracks everything"

def pull_xcom(ti):
    ti_pull = ti.xcom_pull(
        key='return_value',
        task_ids='airflow'
    )

with DAG(
    'hw_ni-nikitina_10', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Tenth Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
   
    t1 = PythonOperator(
        task_id='airflow', 
        python_callable=message
    )

    t2 = PythonOperator(
        task_id='pull', 
        python_callable=pull_xcom
    )

t1 >> t2