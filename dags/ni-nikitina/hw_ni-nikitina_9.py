from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key', 
        value='xcom test'
    )

def pull_xcom(ti):
    ti_pull = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids = 'push'
    )
    print(ti_pull)

with DAG(
    'hw_ni-nikitina_9', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Ninth Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
   
    t1 = PythonOperator(
        task_id='push', 
        python_callable=push_xcom
    )

    t2 = PythonOperator(
        task_id='pull', 
        python_callable=pull_xcom
    )

t1 >> t2