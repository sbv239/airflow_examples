from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )
    
def print_xcom(ti):
    test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='python_push'
    )
    print(test)

with DAG(
    'hw_m-lebedev_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 9, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 23),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    puthon_push =  PythonOperator(
        task_id = 'python_push',
        python_callable=push,
    )
    
    puthon_pull =  PythonOperator(
        task_id= 'python_pull',
        python_callable=print_xcom,
    )
    
    puthon_push >> puthon_pull