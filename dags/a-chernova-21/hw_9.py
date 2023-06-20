from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def put_xcom(ti):
    value = 'xcom test'
    ti.xcom_push(
        key='sample_xcom_key',
        value='value'
    )

def extract_xcom(ti):
    value=ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='put'
    )
    print(value)
    return value
    
with DAG(
    'hw_a-chernova-21_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='A new dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 20),
    catchup=False,
    tags=['example']
) as dag:
    t1 = PythonOperator(
        task_id = 'put',
        python_callable=put_xcom
    )
    t1 = PythonOperator(
        task_id = 'extract',
        python_callable=extract_xcom
    )   
    
t1 >> t2
    
        
    