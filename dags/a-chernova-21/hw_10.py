from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def put_xcom():
    value = 'Airflow tracks everything'
    return value

def extract_xcom(ti):
    value=ti.xcom_pull(
        key='return_value',
        task_ids='put'
    )
    print(value)
    return value
    
with DAG(
    'hw_a-chernova-21_10',
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
    t2 = PythonOperator(
        task_id = 'extract',
        python_callable=extract_xcom
    )   
    
t1 >> t2