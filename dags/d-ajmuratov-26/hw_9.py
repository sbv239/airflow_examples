from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-ajmuratov-26_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
    tags=['HW 3']
) as dag:

    def put_test_data(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )
    
    def get_test_data(ti):
        data = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='put_test_data'
        )
        print(data)
    
    t1 = PythonOperator(
        task_id='put_test_data',
        python_callable=put_test_data
    )
    t2 = PythonOperator(
        task_id='get_test_data',
        python_callable=get_test_data
    )
    t1 >> t2