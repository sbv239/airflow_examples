from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_10_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['hw_al-savelev']
) as dag:

    def put_data(ti):
        return 'Airflow tracks everything'

    def get_data(ti):
        pull_data = ti.xcom_pull(
            key='return_value',
            task_ids= 'hw_10_al-savelev_1'
        )
        return pull_data

    
    t1 = PythonOperator(
        task_id='hw_10_al-savelev_1',
        python_callable=put_data
        )
   
    t2 = PythonOperator(
        task_id='hw_10_al-savelev_2',
        python_callable=get_data
        )

    t1 >> t2
