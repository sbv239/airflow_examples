"""
My ninth DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_r-nazmiev_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='A simple practice DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,7,17),
    catchup=False,
    tags=['example'],
) as dag:
    
    def f1():
       return "Airflow tracks everything"
   
    def pull_xcom(ti):
        test = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom',
        )
        print(test)
    
    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=f1,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2