from airflow import DAG
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#create DAG
with DAG(
        'xcom',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description='Puts in XCom and out of XCom the meaning',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021,1,1),
        catchup=False,
        ) as dag:
    def push_xcom(ti):
        ti.xcom_push(
                key='sample_xcom_key',
                value='xcom test'
                )
    t1 = PythonOperator(
            task_id='put_in',
            python_callable=push_xcom,
            )
    def pull_xcom(ti):
        ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='put_in'
                )
    t2 = PythonOperator(
            task_id = 'put_out',
            python_callable=pull_xcom,
            )
    t1 >> t2


