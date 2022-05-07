from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'k-nevedrov-7-task_8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:
    def push_xcom(ti, **kwargs):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    t1 = PythonOperator(
        task_id='push_xcom',  
        python_callable=push_xcom,  
    )

    def pull_xcom(ti, **kwargs):
        value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(value)

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2