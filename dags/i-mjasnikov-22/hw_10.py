from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def pull_xcom(ti):
    xcom = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_xcom'
    )
    print(xcom)

with DAG(
    'i-mjasnikov-22_hw_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='9 hw',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 10),
    catchup=False,

) as dag:

    t1 = PythonOperator(
        task_id='push_xcom',
        dag=dag,
        python_callable=push_xcom
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        dag=dag,
        python_callable=pull_xcom
    )

    t1 >> t2
