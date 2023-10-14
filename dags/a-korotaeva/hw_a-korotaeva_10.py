from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def xc_push():
    return "Airflow tracks everything"

def xc_pull(ti):
    ti.xcom_pull(
        key='return_value', task_ids='push_xcom'
    )


with DAG(
    'hw_10_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='push_xcom', python_callable=xc_push)

    t2 = PythonOperator(task_id='pull_xcom', python_callable=xc_pull)

    t1 >> t2