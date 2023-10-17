from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def push_xcom():
    return "Airflow tracks everything"

def pull_xcom( ti):
    ti.xcom_pull(
        key = 'return_value',
        task_ids = 'wtf'
    )


with DAG(
        'hw_10_a-vahterkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_a-vahterkina_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 17),
        catchup=False,
        tags=['hw_10_a-vahterkina']
) as dag:

    t1 = PythonOperator(
        task_id='wtf',
        python_callable=push_xcom
    )

    t2 = PythonOperator(
        task_id='result',
        python_callable=pull_xcom
    )

    t1 >> t2

