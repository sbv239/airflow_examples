from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def push_xcom(ti):
    return "Airflow tracks everything"


def pull_data(ti):
    answer = ti.xcom_pull(
        task_ids='task_push_10',
        key='return_value'
    )
    print(answer)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_dmi-rusin_10',
        start_date=datetime(2023, 6, 1),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=2,
        default_args=default_args,
        catchup=False
) as dag:
    task_push = PythonOperator(
        task_id='task_push_10',
        python_callable=push_xcom
    )
    t2 = PythonOperator(
        task_id='dmi-rusin_10_task_get',
        python_callable=pull_data
    )
    task_push >> t2