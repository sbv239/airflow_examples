from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
with DAG(
        'andre-karasev_hw_6',
        default_args=default_args,
        description='hw_6_',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 9),
        catchup=False,
        tags=['andre-karasev_hw_6']) as dag:
    for i in range(10):
        NUMBER = i
        t1 = BashOperator(
            task_id='echo' + str(i),
            bash_command='echo $NUMBER',
        )