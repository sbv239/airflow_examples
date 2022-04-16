from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_first_dag',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    t1 = BashOperator(
        task_id='show_pwd',
        bash_command='pwd',
    )

    def print_ds(ds):
        print(ds)

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t1 >> t2