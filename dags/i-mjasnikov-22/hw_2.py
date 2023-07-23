from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'i-mjasnikov-22_hw_2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW #1',
    start_date=datetime(2023,7,23)
) as dag:

    def print_ds(ds):
        print(ds)
        return '1st dag with operators'

    t1 = PythonOperator(
        task_id='python_1',
        python_callable=print_ds
    )

    t2 = BashOperator(
        task_id='bash_1',
        bash_command='pwd'
    )

    t2 >> t1
