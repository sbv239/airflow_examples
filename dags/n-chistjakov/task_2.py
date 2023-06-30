from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

ID = "hw_2_n-chistjakov_"

def print_info(ds):
    print(ds)

with DAG(
    'hw_2_n-chistjakov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="First task",
    start_date=datetime(2023, 6, 30)
) as dag:
    bash = BashOperator(
        task_id=ID + "1",
        bash_command="pwd"
    )

    pyth = PythonOperator(
        task_id=ID + "2",
        python_callable=print_info
    )

    bash >> pyth