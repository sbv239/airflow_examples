from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'HW_2_Beresnev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd',
    )

    def print_date(ds, **kwargs):
        print(ds)
        return 'check logs!'


    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    t1 >> t2
