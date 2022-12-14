from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'my_first_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 13),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    def print_context(ds, **kwargs):
        print(ds)
        return 0

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_context,
    )

    t1 >> t2
