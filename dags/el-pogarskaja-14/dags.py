from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        "print ds and pwd",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minute=5),
            },
        description='Simple tutorial DAG',
        shedule_interval=timedelta(days=1),
        start_date=datetime(2022,11,15),
        catchup=False,
        ) as dag:
    t1 = BashOperator(
            task_id='print_pwd',
            bash_command='pwd',
            )
    def print_ds(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
            task_id='print_ds',
            python_callable=ptint_ds,
            )

    t1 >> t2


