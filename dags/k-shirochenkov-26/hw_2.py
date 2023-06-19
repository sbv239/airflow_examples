from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_2_k_shirochenkov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 19),
    catchup=False,
    tags=['k-shirochenkov'],
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Okey'


    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

    t1 >> t2

