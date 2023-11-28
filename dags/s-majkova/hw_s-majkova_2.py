from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_s-majkova_2',
    default_args = {
    'depens_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    },
    description = 'first_DAG',
    schedule_interval=timedelta(days=1),
    start_date = datetime(2023, 11, 25),
    catchup = False,
    tags = ['hw_2'],
) as dag:
    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'It is my the first dag'
    t1 = BashOperator(
        task_id = 'bash_command_pwd',
        bash_command = 'pwd',
    )
    t2 = PythonOperator(
        task_id = 'print_context',
        python_callable=print_context,
    )

    t1>>t2