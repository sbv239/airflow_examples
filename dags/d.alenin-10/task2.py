from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'first_dag',
    default_args=default_args,
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd'
    )


    def print_logic_date(ds, **kwargs):
        print(ds)


    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_logic_date
    )

    t1 >> t2
