from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def print_logic_date(ds):
    print(ds)
    return 'Lol Lol See The Logs Buddy'


with DAG(
    'first_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 3),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_logic_date
    )

    t1 >> t2
