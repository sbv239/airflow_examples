from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime


def print_context(ds):
    print(ds)


with DAG(
    "hw_1_v-shibanov-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='First HW',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=5, day=1),
    catchup=False,
    tags=['hw_1'],
) as dag:
    t1 = BashOperator(
        task_id='print_current_folderworking_directory',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id="print_context",
        python_callable=print_context
    )

    t1 >> t2
